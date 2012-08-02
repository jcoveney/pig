package org.apache.pig.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.NewDefaultDataBag.LinkedTuples.TupleLink;

import com.google.common.collect.Lists;

public class NewDefaultDataBag implements DataBag {
    private static final BinInterSedes bis = new BinInterSedes();
    private static final int VALUES_PER_LINK = 1000;
    private static final int SPILL_OUT_BUFFER = 4 * 1024 * 1024;
    private static final int SPILL_IN_BUFFER = 4 * 1024 * 1024;

    private LinkedTuples values = new LinkedTuples(VALUES_PER_LINK);
    private long size;
    private volatile SpillInfo spillInfo;
    private volatile boolean haveStartedIterating = false;

    private static class SpillInfo {
        private File spillFile;
        private DataOutputStream spillOutputStream;
        private volatile boolean havePerformedFinalSpill = false;
        //NOTE: this is the ENDING of the location (which happens to be more useful for us)
        private List<Long> stackLocationInSpillFile = Lists.newArrayList(); //TODO may need to be j.u.c

        public SpillInfo() {
            try {
                spillFile = File.createTempFile("tmp","tmp");
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            try {
                spillOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(spillFile), SPILL_OUT_BUFFER));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }

        public File getSpillFile() {
            return spillFile;
        }

        public long getSafeStacks() {
            return stackLocationInSpillFile.size();
        }

        public long getStackOffset(int idx) {
            return stackLocationInSpillFile.get(idx);
        }

        public void havePerformedFinalSpill() {
            havePerformedFinalSpill = true;
        }

        public boolean checkIfHavePerformedFinalSpill() {
            return havePerformedFinalSpill;
        }

        public void writeStack(Tuple[] buf, int size, int ct) throws IOException {
            spillOutputStream.writeInt(ct);
            for (int i = 0; i < ct; i++) {
                bis.writeDatum(spillOutputStream, buf[i], DataType.TUPLE);
            }
            spillOutputStream.flush();
            stackLocationInSpillFile.add(spillFile.length());
        }
    }

    @Override
    public long spill() {
        if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
            return 0L;
        }
        synchronized (values) {
            if (spillInfo == null) {
                spillInfo = new SpillInfo();
            }

            long spilled;
            try {
                spilled = values.spill(spillInfo, haveStartedIterating);
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }

            if (haveStartedIterating) {
                spillInfo.havePerformedFinalSpill();
            }

            return spilled;
        }
    }

    @Override
    public long getMemorySize() {
        // values.getMemorySize() + size (8) + spillInfo ptr (8) + 8 (padding)
        return values.getMemorySize() + 24; //NEED TO INCLUDE SIZE OF SPILLINFO OBJECT
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new RuntimeException("NEED TO IMPLEMENT readFields");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new RuntimeException("NEED TO IMPLEMENT write");
    }

    @Override
    public int compareTo(Object o) {
        throw new RuntimeException("NEED TO IMPLEMENT compareTo");
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new BagIterator();
    }

    @Override
    public void add(Tuple t) {
        synchronized (values) {
            if (haveStartedIterating) {
                throw new RuntimeException("Cannot add once iteration has begun");
            }
            values.add(t);
            size++;
        }
    }

    @Override
    public void addAll(DataBag b) {
        for (Tuple t : b) {
            add(t);
        }
    }

    @Override
    public void clear() {
        values.reset();
        values = new LinkedTuples(VALUES_PER_LINK);
        size = 0;
        spillInfo = null;
        haveStartedIterating = false;
    }

    @Override
    public void markStale(boolean stale) {}

    private class BagIterator implements Iterator<Tuple> {
        private long remaining = 0;
        private Tuple[] tuples = new Tuple[VALUES_PER_LINK];
        private int tuplesPointer = 0;
        private boolean haveDetectedFinalSpill;
        private int stacksReadFromFile;
        private int stacksReadFromMemory;
        private TupleLink memoryTupleLink;
        private DataInputStream spillInput;

        public BagIterator() {
            remaining = size;
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public Tuple next() {
            if ((remaining-- & 0x3ffL) == 0) {
                reportProgress();
            }

            if (tuplesPointer < VALUES_PER_LINK) {
                return tuples[tuplesPointer++];
            }
            if (haveDetectedFinalSpill) {
                return fillBufferFromFile();
            } else if (spillInfo != null && stacksReadFromFile < spillInfo.getSafeStacks()) {
                if (stacksReadFromMemory > 0) {
                    updateForFinalSpill();
                }
                return fillBufferFromFile();
            } else if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                updateForFinalSpill();
                return fillBufferFromFile();
            } else {
                synchronized (values) {
                    if (!(spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill())) {
                        return fillBufferFromMemory();
                    }
                }
                updateForFinalSpill();
                return fillBufferFromFile();
            }
        }

        private void forceSkip(DataInputStream dis, long toSkip) {
            long leftToSkip = toSkip;
            int iterationsWithNoProgress = 0;
            while (leftToSkip > 0) {
                long skipped;
                try {
                    skipped = dis.skip(leftToSkip);
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
                if (skipped == 0) {
                    iterationsWithNoProgress++;
                }
                if (iterationsWithNoProgress > 1000) {
                    throw new RuntimeException("Had over 1000 iterations trying to skip in InputStream without progress");
                }
                leftToSkip -= skipped;
            }
        }

        private void updateForFinalSpill() {
            if (stacksReadFromMemory > 0) {
                long toSkip = spillInfo.getStackOffset(stacksReadFromFile + stacksReadFromMemory)
                            - spillInfo.getStackOffset(stacksReadFromFile);
                forceSkip(spillInput, toSkip);
            }

            haveDetectedFinalSpill = true;
        }

        private Tuple fillBufferFromFile() {
            if (spillInput == null) {
                try {
                    spillInput = new DataInputStream(new BufferedInputStream(new FileInputStream(spillInfo.getSpillFile()), SPILL_IN_BUFFER));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            int sz;
            try {
                sz = spillInput.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            for (int i = 0; i < sz; i++) {
                try {
                    tuples[i] = (Tuple) bis.readDatum(spillInput, spillInput.readByte());
                } catch (ExecException e) {
                    throw new RuntimeException(e); //TODO do more
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            stacksReadFromFile++;

            tuplesPointer = 1;
            return tuples[0];
        }

        private Tuple fillBufferFromMemory() {
            if (memoryTupleLink == null) {
                memoryTupleLink = values.first;
            } else {
                memoryTupleLink = memoryTupleLink.next;
            }
            tuples = memoryTupleLink.buf;
            stacksReadFromMemory++;

            tuplesPointer = 1;
            return tuples[0];
        }

        @Override
        public void remove() {}
    }

    public static class LinkedTuples {
        public TupleLink first;
        private TupleLink last;
        private int ct = 0;
        private int stacks = 1;

        public void add(Tuple t) {
            Tuple[] buf = first.buf;
            buf[ct++] = t;
            if (ct == buf.length) {
                TupleLink temp = new TupleLink(ct);
                last.next = temp;
                last = temp;
                stacks++;
                ct = 0;
            }
        }

        private void reset() {
            first = new TupleLink(first.buf.length);
            last = first;
            stacks = 1;
            ct = 0;
        }

        public long spill(SpillInfo spillInfo, boolean spillAll) throws IOException {
            TupleLink current = first;
            long recordsSpilled = 0;
            int size = current.buf.length;
            while (current.next != null) {
                spillInfo.writeStack(current.buf, size, size);
                current = current.next;
                first = current;
                stacks--;
                recordsSpilled += size;
            }
            current = last;
            if (spillAll) {
                spillInfo.writeStack(current.buf, size, ct);
                recordsSpilled += ct;
                reset();
            }

            // TODO Auto-generated method stub
            return recordsSpilled;
        }

        public long getMemorySize() {
            return 24 + stacks * (24 + 4 * last.buf.length);
        }

        private LinkedTuples(int size) {
            first = new TupleLink(size);
            last = first;
        }

        public static class TupleLink {
            private Tuple[] buf;
            private TupleLink next;

            public TupleLink(int size) {
                buf = new Tuple[size];
            }
        }
    }

    private void reportProgress() {
        if (PhysicalOperator.reporter != null) {
            PhysicalOperator.reporter.progress();
        }
    }
}
