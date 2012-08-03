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

import com.carrotsearch.hppc.LongArrayList;

public class NewDefaultDataBag implements DataBag {
    private static final long serialVersionUID = 1L;
    private static final BinInterSedes bis = new BinInterSedes();
    private static final int VALUES_PER_LINK = 1000;
    private static final int SPILL_OUT_BUFFER = 4 * 1024 * 1024;
    private static final int SPILL_IN_BUFFER = 4 * 1024 * 1024;
    private static final int BREAK_FOR_SPILL_EVERY = 10;

    private LinkedTuples values = new LinkedTuples(VALUES_PER_LINK);
    private long size;
    private volatile SpillInfo spillInfo;
    private volatile boolean haveStartedIterating = false;

    private static class SpillInfo {
        private File spillFile;
        private DataOutputStream spillOutputStream;
        private volatile boolean havePerformedFinalSpill = false;
        //NOTE: this is the ENDING of the location (which happens to be more useful for us)
        private LongArrayList stackLocationInSpillFile = new LongArrayList(); //TODO analyze need for thread safety

        private long getMemorySize() {
            // spillFile ptr (8) + spillOutputStrea ptr (8) + stackLocationInSpillFile ptr (8) + havePerformedfinalSpill (8 after padding) + size of stackLocationInSpillFile
            return 32 + stackLocationInSpillFile.buffer.length * 8;
        }

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
            try {
                spillOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            havePerformedFinalSpill = true;
        }

        public boolean checkIfHavePerformedFinalSpill() {
            return havePerformedFinalSpill;
        }

        public void writeStack(Tuple[] buf, int size, int ct) throws IOException {
            spillOutputStream.writeInt(ct);
            for (int i = 0; i < ct; i++) {
                Tuple t = buf[i];
                if (t != null) {
                    bis.writeDatum(spillOutputStream, t, DataType.TUPLE);
                } else {
                    spillOutputStream.writeByte(BinInterSedes.NULL);
                }
            }
            spillOutputStream.flush();
            stackLocationInSpillFile.add(spillFile.length());
        }

        public DataOutput getSpillOutputStream() {
            return spillOutputStream;
        }
    }

    public NewDefaultDataBag(List<Tuple> listOfTuples) {
        this();
        for (Tuple t : listOfTuples) {
            add(t);
        }
    }

    public NewDefaultDataBag() {
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
        // values.getMemorySize() + values ptr (8) + size (8) + spillInfo ptr (8) + 8 (padding)
        return values.getMemorySize() + 32; //NEED TO INCLUDE SIZE OF SPILLINFO OBJECT
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readFields(in, in.readByte());
    }

    public void readFields(DataInput in, byte type) throws IOException {
        boolean isFirst = true;
        boolean shouldContinue = true;
        while (shouldContinue) {
            synchronized (values) {
                if (isFirst) {
                    clear();
                    switch (type) {
                        case BinInterSedes.TINYBAG: size = in.readUnsignedByte(); break;
                        case BinInterSedes.SMALLBAG: size = in.readUnsignedShort(); break;
                        case BinInterSedes.BAG: size = in.readLong(); break;
                        default: throw new RuntimeException("Unknown type found in NewDefaultDataBag#readFields: " + type);
                    }
                }

                shouldContinue = values.read(in, isFirst);

                isFirst = false;

                if (!shouldContinue) {
                    haveStartedIterating = true;
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (size < BinInterSedes.UNSIGNED_BYTE_MAX) {
            out.writeByte(BinInterSedes.TINYBAG);
            out.writeByte((int) size);
        } else if (size < BinInterSedes.UNSIGNED_SHORT_MAX) {
            out.writeByte(BinInterSedes.SMALLBAG);
            out.writeShort((int) size);
        } else {
            out.writeByte(BinInterSedes.BAG);
            out.writeLong(size);
        }
        synchronized (values) {
            if (spillInfo != null) {
                DataInputStream in = new DataInputStream(new FileInputStream(spillInfo.getSpillFile()));
                byte[] buf = new byte[SPILL_IN_BUFFER]; //TODO tune this? make it settable?
                int read;
                while ((read = in.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
                in.close();
                if (!spillInfo.checkIfHavePerformedFinalSpill()) {
                    values.write(out); //writes the remainder, will have a -1 length at the end
                }
            } else {
                values.write(out);
            }
        }
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
        haveStartedIterating = true;
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

    private class BagIterator implements Iterator<Tuple> {
        private long remaining = 0;
        private Tuple[] tuples = new Tuple[VALUES_PER_LINK];
        private int tuplesPointer = VALUES_PER_LINK;
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
            boolean res = remaining > 0L;
            if (!res && spillInput != null) {
                try {
                    spillInput.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
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

        public boolean read(DataInput in, boolean isFirst) throws IOException {
            int length = 0;
            int shouldBreakForSpill = 0;

            while (shouldBreakForSpill++ < BREAK_FOR_SPILL_EVERY && (length = in.readInt()) != -1) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    TupleLink temp = new TupleLink(last.buf.length);
                    last.next = temp;
                    last = temp;
                }
                Tuple[] buf = last.buf;
                for (int i = 0; i < length; i++) {
                    buf[i] = (Tuple) bis.readDatum(in);
                }
            }
            return length != -1;
        }

        public void read(DataInput in, TupleLink current) {

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
                spillInfo.getSpillOutputStream().writeInt(-1);
                recordsSpilled += ct;
                reset();
            }

            return recordsSpilled;
        }

        public void write(DataOutput out) throws IOException {
            TupleLink current = first;
            int size = current.buf.length;
            while (current.next != null) {
                Tuple[] buf = current.buf;
                out.writeInt(buf.length);
                for (int i = 0; i < size; i++) {
                    Tuple t = buf[i];
                    if (t == null) {
                        out.writeByte(BinInterSedes.NULL);
                    } else {
                        bis.writeDatum(out, t, DataType.TUPLE);
                    }
                }
                current = current.next;
            }
            out.writeInt(ct);
            Tuple[] buf = current.buf;
            for (int i = 0; i < ct; i++) {
                Tuple t = buf[i];
                if (t == null) {
                    out.writeByte(BinInterSedes.NULL);
                } else {
                    bis.writeDatum(out, t, DataType.TUPLE);
                }
            }
            out.writeInt(-1);
        }

        public long getMemorySize() {
            long est = 24; //the base cost of a TupleLink

            int upper = first == last && ct < 100 ? ct : 100;
            double scale = 1.0 * VALUES_PER_LINK / 100;
            Tuple[] buf = first.buf;
            for (int i = 0; i < upper; i++) {
                Tuple t = buf[i];
                est += t == null ? 8 : t.getMemorySize();
            }
            if (first == last && ct < 100) {
                est += (8 * VALUES_PER_LINK - ct);
            } else {
                est *= scale;
            }
            return 24 + stacks * est;
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

    @Override
    public void markStale(boolean stale) {}

    @Override
    public boolean equals(Object o) {
        return compareTo(o) == 0;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (Tuple t : this) {
            // Use 37 because we want a prime, and tuple uses 31.
            hash = t == null ? hash : 37 * hash + t.hashCode();
        }
        return hash;
    }

    @SuppressWarnings("unchecked")
    public int compareTo(Object other) {
        if (this == other)
            return 0;
        if (other instanceof DataBag) {
            DataBag bOther = (DataBag) other;
            if (this.size() != bOther.size()) {
                if (this.size() > bOther.size()) return 1;
                else return -1;
            }

            // Ugh, this is bogus.  But I have to know if two bags have the
            // same tuples, regardless of order.  Hopefully most of the
            // time the size check above will prevent this.
            // If either bag isn't already sorted, create a sorted bag out
            // of it so I can guarantee order.
            DataBag thisClone;
            DataBag otherClone;
            BagFactory factory = BagFactory.getInstance();

            if (this.isSorted() || this.isDistinct()) {
                thisClone = this;
            } else {
                thisClone = factory.newSortedBag(null);
                Iterator<Tuple> i = iterator();
                while (i.hasNext()) thisClone.add(i.next());

            }
            if (((DataBag) other).isSorted() || ((DataBag)other).isDistinct()) {
                otherClone = bOther;
            } else {
                otherClone = factory.newSortedBag(null);
                Iterator<Tuple> i = bOther.iterator();
                while (i.hasNext()) otherClone.add(i.next());
            }
            Iterator<Tuple> thisIt = thisClone.iterator();
            Iterator<Tuple> otherIt = otherClone.iterator();
            while (thisIt.hasNext() && otherIt.hasNext()) {
                Tuple thisT = thisIt.next();
                Tuple otherT = otherIt.next();

                int c = thisT.compareTo(otherT);
                if (c != 0) return c;
            }

            return 0;   // if we got this far, they must be equal
        } else {
            return DataType.compare(this, other);
        }
    }

    private void reportProgress() {
        if (PhysicalOperator.reporter != null) {
            PhysicalOperator.reporter.progress();
        }
    }
}