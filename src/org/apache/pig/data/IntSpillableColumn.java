package org.apache.pig.data;

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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.cursors.IntCursor;

//TODO we need to incorporate some sort of nullability strategy. should make it generic?
public class IntSpillableColumn implements SpillableColumn {
    private final IntArrayDeque internal = new IntArrayDeque();
    private EncapsulatedSpillInformation spillInfo;

    //TODO the size and count could be pushed into the parent, since that should be parallel
    //would complicate things, but would be more efficient. Then again, the savings compared to even a couple
    //of tuples is immense, so... just need to benchmark
    //TODO many of these may not need to be volatile as synchronizing should flush as well
    private volatile long size = 0;
    private volatile boolean haveStartedIterating = false;

    public void abort(Exception e) {
        clear();
        throw new RuntimeException(e);
    }

    private class EncapsulatedSpillInformation {
        private File spillFile;
        private DataOutputStream spillOutputStream;
        private volatile boolean havePerformedFinalSpill = false;
        private volatile long safeCount = 0; //this represents the number of values that can safely be read from the file

        public File getSpillFile() {
            if (spillFile == null) {
                try {
                    spillFile = File.createTempFile("pig", "bag");
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
                spillFile.deleteOnExit();
            }
            return spillFile;
        }

        public void writeInt(int v) {
            if (spillOutputStream == null) {
                try {
                    spillOutputStream = new DataOutputStream(new FileOutputStream(getSpillFile(), true));
                } catch (FileNotFoundException e) {
                    abort(e); //TODO do more
                }
            }
            try {
                spillOutputStream.writeInt(v);
            } catch (IOException e) {
                abort(e); //TODO do more
            }
        }

        public boolean checkIfHavePerformedFinalSpill() {
            return havePerformedFinalSpill;
        }

        public void havePerformedFinalSpill() {
            havePerformedFinalSpill = true;
        }

        public void incrSafeCount(long val) {
            safeCount += val;
        }

        public long checkSafeCount() {
            return safeCount;
        }

        public void flushOutputStream() {
            try {
                spillOutputStream.flush();
            } catch (IOException e) {
                abort(e);
            }
        }

        public void setSafeCount(long l) {
            safeCount = l;
        }

        public void clear() {
            try {
                spillOutputStream.close();
            } catch (IOException e) {
                abort(e);
            }
            spillFile.delete();
        }
    }

    private static final long flushEvery = 0x3fff; //while spilling, will flush to disk every time this many values is spilled
    private static final int progressEvery = 0x3fff;

    protected IntSpillableColumn() {}

    public void add(int v) {
    	// It is document in Pig that once you start iterating on a bag, that you should not
    	// add any elements. This is not explicitly enforced, however this implementation hinges on
    	// this not being violated, so we add explicit checks.
    	if (haveStartedIterating) {
    		throw new RuntimeException("Cannot write to a Bag once iteration has begun");
    	}
        synchronized (internal) {
            internal.addLast(v);
            size++;
        }
    }

    @Override
    public long spill() {
        if (spillInfo == null) {
            spillInfo = new EncapsulatedSpillInformation();
        }
        if (spillInfo.checkIfHavePerformedFinalSpill()) {
        	return 0L;
        }
        long spilled = 0;
        // We single this case out because we don't want to increment safeCount if
        // we are performing the final spill, as iterators may have begun to read from memory
        // and if they have, then these values are not space. It is possible to include logic
        // to make this unnecessary, it is just unclear whether optimizing for one spill is worth it.
        // Worth revisiting.
        boolean currentlyPerformingFinalSpill = false;
        long startingSafeCount = spillInfo.checkSafeCount();
        synchronized (internal) {
            if (haveStartedIterating) {
                currentlyPerformingFinalSpill = true;
            }

            for (IntCursor cursor : internal) {
                spillInfo.writeInt(cursor.value);

                if ((spilled++ & progressEvery) == 0) {
                    reportProgress();
                }
                if (!currentlyPerformingFinalSpill && (spilled & flushEvery) == 0) {
                    spillInfo.flushOutputStream();
                    spillInfo.incrSafeCount(flushEvery);
                }
            }
            internal.clear();
            spillInfo.flushOutputStream();
            spillInfo.setSafeCount(spilled + startingSafeCount);

            // once we have started iterating, this spill will be the last
            // spill to disk, as there can be no new additions
            if (haveStartedIterating) {
            	spillInfo.havePerformedFinalSpill();
            }
        }
        return spilled;
    }

    @Override
    public long getMemorySize() {
        int sz = internal.size();
        return sz * 4 + ( sz % 2 == 0 ? 0 : 4); //TODO include new fields
    }

    @Override
    public void clear() {
        if (spillInfo != null) {
            spillInfo.clear();
            spillInfo = null;
        }
        internal.clear();
        size = 0;
        haveStartedIterating = false;
    }

    //TODO think about how we want to handle the clearing logic

    @Override
    public long size() {
        return size;
    }

    private void reportProgress() {
        if (PhysicalOperator.reporter != null) {
            PhysicalOperator.reporter.progress();
        }
    }

    public IntIterator iterator() {
    	haveStartedIterating = true;
        return new IntIterator();
    }

    //TODO need to actually take the pointer into account when you detect the final spill.
    //ie NEED to actually take that jump into account

    final public class IntIterator {
        private long readFromFile = 0;
        private long readFromMemory = 0;
        private DataInputStream dis;
        private Iterator<IntCursor> iterator;
		private boolean haveDetectedFinalSpill = false;

        private IntIterator() {}

        public int next() {
            if (((readFromFile + readFromMemory) & 0x3ffL) == 0) {
                reportProgress();
            }

            if (haveDetectedFinalSpill || (spillInfo != null && readFromFile < spillInfo.checkSafeCount())) {
            	return readFromFile();
            } else if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                updateForFinalSpill();
                return readFromFile();
            } else {
                synchronized (internal) {
                    if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                        updateForFinalSpill();
                        return readFromFile();
                    }
                    return readFromMemory();
                }
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
                    finish();
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

        // Since this is how we close the piece, this means there is
        // a potential leak if they early terminate. I think leaks like
        // this exist in Pig... but that doesn't mean I want to introduce
        // a new one.
        public void finish() {
            try {
                dis.close();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }

        private void updateForFinalSpill() {
            forceSkip(dis, readFromMemory * 4);
            readFromFile += readFromMemory;
            readFromMemory = 0;
            iterator = null;
            haveDetectedFinalSpill = true;
        }

        public boolean hasNext() {
            return (readFromFile + readFromMemory) < size;
        }

        private int readFromFile() {
            if (dis == null) {
                try {
                    dis = new DataInputStream(new FileInputStream(spillInfo.getSpillFile()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            readFromFile++;
            try {
                return dis.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }

        /**
         * This assumes that a lock is held and is NOT thread safe!
         * @return
         */
        private int readFromMemory() {
            if (iterator == null) {
                iterator = internal.iterator();
            }
            readFromMemory++;
            return iterator.next().value;
        }
    }

    @Override
    public void writeData(DataOutput out) {
        IntIterator iterator = iterator();
        while (iterator.hasNext()) {
            try {
                out.writeInt(iterator.next());
            } catch (IOException e) {
                abort(e);
            }
        }
    }

    @Override
    public void readData(DataInput in, long records) {
        spillInfo = null;
        internal.clear();
        for (long i = 0; i < records; i++) {
            try {
                internal.addLast(in.readInt());
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }
    }
}
