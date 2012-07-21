package org.apache.pig.data;

import java.io.DataInputStream;
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

public class IntSpillableColumn implements SpillableColumn {
    private IntArrayDeque internal = new IntArrayDeque();
    private File spillFile;
    //TODO the size and count could be pushed into the parent, since that should be parallel
    //would complicate things, but would be more efficient. Then again, the savings compared to even a couple
    //of tuples is immense, so... just need to benchmark
    private volatile long size;
    private volatile long safeCount; //this represents the number of values that can safely be read from the file
    private volatile int spillCount;

    public void add(int v) {
        synchronized (internal) {
            internal.addLast(v);
            size++;
        }
    }

    @Override
    public long spill() {
        long spilled = 0;
        synchronized (internal) {
            if (spillFile == null) {
                try {
                    spillFile = File.createTempFile("pig", "bag");
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }

            DataOutputStream out;
            try {
                out = new DataOutputStream(new FileOutputStream(spillFile, true));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e); //TODO do more
            }

            for (IntCursor cursor : internal) {
                try {
                    out.writeInt(cursor.value);
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
                spilled++;
                safeCount++;
                if ((spilled & 0x3fff) == 0) {
                    reportProgress();
                }
            }
            try {
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            internal.clear();
            spillCount++;
        }
        return spilled;
    }

    @Override
    public long getMemorySize() {
        int sz = internal.size();
        return sz * 4 + ( sz % 2 == 0 ? 0 : 4);
    }

    @Override
    public void clear() {
        internal.clear();
        spillFile = null;
    }

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
        return new IntIterator();
    }

    private static abstract class PrimitiveIterator<T> {
        private long iteratedCount = 0;
        private long readFromFile = 0;
        private int readFromMemory = 0;
        private DataInputStream dis;
        private long spillsWeHaveSeen = 0;
        private Iterator<IntCursor> iterator;

        public PrimitiveIterator(File spillFile) {
            try {
                dis = new DataInputStream(new FileInputStream(spillFile));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }
    }

    private class IntIterator extends PrimitiveIterator<IntCursor> {
        private long iteratedCount = 0;
        private long readFromFile = 0;
        private int readFromMemory = 0;
        private DataInputStream dis;
        private long spillsWeHaveSeen = 0;

        private Iterator<IntCursor> iterator;


        private IntIterator() {
            super(spillFile);
        }

        public int next() {
            if ((iteratedCount++ & 0x3ff) == 0) {
                reportProgress();
            }

            checkForSpill();
            //if we are less than the known number of values on disk, we can safely read from disk
            if (readFromFile < safeCount) {
                return readFromFile();
            } else {
                synchronized (internal) {
                    if (!checkForSpill()) {
                        return readFromMemory();
                    }
                }
                //we only reach this if there was a spill since, which means safeCount should be incremented
                return readFromFile();
            }
        }

        private boolean checkForSpill() {
            //this means there was a spill since we last did something. Now, we only would have been
            //reading from the memory store if we had previously hit the end of the file, so we just increment
            //the position in the file we want to read from and continue
            if (spillsWeHaveSeen < spillCount) {
                readFromFile += readFromMemory;
                readFromMemory = 0;
                spillsWeHaveSeen = spillCount;
                iterator = null;
                return true;
            }
            return false;
        }

        public boolean hasNext() {
            return iteratedCount < size;
        }

        private int readFromFile() {
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
          //we do this in case there was a spill in between the two
            //we need to make sure that we aren't going to miss something
            if (internal.size() > 0) {
                iterator = internal.iterator();
            }
            readFromMemory++;
            return iterator.next().value;
        }
    }
}
