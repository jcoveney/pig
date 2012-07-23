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
    private volatile boolean haveStartedIterating = false;
    private volatile boolean havePerformedFinalSpill = false;

    public void add(int v) {
    	// It is document in Pig that once you start iterating on a bag, that you should not
    	// add any elements. This is not explicitly enforced, however this implementation hinges on
    	// this not being violated, so we add explicit checks.
    	if (haveStartedIterating) {
    		throw new RuntimeException("");
    	}
        synchronized (internal) {
            internal.addLast(v);
            size++;
        }
    }

    @Override
    public long spill() {
        if (havePerformedFinalSpill) {
        	return 0L;
        }
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
                    try {
        				out.close();
        			} catch (IOException e2) {
        				//TODO do I need to do something special to chain these?
        				throw new RuntimeException(e); //TODO do more
        			}
                    throw new RuntimeException(e); //TODO do more
                }
                safeCount++;
                if ((spilled++ & 0x3fff) == 0) {
                    reportProgress();
                }
            }
            internal.clear();
            try {
				out.close();
			} catch (IOException e) {
				throw new RuntimeException(e); //TODO do more
			}
            
            // once we have started iterating, this spill will be the last
            // spill to disk, as there can be no new additions
            if (haveStartedIterating) {
            	havePerformedFinalSpill = true;
            }
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
    	haveStartedIterating = true;
        return new IntIterator();
    }

    private class IntIterator {
        private long readFromFile = 0;
        private long readFromMemory = 0;
        private DataInputStream dis;
        private Iterator<IntCursor> iterator;
		private boolean haveDetectedFinalSpill = false;

        private IntIterator() {
            try {
                dis = new DataInputStream(new FileInputStream(spillFile));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }

        public int next() {
            if (((readFromFile + readFromMemory) & 0x3ffL) == 0) {
                reportProgress();
            }
            
            if (haveDetectedFinalSpill || readFromFile < safeCount) {
            	return readFromFile();
            } else if (havePerformedFinalSpill) {
                readFromFile += readFromMemory;
                readFromMemory = 0;
                iterator = null;
                haveDetectedFinalSpill = true;
                return readFromFile();
            } else {
                synchronized (internal) {
                    if (havePerformedFinalSpill) {
                    	readFromFile += readFromMemory;
                        readFromMemory = 0;
                        iterator = null;
                        haveDetectedFinalSpill = true;
                        return readFromFile();
                    }
                    return readFromMemory();
                }
            }
        }
        
        public boolean hasNext() {
        	boolean retVal = (readFromFile + readFromMemory) < size;
        	if (!retVal) {
        		try {
					dis.close();
					// Since this is how we close the piece, this means there is
					// a potential leak if they early terminate. I think leaks like
					// this exist in Pig... but that doesn't mean I want to introduce
					// a new one.
				} catch (IOException e) {
					throw new RuntimeException(e); //TODO do more
				}
        	}
            return retVal;
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
            if (iterator == null) {
                iterator = internal.iterator();
            }
            readFromMemory++;
            return iterator.next().value;
        }
    }
}
