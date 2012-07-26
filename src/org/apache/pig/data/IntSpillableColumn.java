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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.utils.BytesHelper;

import com.carrotsearch.hppc.ByteArrayDeque;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.cursors.ByteCursor;
import com.carrotsearch.hppc.cursors.IntCursor;

//TODO we need to incorporate some sort of nullability strategy. should make it generic?
public class IntSpillableColumn implements SpillableColumn {
    private final IntArrayDeque values = new IntArrayDeque();
    private final ByteArrayDeque nullStatus = new ByteArrayDeque();
    private volatile EncapsulatedSpillInformation spillInfo;

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
        private volatile long bytesSafeToReadFromSpillFile = 0L; //this represents the number of values that can safely be read from the file
        private long bytesWrittenSinceLastFlush = 0L;

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

		public void writeByte(byte v) {
			if (spillOutputStream == null) {
                try {
                    spillOutputStream = new DataOutputStream(new FileOutputStream(getSpillFile(), true));
                } catch (FileNotFoundException e) {
                    abort(e); //TODO do more
                }
            }
            try {
                spillOutputStream.writeByte(v);
            } catch (IOException e) {
                abort(e); //TODO do more
            }
            bytesWrittenSinceLastFlush++;
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
            bytesWrittenSinceLastFlush += 4;
        }

        public boolean checkIfHavePerformedFinalSpill() {
            return havePerformedFinalSpill;
        }

        public void havePerformedFinalSpill() {
            havePerformedFinalSpill = true;
        }

        public long checkBytesSafeToReadFromSpillFile() {
            return bytesSafeToReadFromSpillFile;
        }

        public void flushOutputStream() {
            if (spillOutputStream != null) {
                try {
                    spillOutputStream.flush();
                } catch (IOException e) {
                    abort(e);
                }
                bytesSafeToReadFromSpillFile += bytesWrittenSinceLastFlush;
                bytesWrittenSinceLastFlush = 0L;
            }
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

    public void add(int v, boolean isNull) {
    	// It is document in Pig that once you start iterating on a bag, that you should not
    	// add any elements. This is not explicitly enforced, however this implementation hinges on
    	// this not being violated, so we add explicit checks.
    	if (haveStartedIterating) {
    		throw new RuntimeException("Cannot write to a Bag once iteration has begun");
    	}
        synchronized (values) {
        	if (!isNull) {
        		values.addLast(v);
        	}
        	int mod = (int)size & 7;
        	byte lastNullStatusByte;
        	if (mod == 0) {
        		lastNullStatusByte = (byte)0xFF;
        	} else {
        		lastNullStatusByte = nullStatus.removeLast();
        	}
        	lastNullStatusByte = BytesHelper.setBitByPos(lastNullStatusByte, isNull, mod);
        	nullStatus.addLast(lastNullStatusByte);
            size++;
        }
    }

    @Override
    public long spill() {
        if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
        	return 0L;
        }
        long spilled = 0;
        // We single this case out because we don't want to increment safeCount if
        // we are performing the final spill, as iterators may have begun to read from memory
        // and if they have, then these values are not space. It is possible to include logic
        // to make this unnecessary, it is just unclear whether optimizing for one spill is worth it.
        // Worth revisiting.
        boolean currentlyPerformingFinalSpill = false;
        synchronized (values) {
            if (spillInfo == null) {
                spillInfo = new EncapsulatedSpillInformation();
            }

            if (haveStartedIterating) {
                currentlyPerformingFinalSpill = true;
            }

            byte byteToSave = 0;
            IntArrayDeque intsToSave = new IntArrayDeque();

            int mod = (int)size & 7;
            if (!currentlyPerformingFinalSpill && mod > 0) {
                byteToSave = nullStatus.removeLast();
                for (int i = mod - 1; i >= 0; i--) {
                    if (!BytesHelper.getBitByPos(byteToSave, i)) {
                        intsToSave.addFirst(values.removeLast());
                    }
                }
            }

            while (!nullStatus.isEmpty()) {
            	byte value = nullStatus.removeFirst();
            	spillInfo.writeByte(value);
            	boolean flush = false;
            	for (int i = 0; i < 8; i++) {
            		if (!BytesHelper.getBitByPos(value, i)) {
            			spillInfo.writeInt(values.removeFirst());
            		}
            		if ((spilled++ & progressEvery) == 0) {
                        reportProgress();
                    }
            		if (!currentlyPerformingFinalSpill && (spilled & flushEvery) == 0) {
                        flush = true;
                    }
            	}
        		if (flush) {
        			spillInfo.flushOutputStream();
        		}
            }

            nullStatus.clear();
            values.clear();

            if (!currentlyPerformingFinalSpill && mod > 0) {
                nullStatus.addFirst(byteToSave);
                for (IntCursor cursor : intsToSave) {
                    values.addLast(cursor.value);
                }
            }

            // once we have started iterating, this spill will be the last
            // spill to disk, as there can be no new additions
            if (haveStartedIterating) {
            	spillInfo.havePerformedFinalSpill();
            }

            spillInfo.flushOutputStream();
        }
        return spilled;
    }

    @Override
    public long getMemorySize() {
        int sz = values.size();
        return sz * 4 + ( sz % 2 == 0 ? 0 : 4); //TODO include new fields, esp. the booleans
    }

    @Override
    public void clear() {
        if (spillInfo != null) {
            spillInfo.clear();
            spillInfo = null;
        }
        values.clear();
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

    public SpillableColumnIterator iterator() {
    	haveStartedIterating = true;
        return new IntIterator();
    }

    public static class IntContainer {
    	private IntContainer() {}

    	public int value = 0;
    	public boolean isNull = true;
    }

    //TODO need to actually take the pointer into account when you detect the final spill.
    //ie NEED to actually take that jump into account

    final public class IntIterator extends SpillableColumnIterator {
        private long remaining;
        private DataInputStream dis;
        private Iterator<IntCursor> intIterator;
        private Iterator<ByteCursor> byteIterator;
		private boolean haveDetectedFinalSpill = false;
		private final IntContainer[] containers = new IntContainer[8];
		private int remainingInContainer = 8;
		private long bytesReadFromFile = 0;
		private long bytesReadFromMemory = 0;

        private IntIterator() {
            remaining = size;
        	for (int i = 0; i < 8; i++) {
				containers[i] = new IntContainer();
			}
        }

        public IntContainer next() {
            if ((remaining-- & 0x3ffL) == 0) {
                reportProgress();
            }

            if (remainingInContainer < 8) {
            	IntContainer retVal = containers[remainingInContainer++];
            	return retVal;
            }

            if (haveDetectedFinalSpill) {
                return readFromFile();
            } else if (spillInfo != null && bytesReadFromFile < spillInfo.checkBytesSafeToReadFromSpillFile()) {
                if (bytesReadFromMemory > 0) {
                    updateForFinalSpill();
                }
            	return readFromFile();
            } else if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                updateForFinalSpill();
                return readFromFile();
            } else {
                synchronized (values) {
                    if (!(spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill())) {
                        return readFromMemory();
                    }
                }
                updateForFinalSpill();
                return readFromFile();
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
            if (dis != null) {
                try {
                    dis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
        }

        private void updateForFinalSpill() {
            if (dis == null) {
                try {
                    dis = new DataInputStream(new FileInputStream(spillInfo.getSpillFile()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            forceSkip(dis, bytesReadFromMemory);
            intIterator = null;
            byteIterator = null;
            haveDetectedFinalSpill = true;
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        private IntContainer readFromFile() {
            if (dis == null) {
                try {
                    dis = new DataInputStream(new FileInputStream(spillInfo.getSpillFile()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }

        	byte cachedByteVal;
        	try {
				cachedByteVal = dis.readByte();
				bytesReadFromFile++;
			} catch (IOException e) {
				throw new RuntimeException(e); //TODO do more
			}
        	for (int i = 0; i < 8; i++) {
        		boolean val = BytesHelper.getBitByPos(cachedByteVal, i);
        		containers[i].isNull = val;
        		if (!val) {
        			try {
						containers[i].value = dis.readInt();
						bytesReadFromFile += 4;
					} catch (IOException e) {
						throw new RuntimeException(e); //TODO do more
					}
        		}
            }
        	remainingInContainer = 1;

            return containers[0];
        }

        /**
         * This assumes that a lock is held and is NOT thread safe!
         * @return
         */
        //TODO buffer this as well, and the reading from file can be updated as well
        private IntContainer readFromMemory() {
        	if (byteIterator == null) {
        		byteIterator = nullStatus.iterator();
        	}
            if (intIterator == null) {
                intIterator = values.iterator();
            }
            byte cachedByteVal = byteIterator.next().value;
            bytesReadFromMemory++;
            for (int i = 0; i < 8; i++) {
                boolean val = BytesHelper.getBitByPos(cachedByteVal, i);
                containers[i].isNull = val;
                if (!val) {
                    containers[i].value = intIterator.next().value;
                    bytesReadFromMemory += 4;
                }
            }
            remainingInContainer = 1;

            return containers[0];
        }

        @Override
        public void setTuplePositionWithNext(Tuple t, int i) throws ExecException {
            IntContainer next = next();
            if (next.isNull) {
                t.set(i, null);
            } else {
                t.set(i, Integer.valueOf(next.value));
            }
        }

        @Override
        public void setTuplePositionWithNext(TypeAwareTuple t, int i) throws ExecException {
            IntContainer next = next();
            if (next.isNull) {
                t.set(i, null);
            } else {
                t.setInt(i, next.value);
            }
        }
    }

    //TODO this could probably be optimized. Benchmark and go from there.
    @Override
    public void writeData(DataOutput out) {
        IntIterator iterator = (IntIterator) iterator();
        byte byteBuffer = 0;
        boolean[] nullBuffer = new boolean[8];
        int[] intBuffer = new int[8];
        int mod = 0;
        while (iterator.hasNext()) {
        	IntContainer container = iterator.next();
        	boolean val = container.isNull;
        	byteBuffer = BytesHelper.setBitByPos(byteBuffer, val, mod);
        	if (!val) {
        		intBuffer[mod] = container.value;
        		nullBuffer[mod] = true;
        	} else {
        	    nullBuffer[mod] = false;
        	}
        	if (mod == 7) {
        		try {
                    out.writeByte(byteBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
        		for (int i = 0; i < nullBuffer.length; i++) {
        		    if (nullBuffer[i]) {
        		        try {
        		            out.writeInt(intBuffer[i]);
        		        } catch (IOException e) {
                            throw new RuntimeException(e); //TODO do more
                        }
        		    }
        		}
        		byteBuffer = 0;
        		mod = 0;
        	} else {
        		mod++;
        	}
        }
        for (int j = 0; j < mod; j++) {
            try {
                out.writeByte(byteBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            for (int i = 0; i < nullBuffer.length; i++) {
                if (nullBuffer[i]) {
                    try {
                        out.writeInt(intBuffer[i]);
                    } catch (IOException e) {
                        throw new RuntimeException(e); //TODO do more
                    }
                }
            }
        }
    }

    @Override
    public void readData(DataInput in, long records) {
        spillInfo = null;
        values.clear();
        for (long i = 0; i < records; i++) {
            try {
                values.addLast(in.readInt());
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }
    }

    @Override
    public void getFromPosition(Tuple t, int i) throws ExecException {
        if (t.isNull(i)) {
            add(0, true);
        } else {
            add(((Integer)t.get(i)).intValue(), false);
        }
    }

    @Override
    public void getFromPosition(TypeAwareTuple t, int i) throws ExecException {
        if (t.isNull(i)) {
            add(0, true);
        } else {
            add(t.getInt(i), false);
        }
    }

    @Override
    public byte getDataType() {
        return DataType.INTEGER;
    }
}
