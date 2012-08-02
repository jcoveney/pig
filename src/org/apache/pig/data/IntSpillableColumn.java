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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.ByteCompactLinkedArray.ByteLink;
import org.apache.pig.data.IntCompactLinkedArray.IntLink;
import org.apache.pig.data.utils.BytesHelper;

//TODO we need to incorporate some sort of nullability strategy. should make it generic?
public class IntSpillableColumn implements SpillableColumn {
    private static final int SPILL_OUTPUT_BUFFER = 4 * 1024 * 1024;
    private static final int SPILL_INPUT_BUFFER = 4 * 1024 * 1024;

    private static final int VALUES_PER_LINK = 1000;

    private final IntCompactLinkedArray values = new IntCompactLinkedArray(VALUES_PER_LINK);
    private final ByteCompactLinkedArray nullStatuses = new ByteCompactLinkedArray(VALUES_PER_LINK);

    //private final IntArrayDeque values = new IntArrayDeque();
    //private final ByteArrayDeque nullStatuses = new ByteArrayDeque();
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
        private File nullSpillFile;
        private File valueSpillFile;
        private DataOutputStream nullSpillOutputStream;
        private DataOutputStream valueSpillOutputStream;
        private volatile boolean havePerformedFinalSpill = false;
        private volatile int safeNullStacksInSpillFile = 0; //this represents the number of values that can safely be read from the file
        private volatile int safeValueStacksInSpillFile = 0; //this represents the number of values that can safely be read from the file
        private int nullStacksWrittenSinceLastFlush = 0;
        private int valueStacksWrittenSinceLastFlush = 0;

        public File getNullSpillFile() {
            if (nullSpillFile == null) {
                try {
                    nullSpillFile = File.createTempFile("pig", "bag");
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
                nullSpillFile.deleteOnExit();
            }
            return nullSpillFile;
        }

        public DataOutputStream getNullOutputStream() {
            if (nullSpillOutputStream == null) {
                try {
                    nullSpillOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(getNullSpillFile(), true), SPILL_OUTPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    abort(e); //TODO do more
                }
            }
            return nullSpillOutputStream;
        }

        public DataOutputStream getValueOutputStream() {
            if (valueSpillOutputStream == null) {
                try {
                    valueSpillOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(getValueSpillFile(), true), SPILL_OUTPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    abort(e); //TODO do more
                }
            }
            return valueSpillOutputStream;
        }

        public File getValueSpillFile() {
            if (valueSpillFile == null) {
                try {
                    valueSpillFile = File.createTempFile("pig", "bag");
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
                valueSpillFile.deleteOnExit();
            }
            return valueSpillFile;
        }

        public boolean checkIfHavePerformedFinalSpill() {
            return havePerformedFinalSpill;
        }

        public void havePerformedFinalSpill() {
            havePerformedFinalSpill = true;
        }

        public void incrementNullStacksInSpillFile(long incr) {
            nullStacksWrittenSinceLastFlush += incr;
        }

        public int checkSafeNullStacksInSpillFile() {
            return safeNullStacksInSpillFile;
        }

        public void incrementValueStacksInSpillFile(long incr) {
            valueStacksWrittenSinceLastFlush += incr;
        }

        public int checkSafeValueStacksInSpillFile() {
            return safeValueStacksInSpillFile;
        }

        public void flushOutputStream() {
            if (valueSpillOutputStream != null) {
                try {
                    valueSpillOutputStream.flush();
                } catch (IOException e) {
                    abort(e);
                }
                safeValueStacksInSpillFile += valueStacksWrittenSinceLastFlush;
                valueStacksWrittenSinceLastFlush = 0;
            }
            if (nullSpillOutputStream != null) {
                try {
                    nullSpillOutputStream.flush();
                } catch (IOException e) {
                    abort(e);
                }
                safeNullStacksInSpillFile += nullStacksWrittenSinceLastFlush;
                nullStacksWrittenSinceLastFlush = 0;
            }
        }

        public void clear() {
            try {
                if (nullSpillOutputStream != null) {
                    nullSpillOutputStream.close();
                }
                if (valueSpillOutputStream != null) {
                    valueSpillOutputStream.close();
                }
            } catch (IOException e) {
                abort(e);
            }
            if (nullSpillFile != null) {
                nullSpillFile.delete();
            }
            if (valueSpillFile != null) {
                valueSpillFile.delete();
            }
        }
    }

    protected IntSpillableColumn() {}

    public void add(int v, boolean isNull) {
    	// It is document in Pig that once you start iterating on a bag, that you should not
    	// add any elements. This is not explicitly enforced, however this implementation hinges on
    	// this not being violated, so we add explicit checks.
    	if (haveStartedIterating) {
    		throw new RuntimeException("Cannot write to a Bag once iteration has begun");
    	}
        synchronized (values) {
            nullStatuses.add(isNull);
        	if (!isNull) {
        	    values.add(v);
        	}
            size++;
        }
    }

    //TODO we need to report progress, and to flush every some odd. Base it on the # per stack and some ratio
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

            DataOutput nullOut = spillInfo.getNullOutputStream();
            DataOutput valueOut = spillInfo.getValueOutputStream();

            try {
                int nullStacksSpilled = nullStatuses.spill(nullOut);
                int valueStacksSpilled = values.spill(valueOut);

                // once we have started iterating, this spill will be the last
                // spill to disk, as there can be no new additions
                if (currentlyPerformingFinalSpill) {
                    nullStacksSpilled += nullStatuses.finalSpill(nullOut);
                    valueStacksSpilled += values.finalSpill(valueOut);

                    spillInfo.havePerformedFinalSpill();
                }

                spillInfo.incrementNullStacksInSpillFile(nullStacksSpilled);
                spillInfo.incrementValueStacksInSpillFile(valueStacksSpilled);
            } catch (IOException e) {
                throw new RuntimeException(e);//TODO do more
            }

            spillInfo.flushOutputStream();
        }
        return spilled;
    }

    @Override
    public long getMemorySize() {
        return values.getMemorySize() + nullStatuses.getMemorySize(); //TODO need to take into account the members
    }

    @Override
    public void clear() {
        if (spillInfo != null) {
            spillInfo.clear();
            spillInfo = null;
        }
        values.reset();
        nullStatuses.reset();
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
		private boolean haveDetectedFinalSpill = false;

		private int[] intBuffer = new int[0];
		private byte[] byteBuffer = new byte[0];
		private int intBufferPosition = 0;
		private int byteBufferPosition = 0;
		private int mod = 0;
		private IntContainer container = new IntContainer();
		private int nullStacksReadFromFile = 0;
		private int valueStacksReadFromFile = 0;
		private int nullBytesReadFromMemory = 0;
        private int valueBytesReadFromMemory = 0;
		private ByteLink memoryByteLink;
		private IntLink memoryIntLink;
        private DataInputStream nullInputStream;
        private DataInputStream valuesInputStream;

        private IntIterator() {
            remaining = size;
        }

        public boolean nextIsNull() {
            if (byteBufferPosition < byteBuffer.length) {
                boolean val = BytesHelper.getBitByPos(byteBuffer[byteBufferPosition], mod++);

                if (mod == 8) {
                    mod = 0;
                    byteBufferPosition++;
                }

                return val;
            }

            if (haveDetectedFinalSpill) {
                return fillNullBytesFromFile();
            } else if (spillInfo != null && nullStacksReadFromFile < spillInfo.checkSafeNullStacksInSpillFile()) {
                if (memoryByteLink != null) {
                    updateForFinalSpill();
                }
                return fillNullBytesFromFile();
            } else if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                updateForFinalSpill();
                return fillNullBytesFromFile();
            } else {
                synchronized (values) {
                    if (!(spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill())) {
                        return fillNullBytesFromMemory();
                    }
                }
                updateForFinalSpill();
                return fillNullBytesFromFile();
            }
        }

        public boolean fillNullBytesFromFile() {
            if (nullInputStream == null) {
                try {
                    nullInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(spillInfo.getNullSpillFile()), SPILL_INPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }

            int values;
            try {
                values = nullInputStream.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            byteBuffer = new byte[values];
            try {
                nullInputStream.readFully(byteBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            nullStacksReadFromFile++;
            mod = 1;
            byteBufferPosition = 0;

            return BytesHelper.getBitByPos(byteBuffer[0], 0);
        }

        public boolean fillNullBytesFromMemory() {
            if (memoryByteLink == null) {
                memoryByteLink = nullStatuses.first;
            } else {
                memoryByteLink = memoryByteLink.next;
            }

            byteBuffer = memoryByteLink.buf;

            nullBytesReadFromMemory += 4 + byteBuffer.length;

            mod = 1;
            byteBufferPosition = 0;

            return BytesHelper.getBitByPos(byteBuffer[0], 0);
        }

        public int nextValue() {
            if (intBufferPosition < intBuffer.length) {
                return intBuffer[intBufferPosition++];
            }

            if (haveDetectedFinalSpill) {
                return fillValuesFromFile();
            } else if (spillInfo != null && valueStacksReadFromFile < spillInfo.checkSafeValueStacksInSpillFile()) {
                if (memoryIntLink != null) {
                    updateForFinalSpill();
                }
                return fillValuesFromFile();
            } else if (spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill()) {
                updateForFinalSpill();
                return fillValuesFromFile();
            } else {
                synchronized (values) {
                    if (!(spillInfo != null && spillInfo.checkIfHavePerformedFinalSpill())) {
                        return fillValuesFromMemory();
                    }
                }
                updateForFinalSpill();
                return fillValuesFromFile();
            }
        }

        public int fillValuesFromFile() {
            if (valuesInputStream == null) {
                try {
                    valuesInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(spillInfo.getValueSpillFile()), SPILL_INPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }

            int values;
            try {
                values = valuesInputStream.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO do more
            }
            intBuffer = new int[values];
            for (int i = 0; i < intBuffer.length; i++) {
                try {
                    intBuffer[i] = valuesInputStream.readInt();
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            valueStacksReadFromFile++;
            intBufferPosition = 1;

            return intBuffer[0];
        }

        //Note that this will keep returning values for the last array, so it's important that they call hasNext to not go over
        public int fillValuesFromMemory() {
            if (memoryIntLink == null) {
                memoryIntLink = values.first;
            } else {
                memoryIntLink = memoryIntLink.next;
            }

            intBuffer = memoryIntLink.buf;

            valueBytesReadFromMemory += 4 + 4 * intBuffer.length;

            intBufferPosition = 1;

            return intBuffer[0];
        }

        public IntContainer next() {
            if ((remaining-- & 0x3ffL) == 0) {
                reportProgress();
            }

            boolean isNull = nextIsNull();
            container.isNull = isNull;
            if (!isNull) {
                container.value = nextValue();
            }

            return container;
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
            if (valuesInputStream != null) {
                try {
                    valuesInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            if (nullInputStream != null) {
                try {
                    nullInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
        }

        private void updateForFinalSpill() {
            if (valuesInputStream == null) {
                try {
                    valuesInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(spillInfo.getValueSpillFile()), SPILL_INPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            if (nullInputStream == null) {
                try {
                    nullInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(spillInfo.getNullSpillFile()), SPILL_INPUT_BUFFER));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
            forceSkip(nullInputStream, nullBytesReadFromMemory);
            forceSkip(valuesInputStream, valueBytesReadFromMemory);
            memoryByteLink = null;
            memoryIntLink = null;
            haveDetectedFinalSpill = true;
        }

        public boolean hasNext() {
            return remaining > 0;
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
    //TODO idea: could block spilling right now, and then just copy over the file directly/directly
    //TODO dump the byte array underlying the ArrayDeque
    @Override
    public void writeData(DataOutput out) throws IOException {
        synchronized (values) {
            haveStartedIterating = true;

            if (spillInfo != null) {
                DataInputStream in = new DataInputStream(new FileInputStream(spillInfo.getNullSpillFile()));
                byte[] buf = new byte[SPILL_INPUT_BUFFER]; //TODO tune this? make it settable?
                int read;
                while ((read = in.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
                in.close();
                if (!spillInfo.checkIfHavePerformedFinalSpill()) {
                    nullStatuses.writeAll(out); //writes the remainder, will have a -1 length at the end
                }

                in = new DataInputStream(new FileInputStream(spillInfo.getValueSpillFile()));
                buf = new byte[SPILL_INPUT_BUFFER]; //TODO tune this? make it settable?
                while ((read = in.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
                in.close();
                if (!spillInfo.checkIfHavePerformedFinalSpill()) {
                    values.writeAll(out); //writes the remainder, will have a -1 length at the end
                }
            } else {
                nullStatuses.writeAll(out); //writes the remainder, will have a -1 length at the end
                values.writeAll(out); //writes the remainder, will have a -1 length at the end
            }
        }
    }

    //TODO need to do something if they request spilling
    public void readData(DataInput in, long records) throws IOException {
        synchronized (values) {
            clear();

            nullStatuses.readAll(in);
            values.readAll(in);

            haveStartedIterating = true;
            size = records;

        }
    }

    // Note that this opens up the synchronization every 1000 or so records to let
    // the spill manager spill it if necessary.
    //TODO We could then optionally decide to just write directly to the file instead,
    //TODO since spilling has started.
    // Note also that this doesn't protect against someone trying to add, which would be bad (but shouldn't
    // happen since Pig is singlethreaded, excepting the spill manager).
    //TODO we can also read into a byte buffer records bytes long, and then patch together the bytes ourselves
    /*@Override
    public void readData(DataInput in, long records) throws IOException {
        boolean isFirst = true;

        long increment = 0;
        long remainingBytes = in.readLong();

        while (remainingBytes > 0) {
            synchronized (values) {
                if (isFirst) {
                    clear(); //TODO consider throwing an exception if this is not an empty column?
                    isFirst = false;
                }

                int bytesInBuffer = (int) Math.min(remainingBytes, READ_BYTE_CAP);
                remainingBytes -= bytesInBuffer;
                byte[] buf = new byte[bytesInBuffer];
                in.readFully(buf);
                ByteBuffer buffer = ByteBuffer.wrap(buf);
                while (bytesInBuffer > 0) {
                    byte val = buffer.get();
                    nullStatuses.addLast(val);
                    bytesInBuffer--;
                    for (int i = 0; i < 8; i++) {
                        if (!BytesHelper.getBitByPos(val, i)) {
                            if (bytesInBuffer >= 4) {
                                values.addLast(buffer.getInt());
                                bytesInBuffer -= 4;
                            } else if (bytesInBuffer == 0) {
                                values.addLast(in.readInt());
                            } else {
                                int temp = 0;
                                for (int k = 0; k < bytesInBuffer; k++) {
                                    val |= (buffer.get() & 0xff) << (24 - k * 8);
                                }
                                for (int k = bytesInBuffer; k < 4; k++) {
                                    val |= (in.readByte() & 0xff) << (24 - k * 8);
                                }
                                values.addLast(temp);
                                bytesInBuffer = 0;
                            }
                        }
                        increment += 8;
                    }
                }

                if (increment > records) {
                    increment = records;
                }

                size = increment;
            }
        }
    }*/

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