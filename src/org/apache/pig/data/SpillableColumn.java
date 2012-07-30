package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.Spillable;

public interface SpillableColumn extends Spillable {
    public void clear();
    public long size();
    public void writeData(DataOutput out) throws IOException;
    public void readData(DataInput in, long records) throws IOException;
    public void getFromPosition(Tuple t, int i) throws ExecException;
    public void getFromPosition(TypeAwareTuple t, int i) throws ExecException;
    public SpillableColumnIterator iterator();
    public byte getDataType();

    public static abstract class SpillableColumnIterator {
        public abstract boolean hasNext();
        public abstract void finish();
        public abstract void setTuplePositionWithNext(Tuple t, int i) throws ExecException;
        public abstract void setTuplePositionWithNext(TypeAwareTuple t, int i) throws ExecException;
    }
}
