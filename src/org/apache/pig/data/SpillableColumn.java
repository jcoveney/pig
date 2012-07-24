package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.pig.impl.util.Spillable;

public interface SpillableColumn extends Spillable {
    public void clear();
    public long size();
    public void writeData(DataOutput out);
    public void readData(DataInput in, long records);
}
