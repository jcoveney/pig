package org.apache.pig.data;

import org.apache.pig.impl.util.Spillable;

public interface SpillableColumn extends Spillable {
    public void clear();
    public long size();
}
