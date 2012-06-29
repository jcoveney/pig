package org.apache.pig.data;

public interface TupleMaker<A extends Tuple> {
    public A newTuple();
    public A newTuple(int size);
}
