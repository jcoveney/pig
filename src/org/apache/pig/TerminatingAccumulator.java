package org.apache.pig;

public interface TerminatingAccumulator<T> extends Accumulator<T> {
    public boolean isFinished();
}
