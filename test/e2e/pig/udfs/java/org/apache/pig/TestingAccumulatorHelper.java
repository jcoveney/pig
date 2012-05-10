package org.apache.pig;

import java.io.IOException;

import org.apache.pig.data.Tuple;

public class TestingAccumulatorHelper extends AccumulatorEvalFunc<Integer> implements TerminatingAccumulator<Integer> {
    public boolean earlyTerminate = false;
    public int accumulates = 0;

    public TestingAccumulatorHelper(String earlyTerminate) {
        this.earlyTerminate = Boolean.parseBoolean(earlyTerminate);
    }

    public void accumulate(Tuple input) throws IOException {
        accumulates++;
    }

    public Integer getValue() {
        return accumulates;
    }

    public void cleanup() {
        accumulates = 0;
    }

    public boolean isFinished() {
        return earlyTerminate;
    }
}
