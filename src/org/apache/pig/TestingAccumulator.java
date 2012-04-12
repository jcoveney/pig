package org.apache.pig;

import java.util.concurrent.atomic.AtomicInteger;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;

public class TestingAccumulator extends IteratingAccumulatorEvalFunc<Long> {
    public  Long exec(Iterator<Tuple> it) throws IOException {
        long i = 0;

        while (it.hasNext()) {
            i += (Integer)it.next().get(0);
        }

        return i;
    }
}
