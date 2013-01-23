package org.apache.pig.algebraic;

import java.util.Iterator;

import org.apache.pig.data.Tuple;

public class CountTest extends PigAlgebraic<Long,Long> {
    @Override
    public Class<? extends PigInitial<Long>> getInitialClass() {
        return CtInit.class;
    }

    @Override
    public Class<? extends PigIntermed<Long>> getIntermedClass() {
        return CtInter.class;
    }

    @Override
    public Class<? extends PigFinal<Long,Long>> getFinalClass() {
        return CtFinal.class;
    }

    public static class CtInit extends PigInitial<Long> {
        @Override
        public Long eval(Tuple input) {
            return 1L;
        }
    }

    public static class CtInter extends PigIntermed<Long> {
        @Override
        public Long eval(Iterator<Long> it) {
            long ct = 0;
            while (it.hasNext()) {
                ct += it.next();
            }
            return ct;
        }
    }

    public static class CtFinal extends PigFinal<Long,Long> {
        @Override
        public Long eval(Iterator<Long> it) {
            long ct = 0;
            while (it.hasNext()) {
                ct += it.next();
            }
            return ct;
        }
    }
}