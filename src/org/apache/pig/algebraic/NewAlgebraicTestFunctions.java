package org.apache.pig.algebraic;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.pig.algebraic.NewAlgebraicTestFunctions.CountTest.CtFinal;
import org.apache.pig.algebraic.NewAlgebraicTestFunctions.CountTest.CtInter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.builtin.TOP.TupleComparator;

public class NewAlgebraicTestFunctions {
    public static class CountTest extends PigAlgebraic<Long,Long> {
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

    public static class SumTest extends PigAlgebraic<Long,Long> {
        @Override
        public Class<? extends PigInitial<Long>> getInitialClass() {
            // TODO Auto-generated method stub
            return SumInit.class;
        }

        @Override
        public Class<? extends PigIntermed<Long>> getIntermedClass() {
            // TODO Auto-generated method stub
            return CtInter.class;
        }

        @Override
        public Class<? extends PigFinal<Long, Long>> getFinalClass() {
            // TODO Auto-generated method stub
            return CtFinal.class;
        }

        public static class SumInit extends PigInitial<Long> {
            @Override
            public Long eval(Tuple input) throws IOException {
                return ((Number)input.get(0)).longValue();
            }
        }
    }

    public static class TopTest extends PigAlgebraic<DataBag,DataBag> {
        @Override
        public Class<? extends PigInitial<DataBag>> getInitialClass() {
            return null;
        }

        @Override
        public Class<? extends PigIntermed<DataBag>> getIntermedClass() {
            return null;
        }

        @Override
        public Class<? extends PigFinal<DataBag, DataBag>> getFinalClass() {
            return null;
        }

        /**
         * int n = (Integer)in.get(0);
                int fieldNum = (Integer)in.get(1);
                DataBag inputBag = (DataBag)in.get(2);
         */
        public static class TopInit extends PigInitial<Tuple> {
            public Tuple eval(Tuple in) throws IOException {
                return in; //TODO is this safe to do?
            }
        }

        public static class TopInter extends PigIntermed<Tuple> {
            @Override
            public Tuple eval(Iterator<Tuple> input) throws IOException {
                if (!input.hasNext()) {
                    return null;
                }
                Tuple t = input.next();
                PriorityQueue<Tuple> pq = new PriorityQueue<Tuple>((Integer)t.get(0), new TupleComparator((Integer)t.get(1)));
                pq.add(t);
                while (input.hasNext()) {
                    t = input.next();
                    pq.add()
                }
                return null;
            }
        }
    }
}