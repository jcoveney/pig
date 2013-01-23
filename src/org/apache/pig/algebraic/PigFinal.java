package org.apache.pig.algebraic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public abstract class PigFinal<InterT, FinT> extends EvalFunc<FinT> {
    public abstract FinT eval(Iterator<InterT> input);

    @Override
    public Tuple exec(Tuple input) throws IOException {
        DataBag bag = (DataBag)input.get(0);
        Tuple t = mTupleFactory.newTuple(1);
        t.set(0, eval(new IntermedIterator<InterT>(bag.iterator())));
        return t;
    }
}
