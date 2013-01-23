package org.apache.pig.algebraic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public abstract class PigFinal<InterT, FinT> extends EvalFunc<FinT> {
    public abstract FinT eval(Iterator<InterT> input) throws IOException;

    @Override
    public FinT exec(Tuple input) throws IOException {
        return eval(new IntermedIterator<InterT>(((DataBag)input.get(0)).iterator()));
    }
}
