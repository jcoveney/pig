package org.apache.pig.algebraic;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

//TODO make the output more strongly typed?
//TODO need to decide if I want it to be an EvalFunc or not...I think so, to leverage existing infra
public abstract class PigInitial<T> extends EvalFunc<Tuple> {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    public abstract T eval(Tuple input) throws IOException;

    @Override
    public Tuple exec(Tuple input) throws IOException {
        Tuple t = mTupleFactory.newTuple(1);
        t.set(0, eval(((DataBag)input.get(0)).iterator().next()));
        return t;
    }
}
