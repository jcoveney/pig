package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;

import org.apache.pig.builtin.COUNT;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;

public class NonAlgNonAccCount extends EvalFunc<Long> {
    private COUNT c = new COUNT();

    public Long exec(Tuple input) throws IOException {
        return c.exec(input);
    }
}
