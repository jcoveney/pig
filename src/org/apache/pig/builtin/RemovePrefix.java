package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

@FlattenOutput(prefix = false)
public class RemovePrefix extends EvalFunc<Tuple> {
    public Tuple exec(Tuple input) throws IOException {
        return input;
    }

    public Schema outputSchema(Schema inputSchema) {
        return inputSchema;
    }
}
