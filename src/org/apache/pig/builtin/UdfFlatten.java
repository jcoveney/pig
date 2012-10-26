package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

@FlattenOutput(prefix = false)
public class UdfFlatten extends EvalFunc<Object> {
    @Override
    public Object exec(Tuple input) throws IOException {
        if (input.size() == 1) {
            Object first = input.get(0);
            if (first instanceof Tuple || first instanceof DataBag) {
                return first;
            }
        }
        return input;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (input.size() == 1) {
                FieldSchema first = input.getField(0);
                byte type = first.type;
                switch (type) {
                case DataType.TUPLE: return first.schema;
                case DataType.BAG: return first.schema.getField(0).schema;
                }
            }
            return new Schema(new FieldSchema(null, input, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
