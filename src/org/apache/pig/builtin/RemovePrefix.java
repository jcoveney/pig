package org.apache.pig.builtin;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

@FlattenOutput(prefix = false)
public class RemovePrefix extends EvalFunc<Tuple> {
    private static final Pattern prefix = Pattern.compile(".*::(.*)");

    public Tuple exec(Tuple input) throws IOException {
        return input;
    }

    public Schema outputSchema(Schema inputSchema) {
        for (Schema.FieldSchema fs : inputSchema.getFields()) {
            Matcher m = prefix.matcher(fs.alias);
            if (m.matches()) {
                fs.alias = m.group(1);
            }
        }
        try {
            return new Schema(new Schema.FieldSchema(null, inputSchema, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e); //shouldn't happen
        }
    }
}
