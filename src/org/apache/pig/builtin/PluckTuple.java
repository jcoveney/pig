package org.apache.pig.builtin;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

public class PluckTuple extends EvalFunc<Tuple> {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private boolean isInitialized = false;
    private int[] indicesToInclude;
    private String prefix;

    public PluckTuple(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (!isInitialized) {
            List<Integer> indicesToInclude = Lists.newArrayList();
            Schema inputSchema = getInputSchema();
            for (int i = 0; i < inputSchema.size(); i++) {
                String alias = inputSchema.getField(i).alias;
                if (alias.startsWith(prefix)) {
                    indicesToInclude.add(i);
                }
            }
            this.indicesToInclude = new int[indicesToInclude.size()];
            int idx = 0;
            for (int val : indicesToInclude) {
                this.indicesToInclude[idx++] = val;
            }
            isInitialized = true;
        }
        Tuple result = mTupleFactory.newTuple(indicesToInclude.length);
        int idx = 0;
        for (int val : indicesToInclude) {
            result.set(idx++, input.get(val));
        }
        return result;
    }

    public Schema outputSchema(Schema inputSchema) {
        if (!isInitialized) {
            List<Integer> indicesToInclude = Lists.newArrayList();
            for (int i = 0; i < inputSchema.size(); i++) {
                String alias;
                try {
                    alias = inputSchema.getField(i).alias;
                } catch (FrontendException e) {
                    throw new RuntimeException(e); // Should never happen
                }
                if (alias.startsWith(prefix)) {
                    indicesToInclude.add(i);
                }
            }
            this.indicesToInclude = new int[indicesToInclude.size()];
            int idx = 0;
            for (int val : indicesToInclude) {
                this.indicesToInclude[idx++] = val;
            }
            isInitialized = true;
        }
        Schema outputSchema = new Schema();
        for (int val : indicesToInclude) {
            try {
                outputSchema.add(inputSchema.getField(val));
            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return new Schema(new Schema.FieldSchema("plucked", outputSchema, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e); // Should never happen
        }
    }
}