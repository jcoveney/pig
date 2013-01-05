package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.Iterator;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Function;

public class VerifySchema {
    private VerifySchema() {
    }

    public static boolean verify(Schema input, VSType... expectedColumns) {
        return verify(RelationSchema.fromSchema(input), expectedColumns);
    }

    public static boolean verify(RelationSchema input, VSType... expectedColumns) {
        Iterator<ColumnSchema<?>> it = input.iterator();
        for (VSType vs : expectedColumns) {
            if (!vs.verifyNext(it)) {
                return false;
            }
        }
        return true;
    }

    public static enum VSType {
        BAG(DataType.BAG),
        TUPLE(DataType.TUPLE),
        MAP(DataType.MAP),
        INT(DataType.INTEGER),
        LONG(DataType.LONG),
        FLOAT(DataType.FLOAT),
        DOUBLE(DataType.DOUBLE),
        CHARARRAY(DataType.CHARARRAY),
        BOOLEAN(DataType.BOOLEAN),
        BYTEARRAY(DataType.BYTEARRAY),
        DATETIME(DataType.DATETIME),
        BAG_WITH_STUFF(new Function<Iterator<ColumnSchema<?>>,Boolean>() {
            @Override
            public Boolean apply(Iterator<ColumnSchema<?>> in) {
                ColumnSchema<?> next = in.next();
                if (!next.isBag()) {
                    return false;
                }
                return ((BagSchema)next).getTupleSchema().size() > 1;
            }
        }),
        VARARG_0(new VarargFunction(0)),
        VARARG_1(new VarargFunction(1));

        private static class VarargFunction implements Function<Iterator<ColumnSchema<?>>,Boolean> {
            private int required;

            public VarargFunction(int required) {
                this.required = required;
            }

            @Override
            public Boolean apply(Iterator<ColumnSchema<?>> it) {
                for (int i = 0; i < required; i++) {
                    if (!it.hasNext()) {
                        return false;
                    }
                    it.next();
                }
                while (it.hasNext()) {
                    it.next();
                }
                return true;
            }
        }

        private Function<Iterator<ColumnSchema<?>>,Boolean> verifier;

        VSType(final byte type) {
            verifier = new Function<Iterator<ColumnSchema<?>>,Boolean>() {
                @Override
                public Boolean apply(Iterator<ColumnSchema<?>> it) {
                    return it.next().getDataType() == type;
                }
            };
        }

        VSType(Function<Iterator<ColumnSchema<?>>,Boolean> verifier) {
            this.verifier = verifier;
        }

        public boolean verifyNext(Iterator<ColumnSchema<?>> columnIt) {
            if (!columnIt.hasNext()) {
                return false;
            }
            return verifier.apply(columnIt);
        }
    }
}
