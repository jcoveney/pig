package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.joda.time.DateTime;

public abstract class PrimitiveSchema<T> extends ColumnSchema<T> {
    public PrimitiveSchema(String alias, byte dataType) {
        super(alias, dataType);
    }

    @Override
    public FieldSchema toFieldSchema(boolean fillInNullAliases) {
        return new FieldSchema(getAlias(), getDataType());
    }

    public static class IntSchema extends PrimitiveSchema<Integer> {
        public IntSchema(String alias) {
            super(alias, DataType.INTEGER);
        }
    }

    public static class LongSchema extends PrimitiveSchema<Long> {
        public LongSchema(String alias) {
            super(alias, DataType.LONG);
        }
    }

    public static class FloatSchema extends PrimitiveSchema<Float> {
        public FloatSchema(String alias) {
            super(alias, DataType.FLOAT);
        }
    }

    public static class DoubleSchema extends PrimitiveSchema<Double> {
        public DoubleSchema(String alias) {
            super(alias, DataType.DOUBLE);
        }
    }

    public static class BooleanSchema extends PrimitiveSchema<Boolean> {
        public BooleanSchema(String alias) {
            super(alias, DataType.BOOLEAN);
        }
    }

    public static class ChararraySchema extends PrimitiveSchema<String> {
        public ChararraySchema(String alias) {
            super(alias, DataType.CHARARRAY);
        }
    }

    public static class DatetimeSchema extends PrimitiveSchema<DateTime> {
        public DatetimeSchema(String alias) {
            super(alias, DataType.DATETIME);
        }
    }

    public static class BytearraySchema extends PrimitiveSchema<DataByteArray> {
        public BytearraySchema(String alias) {
            super(alias, DataType.BYTEARRAY);
        }
    }
}