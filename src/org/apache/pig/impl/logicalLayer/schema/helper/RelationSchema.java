package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.BooleanSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.BytearraySchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.ChararraySchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.DatetimeSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.DoubleSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.FloatSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.IntSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.LongSchema;
import org.apache.pig.impl.util.NumValCarrier;

//TODO implement toString
public class RelationSchema implements Iterable<ColumnSchema<?>> {
    private ColumnSchema<?>[] columns;

    public RelationSchema(ColumnSchema<?>... columns) {
        this.columns = columns;
    }

    public RelationSchema add(ColumnSchema<?> column) {
        if (columns == null) {
            return new RelationSchema(column);
        } else {
            ColumnSchema<?>[] tmp = Arrays.copyOf(columns, columns.length+1);
            tmp[tmp.length-1] = column;
            return new RelationSchema(tmp);
        }
    }

    public Schema toSchema() {
        return toSchema(true);
    }

    public Schema toSchema(boolean fillInNullAliases) {
        Schema s = new Schema();
        for (ColumnSchema<?> column : columns) {
            try {
                s.add(column.toFieldSchema(fillInNullAliases));
            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }
        }
        if (fillInNullAliases) {
            fixSchemaAliases(s);
        }
        return s;
    }

    public ColumnSchema<?>[] getColumns() {
        return columns;
    }

    public ColumnSchema<?> getColumn(int i) {
        return columns[i];
    }

    public int size() {
        return columns.length;
    }

    public static void fixSchemaAliases(Schema s) {
        NumValCarrier nvc = new NumValCarrier();
        for (FieldSchema fs : s.getFields()) {
            if (fs.alias == null) {
                fs.alias = nvc.makeNameFromDataType(fs.type);
            }
        }
    }

    public static RelationSchema fromSchema(Schema schema) {
        RelationSchema rs = new RelationSchema();
        for (FieldSchema fs : schema.getFields()) {
            rs.add(fromFieldSchema(fs));
        }
        return rs;
    }

    public static ColumnSchema<?> fromFieldSchema(FieldSchema fieldSchema) {
        switch (fieldSchema.type) {
        case DataType.BAG: {
            Schema innerSchema = fieldSchema.schema;
            if (innerSchema != null) {
                try {
                    return new BagSchema(fieldSchema.alias,
                            (TupleSchema)fromFieldSchema(fieldSchema.schema.getField(0)));
                } catch (FrontendException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return new BagSchema(fieldSchema.alias);
            }
        }
        case DataType.MAP: {
            Schema innerSchema = fieldSchema.schema;
            if (innerSchema != null) {
                try {
                    return new MapSchema(fieldSchema.alias,
                            (TupleSchema)fromFieldSchema(fieldSchema.schema.getField(0)));
                } catch (FrontendException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return new MapSchema(fieldSchema.alias);
            }
        }
        case DataType.TUPLE:
            Schema innerSchema = fieldSchema.schema;
            if (innerSchema != null) {
                ColumnSchema<?>[] columns = new ColumnSchema[innerSchema.size()];
                int idx = 0;
                for (FieldSchema fs : innerSchema.getFields()) {
                    columns[idx++] = fromFieldSchema(fs);
                }
                return new TupleSchema(fieldSchema.alias, columns);
            } else {
                return new TupleSchema(fieldSchema.alias);
            }
        case DataType.INTEGER: return new IntSchema(fieldSchema.alias);
        case DataType.LONG: return new LongSchema(fieldSchema.alias);
        case DataType.FLOAT: return new FloatSchema(fieldSchema.alias);
        case DataType.DOUBLE: return new DoubleSchema(fieldSchema.alias);
        case DataType.BOOLEAN: return new BooleanSchema(fieldSchema.alias);
        case DataType.CHARARRAY: return new ChararraySchema(fieldSchema.alias);
        case DataType.DATETIME: return new DatetimeSchema(fieldSchema.alias);
        case DataType.BYTEARRAY: return new BytearraySchema(fieldSchema.alias);
        default:
            throw new RuntimeException("Unsupported type in fieldSchema: " + fieldSchema);
        }
    }

    @Override
    public Iterator<ColumnSchema<?>> iterator() {
        return new Iterator<ColumnSchema<?>>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < columns.length;
            }

            @Override
            public ColumnSchema<?> next() {
                return columns[i];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}