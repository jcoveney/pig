package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.Arrays;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TupleSchema extends ColumnSchema<Tuple> {
    private ColumnSchema<?>[] columns = null;

    public TupleSchema(String alias) {
        this(alias, (ColumnSchema[])null);
    }

    public TupleSchema(String alias, ColumnSchema<?>... columns) {
        super(alias, DataType.TUPLE);
        this.columns = columns;
    }

    public TupleSchema(ColumnSchema<?>... columns) {
        this(null, columns);
    }

    public TupleSchema add(ColumnSchema<?> column) {
        if (columns == null) {
            return new TupleSchema(getAlias(), column);
        } else {
            ColumnSchema<?>[] tmp = Arrays.copyOf(columns, columns.length+1);
            tmp[tmp.length-1] = column;
            return new TupleSchema(getAlias(), tmp);
        }
    }

    @Override
    public FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException {
        Schema s = null;
        if (columns != null) {
            s = new Schema();
            for (ColumnSchema<?> column : columns) {
                s.add(column.toFieldSchema(fillInNullAliases));
            }
            if (fillInNullAliases) {
                RelationSchema.fixSchemaAliases(s);
            }
        }
        return new FieldSchema(getAlias(), s, getDataType());
    }

    public ColumnSchema<?>[] getColumns() {
        return columns;
    }

    public ColumnSchema<?> getColumn(int i) {
        return columns[i];
    }

    public int size() {
        if (columns == null) {
            return -1;
        } else {
            return columns.length;
        }
    }

    public TupleSchema setAlias(String alias) {
        return new TupleSchema(alias, columns);
    }
}