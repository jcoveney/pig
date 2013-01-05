package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.BooleanSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.BytearraySchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.ChararraySchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.DatetimeSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.DoubleSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.FloatSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.IntSchema;
import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.LongSchema;

public class S {
    private S() {
    }

    public static RelationSchema srel(ColumnSchema<?>... columns) {
        return new RelationSchema(columns);
    }

    public static TupleSchema stup(String alias, ColumnSchema<?>... columns) {
        return new TupleSchema(alias, columns);
    }

    public static TupleSchema stup(ColumnSchema<?>... columns) {
        return stup(null, columns);
    }

    public static BagSchema sbag(String alias, TupleSchema tup) {
        return new BagSchema(alias, tup);
    }

    public static BagSchema sbag(TupleSchema tup) {
        return sbag(null, tup);
    }

    public static BagSchema sbag(String alias) {
        return new BagSchema(alias);
    }

    public static BagSchema sbag() {
        return sbag((String)null);
    }

    public static MapSchema smap(String alias, ColumnSchema<?>... values) {
        return new MapSchema(alias, values);
    }

    public static MapSchema smap(ColumnSchema<?>... values) {
        return smap(null, values);
    }

    public static MapSchema smap(String alias, TupleSchema value) {
        return new MapSchema(alias, value);
    }

    public static MapSchema smap(TupleSchema value) {
        return smap(null, value);
    }

    public static IntSchema sint(String alias) {
        return new IntSchema(alias);
    }

    public static IntSchema sint() {
        return sint(null);
    }

    public static LongSchema slong(String alias) {
        return new LongSchema(alias);
    }

    public static LongSchema slong() {
        return new LongSchema(null);
    }

    public static FloatSchema sfloat(String alias) {
        return new FloatSchema(alias);
    }

    public static FloatSchema sfloat() {
        return sfloat(null);
    }

    public static DoubleSchema sdouble(String alias) {
        return new DoubleSchema(alias);
    }

    public static DoubleSchema sdouble() {
        return sdouble(null);
    }

    public static ChararraySchema schararray(String alias) {
        return new ChararraySchema(alias);
    }

    public static ChararraySchema schararray() {
        return schararray(null);
    }

    public static BooleanSchema sboolean(String alias) {
        return new BooleanSchema(alias);
    }

    public static BooleanSchema sboolean() {
        return sboolean(null);
    }

    public static DatetimeSchema sdatetime(String alias) {
        return new DatetimeSchema(alias);
    }

    public static DatetimeSchema sdatetime() {
        return sdatetime(null);
    }

    public static BytearraySchema sbytearray(String alias) {
        return new BytearraySchema(alias);
    }

    public static BytearraySchema sbytearray() {
        return sbytearray();
    }
}
