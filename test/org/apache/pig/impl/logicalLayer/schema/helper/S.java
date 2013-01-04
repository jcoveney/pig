package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.impl.logicalLayer.schema.helper.PrimitiveSchema.*;

public class S {
	private S() {}
	
	public static RelationSchema rel(ColumnSchema... columns) {
		return new RelationSchema(columns);
	}
	
	public static TupleSchema tup(String alias, ColumnSchema... columns) {
		return new TupleSchema(alias, columns);
	}
	
	public static TupleSchema tup(ColumnSchema... columns) {
		return tup(null, columns);
	}
	
	public static BagSchema bag(String alias, TupleSchema tup) {
		return new BagSchema(alias, tup);
	}
	
	public static BagSchema bag(TupleSchema tup) {
		return bag(null, tup);
	}
	
	public static MapSchema map(String alias, ColumnSchema... values) {
		return new MapSchema(alias, values);
	}
	
	public static MapSchema map(ColumnSchema... values) {
		return map(null, values);
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
	
	public static ChararraySchema chararray(String alias) {
		return new ChararraySchema(alias);
	}
	
	public static ChararraySchema charraray() {
		return new ChararraySchema(null);
	}
	
	public static BooleanSchema sboolean(String alias) {
		return new BooleanSchema(alias);
	}
	
	public static BooleanSchema sboolean() {
		return new BooleanSchema(null);
	}
}