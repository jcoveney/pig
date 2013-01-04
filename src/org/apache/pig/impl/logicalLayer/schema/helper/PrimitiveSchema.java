package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public abstract class PrimitiveSchema extends ColumnSchema {
	public PrimitiveSchema(String alias, byte dataType) {
		super(alias, dataType);
	}

	public FieldSchema toFieldSchema() {
		return new FieldSchema(getAlias(), getDataType());
	}
	
	public static class IntSchema extends PrimitiveSchema {
		public IntSchema(String alias) {
			super(alias, DataType.INTEGER);
		}
	}
	
	public static class LongSchema extends PrimitiveSchema {
		public LongSchema(String alias) {
			super(alias, DataType.LONG);
		}
	}
	
	public static class FloatSchema extends PrimitiveSchema {
		public FloatSchema(String alias) {
			super(alias, DataType.FLOAT);
		}
	}
	
	public static class DoubleSchema extends PrimitiveSchema {
		public DoubleSchema(String alias) {
			super(alias, DataType.DOUBLE);
		}
	}
	
	public static class BooleanSchema extends PrimitiveSchema {
		public BooleanSchema(String alias) {
			super(alias, DataType.BOOLEAN);
		}
	}
	
	public static class ChararraySchema extends PrimitiveSchema {
		public ChararraySchema(String alias) {
			super(alias, DataType.CHARARRAY);
		}
	}
	
	public static class DatetimeSchema extends PrimitiveSchema {
		public DatetimeSchema(String alias) {
			super(alias, DataType.DATETIME);
		}
	}
	
}
