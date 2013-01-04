package org.apache.pig.impl.logicalLayer.schema.helper;

import static org.junit.Assert.assertEquals;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

public class TestSchemaHelpers {
	@Test
	public void testWithoutAutofillSchema() throws Exception {
		Schema s1 = Utils.getSchemaFromString("a:int,b:int,c:int");
		Schema s2 = S.rel(S.sint("a"), S.sint("b"), S.sint("c")).toSchema();
		assertEquals(s1, s2);

		s1 = Utils.getSchemaFromString("a:int, b:long, c:boolean, d:chararray, e:float, f:double");
		s2 = S.rel(S.sint("a"), S.slong("b"), S.sboolean("c"), S.chararray("d"), S.sfloat("e"), S.sdouble("f")).toSchema();
		assertEquals(s1, s2);

		s1 = Utils.getSchemaFromString("a:int, t:tuple(a:int, b:long, c:boolean, d:chararray, e:float, f:double), c:bag{t:tuple(a:int, b:long, c:boolean, d:chararray, e:float, f:double)}, d:map[(a:int, b:long, c:boolean, d:chararray, e:float, f:double)]");
		TupleSchema ts = S.tup("t", S.sint("a"), S.slong("b"), S.sboolean("c"), S.chararray("d"), S.sfloat("e"), S.sdouble("f"));
		s2 = S.rel(S.sint("a"), ts, S.bag("c", ts), S.map("d", ts.getColumns())).toSchema();
		assertEquals(s1, s2);
	}

	@Test
	public void testWithAutofillSchema() throws Exception {
		Schema s1 = Utils.getSchemaFromString("int,int,int");
		Schema s2 = S.rel(S.sint(), S.sint(), S.sint()).toSchema();
		assertEquals(s1, s2);

		s1 = Utils.getSchemaFromString("int, long, boolean, chararray, float, double");
		s2 = S.rel(S.sint(), S.slong(), S.sboolean(), S.chararray(), S.sfloat(), S.sdouble()).toSchema();
		assertEquals(s1, s2);

		s1 = Utils.getSchemaFromString("int,tuple(int, long, boolean, chararray, float, double), bag{tuple(int, long, boolean, chararray, float, double)}, map[(int, long, boolean, chararray, float, double)]");
		TupleSchema ts = S.tup(S.sint(), S.slong(), S.sboolean(), S.chararray(), S.sfloat(), S.sdouble());
		s2 = S.rel(S.sint(), ts, S.bag(ts), S.map(ts.getColumns())).toSchema();
		assertEquals(s1, s2);
	}

	@Test
	public void testFromSchemaConversion() throws Exception {
	  String[] schemas = {
	      "a:int",
	      "int",
	      "a:int, b:long, c:boolean, d:chararray, e:float, f:double",
	      "a:int, t:tuple(a:int, b:long, c:boolean, d:chararray, e:float, f:double), c:bag{t:tuple(a:int, b:long, c:boolean, d:chararray, e:float, f:double)}, d:map[(a:int, b:long, c:boolean, d:chararray, e:float, f:double)]",
	      "int, long, boolean, chararray, float, double",
	      "int,tuple(int, long, boolean, chararray, float, double), bag{tuple(int, long, boolean, chararray, float, double)}, map[(int, long, boolean, chararray, float, double)]",
	      "t:tuple()",
	      "()",
	      "b:bag{}",
	      "{}",
	      "{()}",
	      "[]",
	      "m:map[]"
	  };
	  for (String schema : schemas) {
	    toAndFromSchema(schema);
	  }
	}

	private void toAndFromSchema(String schema) throws Exception {
	  Schema s = Utils.getSchemaFromString(schema);
	  RelationSchema rs = RelationSchema.fromSchema(s);
	  assertEquals(s, rs.toSchema(false));
	}
}