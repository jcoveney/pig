package org.apache.pig.impl.logicalLayer.schema.helper;

import static org.junit.Assert.assertEquals;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

public class TestSchemaHelpers {
	@Test
	public void test() throws Exception {
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
}
