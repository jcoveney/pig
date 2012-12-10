package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestRemovePrefix {
    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void test() throws Exception {
        Data data = resetData(pigServer);

        data.set("a",
            Utils.getSchemaFromString("x1,y1,z1"),
            tuple(1, 2, 3),
            tuple(1, 2, 3),
            tuple(1, 2, 3)
            );
        data.set("b",
            Utils.getSchemaFromString("x2,y2,z2"),
            tuple(1, 2, 3),
            tuple(1, 2, 3),
            tuple(1, 2, 3)
            );
        data.set("c",
            Utils.getSchemaFromString("x3,y3,z3"),
            tuple(1, 2, 3),
            tuple(1, 2, 3),
            tuple(1, 2, 3)
            );
        pigServer.registerQuery("a = load 'a' using mock.Storage();");
        pigServer.registerQuery("b = load 'b' using mock.Storage();");
        pigServer.registerQuery("c = load 'c' using mock.Storage();");
        pigServer.registerQuery("d = join a by x1, b by x2;");
        pigServer.registerQuery("e = foreach d generate RemovePrefix(*);");
        assertEquals(Utils.getSchemaFromString("x1,y1,z1,x2,y2,z2"), pigServer.dumpSchema("e"));
        Iterator<Tuple> it = pigServer.openIterator("e");
        for(int i = 0; i < 9; i++) {
            assertTrue(it.hasNext());
            assertEquals(tuple(1, 2, 3, 1, 2, 3), it.next());
        }
        assertFalse(it.hasNext());

        pigServer.registerQuery("f = join d by x1, c by x3;");
        pigServer.registerQuery("g = foreach f generate RemovePrefix(*);");
        assertEquals(Utils.getSchemaFromString("x1,y1,z1,x2,y2,z2,x3,y3,z3"), pigServer.dumpSchema("g"));
        it = pigServer.openIterator("g");
        for(int i = 0; i < 27; i++) {
            assertTrue(it.hasNext());
            assertEquals(tuple(1, 2, 3, 1, 2, 3, 1, 2, 3), it.next());
        }
        assertFalse(it.hasNext());
    }
}
