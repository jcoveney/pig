package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestCurrentTime {
    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testAllCurrentTimeSame() throws Exception {
        Data data = resetData(pigServer);

        List<Tuple> justSomeRows = Lists.newArrayList();
        for (int i = 0; i < 1000; i++) {
            justSomeRows.add(tuple(i));
        }

        data.set("justSomeRows", justSomeRows);

        pigServer.registerQuery("A = load 'justSomeRows' using mock.Storage();");
        pigServer.registerQuery("B = foreach A generate CurrentTime();");
        pigServer.registerQuery("C = foreach (group B by $0) generate COUNT($1);");
        Iterator<Tuple> it = pigServer.openIterator("C");
        assertTrue(it.hasNext());
        Tuple t = it.next();
        assertEquals(1000L, t.get(0));
        assertFalse(it.hasNext());
    }
}
