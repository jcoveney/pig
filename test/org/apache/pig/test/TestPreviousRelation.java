package org.apache.pig.test;

import static org.junit.Assert.assertTrue;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Before;
import org.junit.Test;

public class TestPreviousRelation {
    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
      pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test(expected = FrontendException.class)
    public void testPreviousRelationWithNothingExisting() throws Exception {
        try {
            pigServer.registerQuery("a = foreach @ generate *;");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Asked for last relation -- no relations have been defined"));
            throw e;
        }
    }
}
