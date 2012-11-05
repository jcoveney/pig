package org.apache.pig.builtin;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.joda.time.DateTime;
import org.junit.Test;

public class TestCurrentTime {
    @Test
    public void testCurrentTime() throws Exception {
        DateTime dt = new DateTime();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        new BinInterSedes().writeDatum(dos, dt, DataType.DATETIME);
        baos.close();
        String encoded = Base64.encodeBase64URLSafeString(baos.toByteArray());
        CurrentTime ct = new CurrentTime(encoded);
        assertEquals(dt, ct.exec(null));
    }

    @Test(expected = ExecException.class)
    public void testCurrentTimeExecDefaultConstructorFails() throws Exception {
        CurrentTime ct = new CurrentTime();
        ct.exec(null);
    }
}
