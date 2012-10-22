package org.apache.pig.builtin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;

public class NOW extends EvalFunc<DateTime> {
    private static final Log LOG = LogFactory.getLog(NOW.class);
    private static final BinInterSedes bis = new BinInterSedes();

    private DateTime dateTime;
    private boolean isInitialized = false;

    /**
     * This is a default constructor for Pig reflection purposes. It should
     * never actually be used.
     */
    public NOW() {}

    public NOW(String base64DateTime) {
        byte[] buf = Base64.decodeBase64(base64DateTime);
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        DataInput dis = new DataInputStream(bais);
        try {
            dateTime = (DateTime) bis.readDatum(dis);
            bais.close();
        } catch (ExecException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DateTime exec(Tuple input) throws IOException {
        if (!isInitialized) {
            if (dateTime == null) {
                LOG.error("NOW() should not be called without having been constrcuted with serialized DateTime");
            }
            isInitialized  = false;
        }
        return dateTime;
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() {
        DateTime now = new DateTime();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        try {
            bis.writeDatum(dos, now, DataType.DATETIME);
            baos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String encoded = Base64.encodeBase64URLSafeString(baos.toByteArray());
        FuncSpec fs = new FuncSpec(getClass().getName(), new String[] {encoded}, new Schema());
        return ImmutableList.of(fs);
    }
}
