package org.apache.pig.data;

import static junit.framework.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestSchemaTuple {
    private Properties props;
    private Configuration conf;
    private PigContext pigContext;
    private static final BinInterSedes bis = new BinInterSedes();

    @Before
    public void perTestInitialize() {
        SchemaTupleFrontend.reset();
        SchemaTupleBackend.reset();

        props = new Properties();
        props.setProperty(SchemaTupleBackend.SHOULD_GENERATE_KEY, "true");

        conf = ConfigurationUtil.toConfiguration(props);
        pigContext = new PigContext(ExecType.LOCAL, props);
    }

    @Test
    public void testCompileAndResolve() throws Exception {
        //frontend
        Schema udfSchema = Utils.getSchemaFromString("a:int");
        boolean isAppendable = false;
        GenContext context = GenContext.UDF;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:long");
        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)");
        isAppendable = false;
        context = GenContext.LOAD;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("((a:int,b:int),(a:int,b:int),(a:int,b:int)),((a:int,b:int),(a:int,b:int),(a:int,b:int))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("boolean, boolean, boolean, boolean, boolean, boolean"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, b:bag{(int,int,int)}");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        // this compiles and "ships"
        SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

        //backend
        SchemaTupleBackend.initialize(conf, true);

        udfSchema = Utils.getSchemaFromString("a:int");
        isAppendable = false;
        context = GenContext.UDF;
        TupleFactory tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        context = GenContext.MERGE_JOIN;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:long");
        context = GenContext.UDF;
        isAppendable = true;

        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)");
        isAppendable = false;
        context = GenContext.LOAD;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("(a:chararray)");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("((a:int,b:int),(a:int,b:int),(a:int,b:int)),((a:int,b:int),(a:int,b:int),(a:int,b:int))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("boolean, boolean, boolean, boolean, boolean, boolean"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))"
                +"int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double,"
                +"(int, long, chararray, boolean, bytearray, float, double))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = true;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, b:bag{(int,int,int)}");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);
    }

    private void putThroughPaces(TupleFactory tfPrime, Schema udfSchema, boolean isAppendable) throws IOException {
        SchemaTupleFactory tf = (SchemaTupleFactory)tfPrime;
        assertNotNull(tf);
        assertTrue(tf instanceof SchemaTupleFactory);
        assertTrue(tf.newTuple() instanceof SchemaTuple);
        if (isAppendable) {
            assertTrue(tf.newTuple() instanceof AppendableSchemaTuple);
        }

        testNotAppendable(tf, udfSchema);
        if (isAppendable) {
            testAppendable(tf, udfSchema);
        }
    }

    private void testAppendable(TupleFactory tf, Schema udfSchema) {
        SchemaTuple<?> st = (SchemaTuple<?>) tf.newTuple();

        st.append("woah");
        assertEquals(udfSchema.size() + 1, st.size());

    }

    private void testNotAppendable(SchemaTupleFactory tf, Schema udfSchema) throws IOException {
        SchemaTuple<?> st = (SchemaTuple<?>) tf.newTuple();
        Schema.equals(udfSchema, st.getSchema(), false, true);

        assertEquals(udfSchema.size(), st.size());

        shouldAllBeNull(tf);

        copyThenCompare(tf);

        testSerDe(tf);
    }

    private void copyThenCompare(SchemaTupleFactory tf) throws ExecException {
        SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
        SchemaTuple<?> st2 = (SchemaTuple<?>)tf.newTuple();
        fillWithData(st);
        st2.set(st);
        assertTrue(st.equals(st2));
        assertEquals(st.compareTo(st2), 0);
        st.set(0, null);
        assertFalse(st.equals(st2));
        assertEquals(st.compareTo(st2), -1);
        assertTrue(st.isNull(0));
        st2.set(0, null);
        assertTrue(st.equals(st2));
        assertEquals(st.compareTo(st2), 0);
    }

    /**
     * This ensures that a fresh Tuple out of a TupleFactory
     * will be full of null fields.
     * @param   tf a TupleFactory
     * @throws  ExecException
     */
    private void shouldAllBeNull(TupleFactory tf) throws ExecException {
        Tuple t = tf.newTuple();
        for (Object o : t) {
            assertNull(o);
        }
        for (int i = 0; i < t.size(); i++) {
            assertNull(t.get(i));
            assertTrue(t.isNull(i));
        }
    }

    private void fillWithData(SchemaTuple<?> st) throws ExecException {
        Schema udfSchema = st.getSchema();
        int pos = 0;
        for (FieldSchema fs : udfSchema.getFields()) {
            Object val;
            if (fs.type == DataType.TUPLE) {
                val = SchemaTupleFactory
                            .getInstance(fs.schema, false, GenContext.FORCE_LOAD)
                            .newTuple();
                fillWithData((SchemaTuple<?>)val);
            } else {
                val = randData(fs);
            }
            st.set(pos++, val);
        }
    }

    private Random r = new Random(100L);

    private Object randData(FieldSchema fs) throws ExecException {
        switch (fs.type) {
        case DataType.BOOLEAN: return r.nextBoolean();
        case DataType.BYTEARRAY: return new DataByteArray(new BigInteger(130, r).toByteArray());
        case DataType.CHARARRAY: return new BigInteger(130, r).toString(32);
        case DataType.INTEGER: return r.nextInt();
        case DataType.LONG: return r.nextLong();
        case DataType.FLOAT: return r.nextFloat();
        case DataType.DOUBLE: return r.nextDouble();
        case DataType.BAG:
            DataBag db = BagFactory.getInstance().newDefaultBag();
            int sz = r.nextInt(100);
            for (int i = 0; i < sz; i++) {
                int tSz = r.nextInt(10);
                Tuple t = TupleFactory.getInstance().newTuple(tSz);
                for (int j = 0; j < tSz; j++) {
                    t.set(j, r.nextInt());
                }
                db.add(t);
            }
            return db;
        default: throw new RuntimeException("Cannot generate data for given FieldSchema: " + fs);
        }
    }

    public void testTypeAwareGetSetting(TupleFactory tf) throws ExecException {
        SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
        checkNullGetThrowsError(st);
    }

    private void checkNullGetThrowsError(SchemaTuple<?> st) throws ExecException {
        Schema schema = st.getSchema();
        int i = 0;
        for (Schema.FieldSchema fs : schema.getFields()) {
            boolean fieldIsNull = false;
            try {
                switch (fs.type) {
                case DataType.BOOLEAN: st.getBoolean(i); break;
                case DataType.BYTEARRAY: st.getBytes(i); break;
                case DataType.CHARARRAY: st.getString(i); break;
                case DataType.INTEGER: st.getInt(i); break;
                case DataType.LONG: st.getLong(i); break;
                case DataType.FLOAT: st.getFloat(i); break;
                case DataType.DOUBLE: st.getDouble(i); break;
                case DataType.TUPLE: st.getTuple(i); break;
                case DataType.BAG: st.getDataBag(i); break;
                default: throw new RuntimeException("Unsupported FieldSchema in SchemaTuple: " + fs);
                }
            } catch (FieldIsNullException e) {
                fieldIsNull = true;
            }
            assertTrue(fieldIsNull);
            i++;
        }
    }

    public void testSerDe(TupleFactory tf) throws IOException {
        List<Tuple> written = new ArrayList<Tuple>(4096);

        File temp = File.createTempFile("tmp", "tmp");
        FileOutputStream fos = new FileOutputStream(temp);
        DataOutput dos = new DataOutputStream(fos);

        for (int i = 0; i < written.size(); i++) {
            SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
            fillWithData(st);
            bis.writeDatum(dos, st);
            written.add(st);
        }
        fos.close();

        FileInputStream fis = new FileInputStream(temp);
        DataInput din = new DataInputStream(fis);
        for (int i = 0; i < written.size(); i++) {
            SchemaTuple<?> st = (SchemaTuple<?>)bis.readDatum(din);
            assertEquals(written.get(i), st);
        }
        fis.close();
    }

    @Test
    public void testFRJoinWithSchemaTuple() throws Exception {
        testJoinType("replicated");
    }

    @Test
    public void testMergeJoinWithSchemaTuple() throws Exception {
        testJoinType("merge");
    }

    public void testJoinType(String joinType) throws Exception {
        Properties props = PropertiesUtil.loadDefaultProperties();
        props.setProperty("pig.schematuple", "true");
        PigServer pigServer = new PigServer(ExecType.LOCAL, props);

        Data data = resetData(pigServer);

        data.set("foo1",
            tuple(1),
            tuple(2),
            tuple(3),
            tuple(4),
            tuple(5),
            tuple(6),
            tuple(7),
            tuple(8),
            tuple(9),
            tuple(10)
            );

        data.set("foo2",
            tuple(1),
            tuple(2),
            tuple(3),
            tuple(4),
            tuple(5),
            tuple(6),
            tuple(7),
            tuple(8),
            tuple(9),
            tuple(10)
            );

        pigServer.registerQuery("A = LOAD 'foo1' USING mock.Storage() as (x:int);");
        pigServer.registerQuery("B = LOAD 'foo1' USING mock.Storage() as (x:int);");
        pigServer.registerQuery("C = ORDER A BY x ASC;");
        pigServer.registerQuery("D = ORDER B BY x ASC;");
        pigServer.registerQuery("E = JOIN C by x, D by x using '"+joinType+"';");

        Iterator<Tuple> out = pigServer.openIterator("E");
        for (int i = 1; i <= 10; i++) {
            assertEquals(tuple(i, i), out.next());
        }
        assertFalse(out.hasNext());

        pigServer.registerQuery("STORE E INTO 'bar' USING mock.Storage();");

        out = data.get("bar").iterator();
        for (int i = 1; i <= 10; i++) {
            assertEquals(tuple(i, i), out.next());
        }
        assertFalse(out.hasNext());
    }

}
