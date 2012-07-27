package org.apache.pig.data;

import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestSchemaDataBag {
    private Properties props;
    private Configuration conf;
    private PigContext pigContext;

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
    public void testBagWithIntColumns() throws Exception {
        //frontend
        Schema udfSchema = Utils.getSchemaFromString("a:int, b:int, c:int");
        boolean isAppendable = false;
        GenContext context = GenContext.UDF;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        // this compiles and "ships"
        SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

        //backend
        SchemaTupleBackend.initialize(conf, ExecType.LOCAL);
        SchemaTupleFactory tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        TupleFactory defaultTF = TupleFactory.getInstance();

        SchemaDataBag sdb = new SchemaDataBag(tf);
        Random r = new Random(100L);
        Set<Tuple> tuples = Sets.newHashSet();

        for (int i = 0; i < 10; i++) {
            Tuple t = defaultTF.newTuple(udfSchema.size());
            for (int j = 0; j < udfSchema.size(); j++) {
                t.set(j, Integer.valueOf(r.nextInt()));
            }
            sdb.add(t);
            tuples.add(t);
        }

        for (Tuple t : sdb) {
            tuples.remove(t);
        }

        assertTrue(tuples.isEmpty());

    }
}
