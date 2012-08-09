package org.apache.pig.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


public class CaliperBenchmarking {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final BinInterSedes bis = new BinInterSedes();

    private static final boolean isAppendable = false;
    private static final GenContext context = GenContext.UDF;
    private static final Properties props = new Properties() {
        private static final long serialVersionUID = 1L;
    {
        setProperty(SchemaTupleBackend.SHOULD_GENERATE_KEY, "true");
    }};
    private static final Configuration conf = ConfigurationUtil.toConfiguration(props);

    public static class SchemaBagBenchmarking extends SimpleBenchmark {
        @Param int size;
        @Param int elements;
        private PigContext pigContext;
        private SchemaTupleFactory stf;
        private Random r;

//        private DataBag bag1;
        private DataBag bag2;
        private DataBag bag3;
        private DataBag bag4;
        private DataBag bag5;
//        private DataBag spilledBag1;
        private DataBag spilledBag2;
        private DataBag spilledBag3;
        private DataBag spilledBag4;
        private DataBag spilledBag5;
//        private File f1;
        private File f2;
        private File f3;
        private File f4;
        private File f5;

        @Override
        protected void setUp() {
            try {
                r = new Random(100L);

                pigContext = new PigContext(ExecType.LOCAL, props);

                List<String> list = Lists.newArrayListWithCapacity(size);
                for (int i = 0; i < size; i++) {
                    list.add("val_" + i + ":int");
                }
                Schema udfSchema = Utils.getSchemaFromString(Joiner.on(",").join(list));
                SchemaTupleFrontend.reset();
                SchemaTupleBackend.reset();
                //frontend
                SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

                // this compiles and "ships"
                SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

                //backend
                SchemaTupleBackend.initialize(conf, ExecType.LOCAL);

                stf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);

//                bag1 = new SchemaDataBag(stf);
                bag2 = new DefaultDataBag();
                bag3 = new DefaultDataBag();
                bag4 = new NewDefaultDataBag();
                bag5 = new NewDefaultDataBag();

//                spilledBag1 = new SchemaDataBag(stf);
                spilledBag2 = new DefaultDataBag();
                spilledBag3 = new DefaultDataBag();
                spilledBag4 = new NewDefaultDataBag();
                spilledBag5 = new NewDefaultDataBag();

                for (int i = 0; i < elements; i++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    SchemaTuple<?> st = stf.newTuple();
                    for (int j = 0; j < size; j++) {
                        int val = r.nextInt();
                        t.set(j, Integer.valueOf(val));
                        st.setInt(j, val);
                    }
//                    bag1.add(t);
                    bag2.add(t);
                    bag3.add(st);
                    bag4.add(t);
                    bag5.add(st);

//                    spilledBag1.add(t);
                    spilledBag2.add(t);
                    spilledBag3.add(st);
                    spilledBag4.add(t);
                    spilledBag5.add(st);
                }

                //This is only really necessary for the SchemaDataBag. Pulling the iterator
                //forces the spill to entirely go to disk
//                spilledBag1.iterator();
                spilledBag2.iterator();
                spilledBag3.iterator();
                spilledBag4.iterator();
                spilledBag5.iterator();

//                spilledBag1.spill();
                spilledBag2.spill();
                spilledBag3.spill();
                spilledBag4.spill();
                spilledBag5.spill();

//                f1 = serializeGetFile(bag1);
                f2 = serializeGetFile(bag2);
                f3 = serializeGetFile(bag3);
                f4 = serializeGetFile(bag4);
                f5 = serializeGetFile(bag5);

//                f1.deleteOnExit();
                f2.deleteOnExit();
                f3.deleteOnExit();
                f4.deleteOnExit();
                f5.deleteOnExit();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private long serialize(DataBag b) {
            try {
                File f = serializeGetFile(b);
                long len = f.length();
                f.delete();
                return len;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private File serializeGetFile(DataBag b) {
            try {
                File f = File.createTempFile("tmp", "tmp");
                DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f), 4 * 1024 * 1024));
                b.write(os);
                os.close();
                return f;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private long deserialize(File f, DataBag newBag) {
            try {
                DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(f), 4 * 1024 * 1024));
                newBag.readFields(is);
                is.close();
                return newBag.size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private long serDe(DataBag b, DataBag newBag) {
            try {
                File f = serializeGetFile(b);
                long res = deserialize(f, newBag);
                f.delete();
                return res;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Add
 /*
        public long timeAddSbAndTuple(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                SchemaDataBag bag = new SchemaDataBag(stf);
                for (int j = 0; j < elements; j++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }

        public long timeAddSbAndSt(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                SchemaDataBag bag = new SchemaDataBag(stf);
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }
*/
        public long timeAddBagAndTuple(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new DefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }

        public long timeAddBagAndSt(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new DefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }

        public long timeAddNewBagAndTuple(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new NewDefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }

        public long timeAddNewBagAndSt(int reps) {
            long totalSize = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new NewDefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                totalSize += bag.size();
            }
            return totalSize;
        }

        // Add and Spill
/*
        public long timeAddAndSpillSbAndTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                SchemaDataBag bag = new SchemaDataBag(stf);
                for (int j = 0; j < elements; j++) {
                    Tuple t = stf.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }

        public long timeAddAndSpillSbAndSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                SchemaDataBag bag = new SchemaDataBag(stf);
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }
*/
        public long timeAddAndSpillBagAndTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new DefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }

        public long timeAddAndSpillBagAndSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new DefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }

        public long timeAddAndSpillNewBagAndTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new NewDefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    Tuple t = mTupleFactory.newTuple(size);
                    for (int k = 0; k < size; k++) {
                        try {
                            t.set(k, Integer.valueOf(r.nextInt()));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }

        public long timeAddAndSpillNewBagAndSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                DataBag bag = new NewDefaultDataBag();
                for (int j = 0; j < elements; j++) {
                    SchemaTuple<?> t = stf.newTuple();
                    for (int k = 0; k < size; k++) {
                        try {
                            t.setInt(k, r.nextInt());
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    bag.add(t);
                }
                accum += bag.spill();
            }
            return accum;
        }

        // Iterate from Memory
        private int iterate(DataBag b) {
            int hashCode = 0;
            for (Tuple t : b) {
                hashCode += t == null ? 0 : t.hashCode();
            }
            return hashCode;
        }
/*
        public int timeIterateMemorySb(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(bag1);
            }
            return hashCode;
        }
*/
        public int timeIterateMemoryBagTuple(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(bag2);
            }
            return hashCode;
        }

        public int timeIterateMemoryBagSt(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(bag3);
            }
            return hashCode;
        }

        public int timeIterateMemoryNewBagTuple(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(bag4);
            }
            return hashCode;
        }

        public int timeIterateMemoryNewBagSt(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(bag5);
            }
            return hashCode;
        }

        // Iterate from disk
/*
        public int timeIterateSpilledSb(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(spilledBag1);
            }
            return hashCode;
        }
*/
        public int timeIterateSpilledBagTuple(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(spilledBag2);
            }
            return hashCode;
        }

        public int timeIterateSpilledBagSt(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(spilledBag3);
            }
            return hashCode;
        }

        public int timeIterateSpilledNewBagTuple(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(spilledBag4);
            }
            return hashCode;
        }

        public int timeIterateSpilledNewBagSt(int reps) {
            int hashCode = 0;
            for (int i = 0; i < reps; i++) {
                hashCode += iterate(spilledBag5);
            }
            return hashCode;
        }

        // Serialize
/*
        public long timeSerializeSb(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serialize(bag1);
            }
            return accum;
        }
*/
        public long timeSerializeBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serialize(bag2);
            }
            return accum;
        }

        public long timeSerializeBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serialize(bag3);
            }
            return accum;
        }

        public long timeSerializeNewBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serialize(bag4);
            }
            return accum;
        }

        public long timeSerializeNewBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serialize(bag5);
            }
            return accum;
        }

        // SerDe
/*
        public long timeSerDeSb(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serDe(bag1);
            }
            return accum;
        }
*/
        public long timeSerDeBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serDe(bag2, new DefaultDataBag());
            }
            return accum;
        }

        public long timeSerDeBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serDe(bag3, new DefaultDataBag());
            }
            return accum;
        }

        public long timeSerDeNewBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serDe(bag4, new NewDefaultDataBag());
            }
            return accum;
        }

        public long timeSerDeNewBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += serDe(bag5, new NewDefaultDataBag());
            }
            return accum;
        }

        // Deserialize
/*
        public long timeDeserializeSb(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += deserialize(f1);
            }
            return accum;
        }
*/
        public long timeDeserializeBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += deserialize(f2, new DefaultDataBag());
            }
            return accum;
        }

        public long timeDeserializeBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += deserialize(f3, new DefaultDataBag());
            }
            return accum;
        }
        public long timeDeserializeNewBagTuple(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += deserialize(f4, new NewDefaultDataBag());
            }
            return accum;
        }

        public long timeDeserializeNewBagSt(int reps) {
            long accum = 0;
            for (int i = 0; i < reps; i++) {
                accum += deserialize(f5, new NewDefaultDataBag());
            }
            return accum;
        }
    }

    public static void main(String[] args) throws Exception {
        String[] myArgs = new String[] {
            "-Dsize=7,4,1",
            "-Delements=90000,10000,70000,50000,30000"
        };
        Runner.main(SchemaBagBenchmarking.class, myArgs);
    }
}