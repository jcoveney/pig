/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestPigStreaming extends TestCase {
    PigStreaming ps = new PigStreaming();

    /**
     * We want to test 4 serializations (combination of delimiter/type information)
     * for each test case.
     *
     * @param t - tuple to serialize
     * @param expectedOutput - 4 values corresponding to calling serialize with:
     *            1. no delimiters and no type information
     *            2. delimiters and no type information
     *            3. no delimiters and type information
     *            4. delimiters and type information 
     */
    private void assertSerializationCorrect(Tuple t, byte[][] expectedOutput) 
            throws IOException{
        for (int i = 0; i < 4; i++) {
            boolean useDelimiters = (i % 2 == 1);
            boolean useTypeInformation = (i >= 2);

            //Test that default serialization is correct.
            if (i == 0) {
                byte[] output = ps.serialize(t);
                Assert.assertArrayEquals(expectedOutput[i], output);
            }

            byte[] output = ps.serialize(t, useDelimiters, useTypeInformation);
            Assert.assertArrayEquals(expectedOutput[i], output);
        }
    }

    @Test
    public void testSerialize__nullTuple() throws IOException {
        byte[][] expectedOutput = new byte[][] {
                "|&\n".getBytes(),
                "|&\n".getBytes(),
                "|&\n".getBytes(),
                "|&\n".getBytes()
        };
        assertSerializationCorrect(null, expectedOutput);
    }
    
    @Test
    public void testSerialize__emptyTuple() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(0);
        byte[][] expectedOutput = new byte[][] {
                "|&\n".getBytes(),
                "|&\n".getBytes(),
                "|&\n".getBytes(),
                "|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__record() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        t.set(0, "12\n34\n");
        byte[][] expectedOutput = new byte[][] {
                "12\n34\n|&\n".getBytes(),
                "12\n34\n|&\n".getBytes(),
                "C12\n34\n|&\n".getBytes(),
                "C12\n34\n|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }
 
    @Test
    public void testSerialize__string() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        t.set(0, "1234");
        byte[][] expectedOutput = new byte[][] {
                "1234|&\n".getBytes(),
                "1234|&\n".getBytes(),
                "C1234|&\n".getBytes(),
                "C1234|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__emptyString() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        t.set(0, "");
        byte[][] expectedOutput = new byte[][] {
                "|&\n".getBytes(),
                "|&\n".getBytes(),
                "C|&\n".getBytes(),
                "C|&\n".getBytes() 
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__nullString() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        t.set(0, null);
        byte[][] expectedOutput = new byte[][] {
                "|&\n".getBytes(),
                "|-_|&\n".getBytes(),
                "|&\n".getBytes(),
                "|-_|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__multifieldTuple() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(2);
        t.set(0, 1234);
        t.set(1, "sam");
        byte[][] expectedOutput = new byte[][] { 
                "1234\tsam|&\n".getBytes(),
                "1234|\t_sam|&\n".getBytes(),
                "I1234\tCsam|&\n".getBytes(),
                "I1234|\t_Csam|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__multifieldTupleWithNull() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(3);
        t.set(0, 1234);
        t.set(1, "sam");
        t.set(2, null);
        byte[][] expectedOutput = new byte[][] {
                "1234\tsam\t|&\n".getBytes(), 
                "1234|\t_sam|\t_|-_|&\n".getBytes(),
                "I1234\tCsam\t|&\n".getBytes(), 
                "I1234|\t_Csam|\t_|-_|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__nestedTuple() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(2);
        Tuple nestedTuple = DefaultTupleFactory.getInstance().newTuple(2);
        nestedTuple.set(0, "sam");
        nestedTuple.set(1, "oes");
        t.set(0, nestedTuple);
        t.set(1, "basil");
        byte[][] expectedOutput = new byte[][] { 
                "(sam,oes)\tbasil|&\n".getBytes(),
                "|(_sam|,_oes|)_|\t_basil|&\n".getBytes(),
                "(Csam,Coes)\tCbasil|&\n".getBytes(),
                "|(_Csam|,_Coes|)_|\t_Cbasil|&\n".getBytes()
        
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__bag() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        Tuple t1 = DefaultTupleFactory.getInstance().newTuple(2);
        Tuple t2 = DefaultTupleFactory.getInstance().newTuple(2);
        List<Tuple> bagTuples = new ArrayList<Tuple>();
        bagTuples.add(t1);
        bagTuples.add(t2);
        t1.set(0, "A");
        t1.set(1, "B");
        t2.set(0, 1);
        t2.set(1, 2);
        DataBag b = DefaultBagFactory.getInstance().newDefaultBag(bagTuples);
        t.set(0,b);
        byte[][] expectedOutput = new byte[][] {
                "{(A,B),(1,2)}|&\n".getBytes(),
                "|{_|(_A|,_B|)_|,_|(_1|,_2|)_|}_|&\n".getBytes(),
                "{(CA,CB),(I1,I2)}|&\n".getBytes(),
                "|{_|(_CA|,_CB|)_|,_|(_I1|,_I2|)_|}_|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__map() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        Map<String, String> m = new HashMap<String, String>();
        m.put("A", "B");
        m.put("C", "D");
        t.set(0,m);
        byte[][] expectedOutput = new byte[][] {
                "[A#B,C#D]|&\n".getBytes(),
                "|[_A#B|,_C#D|]_|&\n".getBytes(),
                "[CA#CB,CC#CD]|&\n".getBytes(),
                "|[_CA#CB|,_CC#CD|]_|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__complex_map() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        Map<String, Object> inner_map = new HashMap<String, Object>();
        inner_map.put("A", 1);
        inner_map.put("B", "E");
        
        Map<String, Object> outer_map = new HashMap<String, Object>();
        outer_map.put("C", "F");
        outer_map.put("D", inner_map);
        
        t.set(0,outer_map);
        byte[][] expectedOutput = new byte[][] {
                "[D#[A#1,B#E],C#F]|&\n".getBytes(),
                "|[_D#|[_A#1|,_B#E|]_|,_C#F|]_|&\n".getBytes(),
                "[CD#[CA#I1,CB#CE],CC#CF]|&\n".getBytes(),
                "|[_CD#|[_CA#I1|,_CB#CE|]_|,_CC#CF|]_|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testSerialize__allDataTypes() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(8);
        t.set(0, null);
        t.set(1, true);
        t.set(2, 2);
        t.set(3, 3L);
        t.set(4, 4.0f);
        t.set(5, 5.0d);
        t.set(6, new DataByteArray("six"));
        t.set(7, "seven");
        byte[][] expectedOutput = new byte[][] {
                "\ttrue\t2\t3\t4.0\t5.0\tsix\tseven|&\n".getBytes(),
                "|-_|\t_true|\t_2|\t_3|\t_4.0|\t_5.0|\t_six|\t_seven|&\n".getBytes(),
                "\tBtrue\tI2\tL3\tF4.0\tD5.0\tAsix\tCseven|&\n".getBytes(),
                "|-_|\t_Btrue|\t_I2|\t_L3|\t_F4.0|\t_D5.0|\t_Asix|\t_Cseven|&\n".getBytes()
        };
        assertSerializationCorrect(t, expectedOutput);
    }

    @Test
    public void testDeserialize__newline() throws IOException {
    	byte[] input = "12\n34\n|&".getBytes();
    	FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
    	Object out = ps.deserializeStreamOutput(schema, input);
    	Assert.assertEquals("12\n34\n", out);
    }
    
    @Test
    public void testDeserialize__string() throws IOException {
        byte[] input = "1234|&".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        Object out = ps.deserializeStreamOutput(schema, input);
        Assert.assertEquals("1234", out);
    }
    
    @Test
    public void testDeserialize__emptyString() throws IOException {
        byte[] input = "|&".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        Object out = ps.deserializeStreamOutput(schema, input);
        Assert.assertEquals("", out);
    }

    @Test
    public void testDeserialize__nullString() throws IOException {
        byte[] input = "|-_|&".getBytes();
        FieldSchema schema = new FieldSchema("", DataType.CHARARRAY);
        Object out = ps.deserializeStreamOutput(schema, input);
        Assert.assertEquals(null, out);
    }

    @Test
    public void testDeserialize__multifieldTuple() throws IOException {
        byte[] input = "|(_1234|,_sam|)_|&".getBytes();
        FieldSchema f1 = new FieldSchema("", DataType.INTEGER);
        FieldSchema f2 = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(f1);
        fsl.add(f2);
        Schema schema = new Schema(fsl);

        FieldSchema fs = new FieldSchema("", schema, DataType.TUPLE);
        Tuple expectedOutput = DefaultTupleFactory.getInstance().newTuple(2);
        expectedOutput.set(0, 1234);
        expectedOutput.set(1, "sam");
        
        Object out = ps.deserializeStreamOutput(fs, input);
        Assert.assertEquals(expectedOutput, out);
    }
    
    @Test
    public void testDeserialize__nestedTuple() throws IOException {
        byte[] input = "|(_|(_sammy|,_oes|)_|,_1234|,_sam|)_|&".getBytes();
        FieldSchema f1Inner = new FieldSchema("", DataType.CHARARRAY);
        FieldSchema f2Inner = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fslInner = new ArrayList<FieldSchema>();
        fslInner.add(f1Inner);
        fslInner.add(f2Inner);
        Schema schemaInner = new Schema(fslInner);
        
        FieldSchema f1 = new FieldSchema("", schemaInner, DataType.TUPLE);
        FieldSchema f2 = new FieldSchema("", DataType.INTEGER);
        FieldSchema f3 = new FieldSchema("", DataType.CHARARRAY);
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(f1);
        fsl.add(f2);
        fsl.add(f3);
        Schema schema = new Schema(fsl);
        FieldSchema fs = new FieldSchema("", schema, DataType.TUPLE);
        
        Tuple expectedOutputInner = DefaultTupleFactory.getInstance().newTuple(2);
        expectedOutputInner.set(0, "sammy");
        expectedOutputInner.set(1, "oes");
        
        Tuple expectedOutput = DefaultTupleFactory.getInstance().newTuple(3);
        expectedOutput.set(0, expectedOutputInner);
        expectedOutput.set(1, 1234);
        expectedOutput.set(2, "sam");
        
        Object out = ps.deserializeStreamOutput(fs, input);
        Assert.assertEquals(expectedOutput, out);
    }
    
    @Test
    public void testDeserialize__bag() throws IOException {
        byte[] input = "|{_|(_A|,_1|)_|,_|(_B|,_2|)_|}_|&".getBytes();
        FieldSchema f1Inner = new FieldSchema("", DataType.CHARARRAY);
        FieldSchema f2Inner = new FieldSchema("", DataType.INTEGER);
        List<FieldSchema> fslInner = new ArrayList<FieldSchema>();
        fslInner.add(f1Inner);
        fslInner.add(f2Inner);
        Schema schemaInner = new Schema(fslInner);
        FieldSchema fsInner = new FieldSchema("", schemaInner, DataType.TUPLE);
        
        List<FieldSchema> fsl = new ArrayList<FieldSchema>();
        fsl.add(fsInner);
        Schema schema = new Schema(fsl);
        
        FieldSchema fs = new FieldSchema("", schema, DataType.BAG);
        
        Tuple expectedOutputInner1 = DefaultTupleFactory.getInstance().newTuple(2);
        expectedOutputInner1.set(0, "A");
        expectedOutputInner1.set(1, 1);
        
        Tuple expectedOutputInner2 = DefaultTupleFactory.getInstance().newTuple(2);
        expectedOutputInner2.set(0, "B");
        expectedOutputInner2.set(1, 2);
        
        List<Tuple> tuples = new ArrayList<Tuple>();
        tuples.add(expectedOutputInner1);
        tuples.add(expectedOutputInner2);
        DataBag expectedOutput = DefaultBagFactory.getInstance().newDefaultBag(tuples);

        Object out = ps.deserializeStreamOutput(fs, input);
        Assert.assertEquals(expectedOutput, out);
    }
    
    @Test
    public void testDeserialize__map() throws IOException {
        byte[] input = "|[_A#B|,_C#D|]_|&".getBytes();
        FieldSchema fs = new FieldSchema("", DataType.MAP);
        
        Map<String, String> expectedOutput = new HashMap<String, String>();
        expectedOutput.put("A", "B");
        expectedOutput.put("C", "D");        
        
        Object out = ps.deserializeStreamOutput(fs, input);
        Assert.assertEquals(expectedOutput, out);
    }
}
