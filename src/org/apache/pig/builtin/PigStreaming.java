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
package org.apache.pig.builtin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigToStream;
import org.apache.pig.StreamToPig;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.streaming.OutputHandler;
import org.apache.pig.impl.util.StorageUtil;

import com.google.common.base.Charsets;

/**
 * The default implementation of {@link PigToStream} and {@link StreamToPig}
 * interfaces. It converts tuples into <code>fieldDel</code> separated lines 
 * and <code>fieldDel</code> separated lines into tuples.
 * 
 */
public class PigStreaming implements PigToStream, StreamToPig {
    private static final Log log = LogFactory.getLog(PigStreaming.class);
    
    //Delimiter: Every delimiter needs to have preWrapDelimField as the character before and 
    //postWrapDelimField as the character after it.  Ex: A tuple starts with "|(_ and ends with "|)_"
    private static byte[] recordDel = (OutputHandler.END_OF_RECORD + "\n").getBytes();
    private static byte paramDel = '\t';
    private static byte nullByte = '-';
    private static byte tupleBeginDelim = '(';
    private static byte tupleEndDelim = ')';
    private static byte bagBeginDelim = '{';
    private static byte bagEndDelim = '}';
    private static byte mapBeginDelim = '[';
    private static byte mapEndDelim = ']';
    private static byte fieldDelim = ',';
    private static byte mapKeyValueDelim = '#'; //Not wrapped by wrapDelimField
    private static byte preWrapDelimField = '|';
    private static byte postWrapDelimField = '_';

    private static Map<String, byte[]> DELIMITERS;
    static {
        DELIMITERS = new HashMap<String, byte[]>();
        DELIMITERS.put(StorageUtil.TUPLE_BEGIN, new byte[]{preWrapDelimField, tupleBeginDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.TUPLE_END, new byte[]{preWrapDelimField, tupleEndDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.BAG_BEGIN, new byte[]{preWrapDelimField, bagBeginDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.BAG_END, new byte[]{preWrapDelimField, bagEndDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.MAP_BEGIN, new byte[]{preWrapDelimField, mapBeginDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.MAP_END, new byte[]{preWrapDelimField, mapEndDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.FIELD, new byte[]{preWrapDelimField, fieldDelim, postWrapDelimField});
        DELIMITERS.put(StorageUtil.MAP_KEY, new byte[]{mapKeyValueDelim});
        DELIMITERS.put(StorageUtil.NULL, new byte[]{preWrapDelimField, nullByte, postWrapDelimField});
    }

    private ByteArrayOutputStream out;
    
    /**
     * The constructor that uses the default field delimiter.
     */
    public PigStreaming() {
        out = new ByteArrayOutputStream();
    }
    
    /**
     * The constructor that accepts a user-specified field
     * delimiter.
     * 
     * @param delimiter a <code>String</code> specifying the field
     * delimiter.
     */
    public PigStreaming(String delimiter) {
        this();
        paramDel = StorageUtil.parseFieldDel(delimiter);
    }
    
    @Override
    public byte[] serialize(Tuple t, boolean wrapFieldDelimiter, boolean includeTypeInformation)
            throws IOException {
        out.reset();
        int sz;
        Object field;
        if (t == null) {
            sz = 0;
        } else {
            sz = t.size();
        }
        for (int i=0; i < sz-1; i++) {
            field = t.get(i);
            if (wrapFieldDelimiter) {
                StorageUtil.putField(out, field, DELIMITERS, includeTypeInformation);
                out.write(preWrapDelimField);
            } else {
                StorageUtil.putField(out, field, includeTypeInformation);
            }
            out.write(paramDel);
            if (wrapFieldDelimiter)
                out.write(postWrapDelimField);
        }
        if (t != null && t.size() > 0) {
            field = t.get(t.size() - 1);
            if (wrapFieldDelimiter)
                StorageUtil.putField(out, field, DELIMITERS, includeTypeInformation);
            else
                StorageUtil.putField(out, field, includeTypeInformation);
        }
        out.write(recordDel, 0, recordDel.length);
        return out.toByteArray();
    }

    @Override
    public byte[] serialize(Tuple t) throws IOException {
        return serialize(t, false, false);
    }

    @Override
    public Tuple deserializeTuple(byte[] bytes) throws IOException {
        Text val = new Text(bytes);
        return StorageUtil.textToTuple(val, paramDel);
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }

    @Override
    public Object deserializeStreamOutput(FieldSchema fs, byte[] bytes) throws IOException {
        return deserializeStream(fs, bytes, 0, bytes.length - recordDel.length); //Drop newline
    }

    private Object deserializeStream(FieldSchema fs, byte[] bytes, int startIndex, int endIndex) throws IOException {
        //Check for null
        if (endIndex >= startIndex + 2 && bytes[startIndex+1] == nullByte && bytes[startIndex] == preWrapDelimField && bytes[startIndex+2] == postWrapDelimField) {
            return null;
        }

        if (fs.type == DataType.BAG) {
            return deserializeBag(fs, bytes, startIndex + 3, endIndex - 2);
        } else if (fs.type == DataType.TUPLE) {
            return deserializeTuple(fs, bytes, startIndex + 3, endIndex - 2);
        } else if (fs.type == DataType.MAP) {
            return deserializeMap(bytes, startIndex + 3, endIndex - 2);
        }

        if (fs.type == DataType.CHARARRAY) {
            return extractString(bytes, startIndex, endIndex, true);
        }

        //Can we do this faster?
        String val = extractString(bytes, startIndex, endIndex, false);

        if (fs.type == DataType.LONG) {
            return Long.valueOf(val);
        } else if (fs.type == DataType.INTEGER) {
            return Integer.valueOf(val);
        } else if (fs.type == DataType.FLOAT) {
            return Float.valueOf(val);
        } else if (fs.type == DataType.DOUBLE) {
            return Double.valueOf(val);
        } else if (fs.type == DataType.BYTEARRAY) {
            return new DataByteArray(val.getBytes());
        } else {
            throw new ExecException("Can't deserialize type: " + DataType.findTypeName(fs.type));
        }
    }

    private Tuple deserializeTuple(FieldSchema fs, byte[] buf, int startIndex, int endIndex) throws IOException {
        ArrayList<Object> protoTuple = new ArrayList<Object>();
        int depth = 0;
        int fieldNum = 0;
        int fieldStart = startIndex;
        Schema tupleSchema = fs.schema;

        for (int index = startIndex; index <= endIndex; index++) {
            depth = updateDepth(buf, depth, index);
            if ( StorageUtil.isDelimiter(fieldDelim, preWrapDelimField, postWrapDelimField, 
                                            buf, index, depth, endIndex) ) {
                protoTuple.add(deserializeStream(tupleSchema.getField(fieldNum), buf, fieldStart, index - 1));
                fieldStart = index + 3;
                fieldNum++;
            }
        }
        return TupleFactory.getInstance().newTupleNoCopy(protoTuple);
    }

    private DataBag deserializeBag(FieldSchema fs, byte[] buf, int startIndex, int endIndex) throws IOException {
        ArrayList<Tuple> protoBag = new ArrayList<Tuple>();
        int depth = 0;
        int fieldStart = startIndex;

        for (int index = startIndex; index <= endIndex; index++) {
            depth = updateDepth(buf, depth, index);
            if ( StorageUtil.isDelimiter(fieldDelim, preWrapDelimField, postWrapDelimField, 
                                            buf, index, depth, endIndex) ) {
                protoBag.add((Tuple)deserializeStream(fs.schema.getField(0), buf, fieldStart, index - 1));
                fieldStart = index + 3;
            }
        }
        return BagFactory.getInstance().newDefaultBag(protoBag);
    }

    private Map<String, Object> deserializeMap(byte[] buf, int startIndex, int endIndex) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        int depth = 0;
        int fieldStart = startIndex;

        String key = null;
        Object val = null;

        for (int index = startIndex; index <= endIndex; index++) {
            byte currChar = buf[index];
            depth = updateDepth(buf, depth, index);

            if (currChar == mapKeyValueDelim && depth == 0) {
                key = extractString(buf, fieldStart, index - 1, true);
                fieldStart = index + 1;
            }

            if ( StorageUtil.isDelimiter(fieldDelim, preWrapDelimField, postWrapDelimField,
                                            buf, index, depth, endIndex) ) {
                val = extractString(buf, fieldStart, index - 1, true);
                map.put(key, val);
                fieldStart = index + 3;
            }
        }

        return map;
    }

    private String extractString(byte[] bytes, int startIndex, int endIndex, boolean useUtf8) {
        byte[] sub = new byte[endIndex - startIndex + 1];
        System.arraycopy(bytes, startIndex, sub, 0, sub.length);
        if (useUtf8) {
            return new String(sub, Charsets.UTF_8);
        } else {
            return new String(sub);
        }
    }

    /**
     * Update depth on seeing the third character in a 3-character delimiter.
     */
    private int updateDepth(byte[] buf, int currDepth, int index) {
        if (index < 2 || buf[index-2] != preWrapDelimField || buf[index] != postWrapDelimField) {
            return currDepth;
        }

        byte midChar = buf[index-1];
        if (midChar == bagBeginDelim || midChar == tupleBeginDelim || midChar == mapBeginDelim) {
            return currDepth + 1;
        } else if (midChar == bagEndDelim || midChar == tupleEndDelim || midChar == mapEndDelim) {
            return currDepth - 1;
        } else {
            return currDepth;
        }
    }
}
