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
package org.apache.pig.impl.util;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Charsets;

/**
 * This util class provides methods that are shared by storage class
 * {@link PigStorage} and streaming class {@link PigStreaming}
 *
 */
public final class StorageUtil {
    private static final Log log = LogFactory.getLog(StorageUtil.class);
    
    private static Map<Byte, byte[]> TYPE_INDICATOR;
    static {
        TYPE_INDICATOR = new HashMap<Byte, byte[]>();
        TYPE_INDICATOR.put(DataType.BOOLEAN, "B".getBytes());
        TYPE_INDICATOR.put(DataType.INTEGER, "I".getBytes());
        TYPE_INDICATOR.put(DataType.LONG, "L".getBytes());
        TYPE_INDICATOR.put(DataType.FLOAT, "F".getBytes());
        TYPE_INDICATOR.put(DataType.DOUBLE, "D".getBytes());
        TYPE_INDICATOR.put(DataType.BYTEARRAY, "A".getBytes());
        TYPE_INDICATOR.put(DataType.CHARARRAY, "C".getBytes());
    }
    /**
     * Transform a <code>String</code> into a byte representing the
     * field delimiter.
     *
     * @param delimiter a string that may be in single-quoted form
     * @return the field delimiter in byte form
     */
    public static byte parseFieldDel(String delimiter) {
        if (delimiter == null) {
            throw new IllegalArgumentException("Null delimiter");
        }

        delimiter = parseSingleQuotedString(delimiter);

        if (delimiter.length() > 1 && delimiter.charAt(0) != '\\') {
            throw new IllegalArgumentException("Delimeter must be a " +
            		"single character " + delimiter);
        }

        byte fieldDel = '\t';

        if (delimiter.length() == 1) {
            fieldDel = (byte)delimiter.charAt(0);
        } else if (delimiter.charAt(0) == '\\') {
            switch (delimiter.charAt(1)) {
            case 't':
                fieldDel = (byte)'\t';
                break;

            case 'x':
                fieldDel =
                    Integer.valueOf(delimiter.substring(2), 16).byteValue();
                break;
            case 'u':
                fieldDel =
                    Integer.valueOf(delimiter.substring(2)).byteValue();
                break;

            default:
                throw new IllegalArgumentException("Unknown delimiter " +
                        delimiter);
            }
        }

        return fieldDel;
    }

    public static final String TUPLE_BEGIN = "TB";
    public static final String TUPLE_END = "TE";
    public static final String BAG_BEGIN = "BB";
    public static final String BAG_END = "BE";
    public static final String MAP_BEGIN = "MB";
    public static final String MAP_END = "ME";
    public static final String FIELD = "F";
    public static final String MAP_KEY = "MK";
    public static final String NULL = "N";
    private static Map<String, byte[]> DEFAULT_DELIMITERS;
    static {
        DEFAULT_DELIMITERS = new HashMap<String, byte[]>();
        DEFAULT_DELIMITERS.put(TUPLE_BEGIN, "(".getBytes());
        DEFAULT_DELIMITERS.put(TUPLE_END, ")".getBytes());
        DEFAULT_DELIMITERS.put(BAG_BEGIN, "{".getBytes());
        DEFAULT_DELIMITERS.put(BAG_END, "}".getBytes());
        DEFAULT_DELIMITERS.put(MAP_BEGIN, "[".getBytes());
        DEFAULT_DELIMITERS.put(MAP_END, "]".getBytes());
        DEFAULT_DELIMITERS.put(FIELD, ",".getBytes());
        DEFAULT_DELIMITERS.put(MAP_KEY, "#".getBytes());
        DEFAULT_DELIMITERS.put(NULL, new byte[] {});
    }

    public static void putField(OutputStream out, Object field) throws IOException {
        putField(out, field, DEFAULT_DELIMITERS, false);
    }
    
    public static void putField(OutputStream out, Object field, boolean includeTypeInformation) throws IOException {
        putField(out, field, DEFAULT_DELIMITERS, includeTypeInformation);
    }
    
    /**
     * Serialize an object to an {@link OutputStream} in the
     * field-delimited form.
     *
     * @param out an OutputStream object
     * @param field an object to be serialized
     * @throws IOException if serialization fails.
     */
    @SuppressWarnings("unchecked")
    public static void putField(OutputStream out, Object field, Map<String, byte[]> delimiters, boolean includeTypeInformation)
    throws IOException {
        switch (DataType.findType(field)) {
        case DataType.NULL:
            out.write(delimiters.get(NULL));
            break;

        case DataType.BOOLEAN:
            writeField(out, ((Boolean)field).toString().getBytes(), 
                    DataType.BOOLEAN, includeTypeInformation);
            break;

        case DataType.INTEGER:
            writeField(out, ((Integer)field).toString().getBytes(), 
                    DataType.INTEGER, includeTypeInformation);
            break;

        case DataType.LONG:
            writeField(out, ((Long)field).toString().getBytes(), 
                    DataType.LONG, includeTypeInformation);
            break;

        case DataType.FLOAT:
            writeField(out, ((Float)field).toString().getBytes(), 
                    DataType.FLOAT, includeTypeInformation);
            break;

        case DataType.DOUBLE:
            writeField(out, ((Double)field).toString().getBytes(), 
                    DataType.DOUBLE, includeTypeInformation);
            break;

        case DataType.BIGINTEGER:
            if (includeTypeInformation) {
                throw new UnsupportedOperationException("BigInteger is not supported when writing out with type information.");
            }
            writeField(out, ((BigInteger)field).toString().getBytes(),
                    DataType.BIGINTEGER, includeTypeInformation);
            break;

        case DataType.BIGDECIMAL:
            if (includeTypeInformation) {
                throw new UnsupportedOperationException("BigDecimal is not supported when writing out with type information.");
            }
            writeField(out, ((BigDecimal)field).toString().getBytes(),
                    DataType.BIGDECIMAL, includeTypeInformation);
            break;

        case DataType.DATETIME:
            if (includeTypeInformation) {
                throw new UnsupportedOperationException("DateTime is not supported when writing out with type information.");
            }
            writeField(out, ((DateTime)field).toString().getBytes(),
                    DataType.DATETIME, includeTypeInformation);
            break;

        case DataType.BYTEARRAY:
            writeField(out, ((DataByteArray)field).get(), 
                    DataType.BYTEARRAY, includeTypeInformation);
            break;

        case DataType.CHARARRAY:
            writeField(out, ((String)field).getBytes(Charsets.UTF_8), 
                    DataType.CHARARRAY, includeTypeInformation);
            break;

        case DataType.MAP:
            boolean mapHasNext = false;
            Map<String, Object> m = (Map<String, Object>)field;
            out.write(delimiters.get(MAP_BEGIN));
            for(Map.Entry<String, Object> e: m.entrySet()) {
                if(mapHasNext) {
                    out.write(delimiters.get(FIELD));
                } else {
                    mapHasNext = true;
                }
                putField(out, e.getKey(), delimiters, includeTypeInformation);
                out.write(delimiters.get(MAP_KEY));
                putField(out, e.getValue(), delimiters, includeTypeInformation);
            }
            out.write(delimiters.get(MAP_END));
            break;

        case DataType.TUPLE:
            boolean tupleHasNext = false;
            Tuple t = (Tuple)field;
            out.write(delimiters.get(TUPLE_BEGIN));
            for(int i = 0; i < t.size(); ++i) {
                if(tupleHasNext) {
                    out.write(delimiters.get(FIELD));
                } else {
                    tupleHasNext = true;
                }
                try {
                    putField(out, t.get(i), delimiters, includeTypeInformation);
                } catch (ExecException ee) {
                    throw ee;
                }
            }
            out.write(delimiters.get(TUPLE_END));
            break;

        case DataType.BAG:
            boolean bagHasNext = false;
            out.write(delimiters.get(BAG_BEGIN));
            Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
            while(tupleIter.hasNext()) {
                if(bagHasNext) {
                    out.write(delimiters.get(FIELD));
                } else {
                    bagHasNext = true;
                }
                putField(out, (Object)tupleIter.next(), delimiters, includeTypeInformation);
            }
            out.write(delimiters.get(BAG_END));
            break;

        default: {
            int errCode = 2108;
            String msg = "Could not determine data type of field: " + field;
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        }
    }
    
    private static void writeField(OutputStream out, byte[] bytes, byte dataType, 
            boolean includeTypeInformation) throws IOException {
        if (includeTypeInformation) {
            out.write(TYPE_INDICATOR.get(dataType));
        }
        out.write(bytes);
    }

    /**
     * Transform a line of <code>Text</code> to a <code>Tuple</code>
     *
     * @param val a line of text
     * @param fieldDel the field delimiter
     * @return tuple constructed from the text
     */
    public static Tuple textToTuple(Text val, byte fieldDel) {

        byte[] buf = val.getBytes();
        int len = val.getLength();
        int start = 0;

        ArrayList<Object> protoTuple = new ArrayList<Object>();

        for (int i = 0; i < len; i++) {
            if (buf[i] == fieldDel) {
                readField(protoTuple, buf, start, i);
                start = i + 1;
            }
        }

        // pick up the last field
        if (start <= len) {
            readField(protoTuple, buf, start, len);
        }

        return TupleFactory.getInstance().newTupleNoCopy(protoTuple);
    }

    public static boolean isDelimiter(byte delim, byte preWrapDelim, byte postWrapDelim, byte[] buf, int index, int depth, int endIndex) {
        return (depth == 0 && ( index == endIndex ||
                                ( index <= endIndex - 2 &&
                                  buf[index] == preWrapDelim &&
                                  buf[index+1] == delim &&
                                  buf[index+2] == postWrapDelim)));
    }
    
    //---------------------------------------------------------------
    // private methods

    private static void readField(ArrayList<Object> protoTuple,
                          byte[] buf, int start, int end) {
        if (start == end) {
            // NULL value
            protoTuple.add(null);
        } else {
            protoTuple.add(new DataByteArray(buf, start, end));
        }
    }

    private static String parseSingleQuotedString(String delimiter) {
        int startIndex = 0;
        int endIndex;
        while (startIndex < delimiter.length()
                && delimiter.charAt(startIndex++) != '\'')
            ;
        endIndex = startIndex;
        while (endIndex < delimiter.length()
                && delimiter.charAt(endIndex) != '\'') {
            if (delimiter.charAt(endIndex) == '\\') {
                endIndex++;
            }
            endIndex++;
        }

        return (endIndex < delimiter.length()) ?
                delimiter.substring(startIndex, endIndex) : delimiter;
    }
}
