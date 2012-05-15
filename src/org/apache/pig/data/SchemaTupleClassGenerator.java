package org.apache.pig.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.common.base.Joiner;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.scripting.ScriptEngine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;

//TODO: implement a raw comparator for it?
//TODO: massLoad() should be based on a properties file in the jar that has all of the values I wrote to it
//TODO: generate code for each unique tuple we get (don't strip on generation)

//the benefit of having the generic here is that in the case that we do ".set(t)" and t is the right type, it will be very fast
public class SchemaTupleClassGenerator {
    private static final Log LOG = LogFactory.getLog(SchemaTupleClassGenerator.class);

    private static int globalClassIdentifier = 0;
    private static boolean filesAdded = false; //a marker for whether necessary files/dependencies have been added to the job jar

    public static List<String> readClasslistFromJar() {
        return null; //TODO whatever method we end up putting in addClassFilesToJar, this should read that
    }

    public static void addClassFilesToJar(File classFile) {
        //TODO detect if we're in local mode and not do anything if that is the case
        PigContext pc = ScriptState.get().getPigContext();

        if (pc == null) {
            LOG.warn("PigContext not available! Unable to add file " + classFile + " to job jar");
            return;
         }

        pc.addScriptFile(classFile.getName(), classFile.getAbsolutePath());

        //TODO need to add this information to the jar manifest
    }

    //this is called on the front end
    public static int generateAndAddToJar(Schema s) {
        SchemaTupleFactory.GeneratedSchemaTupleInfoRepository genned = SchemaTupleFactory.getGeneratedInfo();

        Class<SchemaTuple> clazz = genned.getTupleClass(s);

        if (clazz != null) {
            try {
                SchemaTuple st = SchemaTupleFactory.instantiateClass(clazz);
                return st.getSchemaTupleIdentifier();
            } catch (ExecException e) {
                throw new RuntimeException("Unable to instantiate found class object for Schema " + s, e);
            }
        }

        int id = getGlobalClassIdentifier();
        String codeString = generateCodeString(s, id);

        File current;
        try {
            current = compileCodeString(codeString, "SchemaTuple_" + id);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to compile codeString:\n" + codeString, e);
        }
        current.deleteOnExit();
        addClassFilesToJar(current);

        try {
            genned.registerClass("SchemaTuple_" + id);
        } catch (ExecException e) {
            throw new RuntimeException("Generated class SchemaTuple_"+id+" not found in classpath", e);
        }

        return id;
    }

    public static String generateCodeString(Schema s, int id) {
        TypeInFunctionStringOutFactory f = new TypeInFunctionStringOutFactory(s, id);

        for (Schema.FieldSchema fs : s.getFields())
            f.process(fs);

        //return f.end();
        String tmp = f.end(); //remove
        System.out.println(tmp); //remove
        return tmp; //remove
    }


    public static int getGlobalClassIdentifier() {
        return globalClassIdentifier++;
    }

    //TODO should generate directly to a temp directory
    public static File compileCodeString(String generatedCodeString, String className) throws ExecException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = Lists.newArrayList(new JavaSourceFromString(className, generatedCodeString));

        if (!compiler.getTask(null, fileManager, null, null, null, compilationUnits).call())
            throw new ExecException(className + " failed to compile properly");

        try {
            fileManager.close();
        } catch (IOException e) {
            throw new ExecException("Unable to close file manager", e);
        }

        File current = new File(className + ".class");

        if (!current.exists())
            throw new ExecException("Generated class file " + className + ".class not found");

        return current;
    }

    //taken from http://docs.oracle.com/javase/6/docs/api/javax/tools/JavaCompiler.html
    static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.','/') + JavaFileObject.Kind.SOURCE.extension), JavaFileObject.Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }


    static class CompareToSpecificString extends TypeInFunctionStringOut {
        private int id;

        public CompareToSpecificString(int id) {
            this.id = id;
        }

        public void prepare() {
            add("public int compareToSpecific(SchemaTuple_"+id+" t) {");
            add("    int i = appendSize() - t.appendSize();");
            add("    if (i != 0)");
            add("        return Math.abs(i);");
            add("    boolean themNull;");
            add("    boolean usNull;");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (isPrimitive()) {
                add("    themNull = checkIfNull_"+fieldNum+"();");
                add("    usNull = t.checkIfNull_"+fieldNum+"();");
            } else {
                add("    themNull = pos_"+fieldNum+" == null;");
                add("    usNull = pos_"+fieldNum+" == null;");
            }
            add("    if (usNull && !themNull) {");
            add("        return 1;");
            add("    } else if (!themNull) {");
            if (isBoolean()) {
                add("        i = ( getPos_"+fieldNum+"() ? 1 : 0 ) - ( t.getPos_"+fieldNum+"() ? 1 : 0 );");
                add("        if (i != 0)");
                add("            return i;");
            } else if (isPrimitive()) {
                add("        if (getPos_"+fieldNum+"() != t.getPos_"+fieldNum+"())");
                add("            return getPos_"+fieldNum+"() > t.getPos_"+fieldNum+"() ? 1 : -1;");
            } else if (isString()) {
                add("        i = getPos_"+fieldNum+"().compareTo(t.getPos_"+fieldNum+"());");
                add("        if (i != 0)");
                add("            return i;");
            } else if (isBytearray()) {
                add("        i = DataByteArray.compare(getPos_"+fieldNum+"(), t.getPos_"+fieldNum+"());");
                add("        if (i != 0)");
                add("            return i;");
            } else {
                add("        i = pos_"+fieldNum+".compareTo(t.getPos_"+fieldNum+"());");
                add("        if (i != 0)");
                add("            return i;");
            }
            add("    } else {");
            add("        return -1;");
            add("    }");
        }

        public void end() {
            add("    for (int z = 0; z < appendSize(); z++) {");
            add("        try {");
            add("            i = DataType.compare(getAppend(z), t.getAppend(z));");
            add("        } catch (ExecException e) {");
            add("            throw new RuntimeException(\"Unable to get append\", e);");
            add("        }");
            add("        if (i != 0)");
            add("            return i;");
            add("    }");
            add("    return 0;");
            add("}");
        }
    }

    //TODO clear up how it deals with nulls etc. IE is the logic correct
    static class CompareToString extends TypeInFunctionStringOut {
        private Queue<Integer> nextNestedSchemaIdForCompareTo;
        private int id;

        public CompareToString(Queue<Integer> nextNestedSchemaIdForCompareTo, int id) {
            this.nextNestedSchemaIdForCompareTo = nextNestedSchemaIdForCompareTo;
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("public int compareTo(SchemaTuple t, boolean checkType) {");
            add("    if (checkType && t instanceof SchemaTuple_"+id+")");
            add("        return compareToSpecific((SchemaTuple_"+id+")t);");
            add("    int mySz = size();");
            add("    int tSz = t.size();");
            add("    if (mySz > tSz) {");
            add("        return 1;");
            add("    } else if (mySz < tSz) {");
            add("        return -1;");
            add("    } else {");
            add("        int i = 0;");
            add("        boolean themNull;");
        }

        boolean compTup = false;
        boolean compStr = false;
        boolean compIsNull = false;
        boolean compByte = false;

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (!compIsNull) {
                add("        boolean compIsNull = false;");
                compIsNull = true;
            }
            add("        try {");
            add("            themNull = t.isNull("+fieldNum+");");
            add("        } catch (ExecException e) {");
            add("            throw new RuntimeException(\"Unable to see if field is null for field: "+fieldNum+"\", e);");
            add("        }");
            if (isPrimitive()) {
                add("        if (checkIfNull_"+fieldNum+"() && !themNull) {");
                add("            return -1;");
                add("        } else if (!checkIfNull_"+fieldNum+"() && themNull) {");
                add("            return 1;");
                add("        } else if (!checkIfNull_"+fieldNum+"() && !themNull) {");
                String s = typeName();
                s = s.substring(0,1).toUpperCase() + s.substring(1);
                add("            try {");
                add("                if (getPos_"+fieldNum+"() != t.get"+s+"("+fieldNum+"))");
                if (isBoolean())
                    add("                    return getPos_"+fieldNum+"() ? 1 : -1 ;");
                else
                    add("                    return getPos_"+fieldNum+"() > t.get"+s+"("+fieldNum+") ? 1 : -1 ;");
                add("            } catch (ExecException e) {");
                add("                throw new RuntimeException(\"Unable to retrieve field "+fieldNum+" in given Tuple: \" + t, e);");
                add("            }");
                add("        }");
            } else if (isString()) {
                if (!compStr) {
                   add("            String str;");
                   compStr = true;
                }
                add("        try {");
                add("            str = t.getString("+fieldNum+");");
                add("        } catch (ExecException e) {");
                add("            throw new RuntimeException(\"Failed to retrieve String field "+fieldNum+" in tuple: \" + t, e);");
                add("        }");
                add("        compIsNull = str == null || themNull;");
                add("        if (pos_"+fieldNum+" == null && !compIsNull) {");
                add("            return -1;");
                add("        } else if (!compIsNull) {");
                add("            i = pos_"+fieldNum+".compareTo(str);");
                add("            if (i != 0)");
                add("                return i;");
                add("        } else if (pos_"+fieldNum+" != null) {");
                add("            return 1;");
                add("        }");
            } else if (isBytearray()) {
                if (!compByte) {
                    add("        byte[] compBuf;");
                    compByte = true;
                }
                add("        try {");
                add("            compBuf = t.getBytes("+fieldNum+");");
                add("        } catch (ExecException e) {");
                add("            throw new RuntimeException(\"Failed to retrieve byte[] field "+fieldNum+" in tuple: \" + t, e);");
                add("        }");
                add("        compIsNull = compBuf == null || themNull;");
                add("        if (pos_"+fieldNum+" == null && !compIsNull) {");
                add("            return -1;");
                add("        } else if (!compIsNull) {");
                add("            i = new DataByteArray(pos_"+fieldNum+").compareTo(new DataByteArray(compBuf));");
                add("            if (i != 0)");
                add("                return i;");
                add("        } else if (pos_"+fieldNum+" != null) {");
                add("            return 1;");
                add("        }");
            } else if (isTuple()) {
                int nestedSchemaTupleId = nextNestedSchemaIdForCompareTo.remove();
                if (!compTup) {
                    add("        Tuple tup;");
                    compTup = true;
                }
                add("        try {");
                add("            tup = (Tuple)t.get("+fieldNum+");");
                add("        } catch (ExecException e) {");
                add("            throw new RuntimeException(\"Unable to retrieve expected field "+fieldNum+" from Tuple: \" + t, e);");
                add("        }");
                add("        compIsNull = tup == null || themNull;");
                add("        if (pos_"+fieldNum+" == null && !compIsNull) {");
                add("            return -1;");
                add("        } else if (!compIsNull) {");
                add("            if (tup instanceof SchemaTuple_"+nestedSchemaTupleId+") {");
                add("                i = pos_"+fieldNum+".compareToSpecific((SchemaTuple_"+nestedSchemaTupleId+")tup);");
                add("            } else if (tup instanceof SchemaTuple) {");
                add("                i = pos_"+fieldNum+".compareTo((SchemaTuple)tup, false);");
                add("            } else {");
                add("                i = pos_"+fieldNum+".compareTo(tup);");
                add("            }");
                add("            if (i != 0)");
                add("                return i;");
                add("        } else {");
                add("            return 1;");
                add("        }");
            }
        }

        public void end() {
            add("        if (sizeNoAppend() < size()) {");
            add("            int m = sizeNoAppend();");
            add("            for (int k = 0; k < size() - sizeNoAppend(); k++) {");
            add("                try {");
            add("                    i = DataType.compare(getAppend(k), t.get(m++));");
            add("                } catch (ExecException e) {");
            add("                    throw new RuntimeException(\"Unable to get append value\", e);");
            add("                }");
            add("                if (i != 0)");
            add("                    return i;");
            add("            }");
            add("        }");
            add("    }");
            add("    return 0;");
            add("}");
        }
    }

    static class HashCode extends TypeInFunctionStringOut {
        private int nulls = 0;

        public void prepare() {
            add("@Override");
            add("public int hashCode() {");
            add("    int h = 17;");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    h = hashCodePiece(h, getPos_" + fieldPos + "(), checkIfNull_" + fieldPos + "());");
        }

        public void end() {
            add("    return h;");
            add("}");
        }
    }

    static class FieldString extends TypeInFunctionStringOut {
        private List<Queue<Integer>> listOfQueuesForIds;
        private Schema schema;

        private int primitives = 0;
        private int isNulls = 0;

        private int booleanBytes = 0;
        private int booleans = 0;

        public void prepare() {
            String s = schema.toString();
            s = s.substring(1, s.length() - 1);
            add("private static Schema schema;");
            add("static {");
            add("    try {");
            add("        schema = Utils.getSchemaFromString(\""+s+"\");");
            add("    } catch (FrontendException e) {");
            add("        throw new RuntimeException(\"Unable to make Schema for Schema String: \\\""+s+"\\\"\");");
            add("    }");
            add("}");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                if (isPrimitive() && (primitives++ % 8 == 0))
                    add("private byte isNull_"+ isNulls++ +" = (byte)0xFF;"); //TODO make sure this is the right value for all 1's

                if (isBoolean() && booleans++ % 8 == 0) {
                    add("private byte booleanByte_"+ booleanBytes++ +";");
                } else {
                    add("private "+typeName()+" pos_"+fieldPos+";");
                }
            } else {
                int id = SchemaTupleClassGenerator.generateAndAddToJar(fs.schema);

                for (Queue<Integer> q : listOfQueuesForIds)
                    q.add(id);

                add("private SchemaTuple_"+id+" pos_"+fieldPos+";");
            }
        }

        @Override
        public void end() {
            addBreak();
            add("@Override");
            add("public Schema getSchema() {");
            add("    return schema;");
            add("}");
            addBreak();
        }

        public FieldString(List<Queue<Integer>> listOfQueuesForIds, Schema schema) {
            this.listOfQueuesForIds = listOfQueuesForIds;
            this.schema = schema;
        }
    }

    static class SetPosString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;

        private int byteField = 0; //this is for setting booleans
        private int byteIncr = 0; //this is for counting the booleans we've encountered

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                add("public void setPos_"+fieldPos+"("+typeName()+" v) {");
                if (isPrimitive())
                    add("    setNull_"+fieldPos+"(false);");

                if (!isBoolean()) {
                    add("    pos_"+fieldPos+" = v;");
                } else {
                    add("    if (v)");
                    add("        booleanByte_"+byteField+" |= (byte)"+CheckIfNullString.masks[byteIncr]+";");
                    add("    else");
                    add("        booleanByte_"+byteField+" &= (byte)"+SetNullString.inverseMasks[byteIncr++]+";");

                    if (byteIncr % 8 == 0) {
                        byteIncr = 0;
                        byteField++;
                    }
                }

                add("    updateLargestSetValue("+fieldPos+");");
                add("}");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("public void setPos_"+fieldPos+"(SchemaTuple_"+nestedSchemaTupleId+" t) {");
                add("    if (pos_"+fieldPos+" == null)");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    pos_"+fieldPos+".setSpecific(t);");
                add("    updateLargestSetValue("+fieldPos+");");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(SchemaTuple t) {");
                add("    if (pos_"+fieldPos+" == null)");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    try {");
                add("        pos_"+fieldPos+".set(t, false);");
                add("    } catch (ExecException e) {");
                add("        throw new RuntimeException(\"Unable to set position "+fieldPos+" with Tuple: \" + t, e);");
                add("    }");
                add("    updateLargestSetValue("+fieldPos+");");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(Tuple t) {");
                add("    if (pos_"+fieldPos+" == null)");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    try {");
                add("        pos_"+fieldPos+".set(t, false);");
                add("    } catch (ExecException e) {");
                add("        throw new RuntimeException(\"Unable to set position "+fieldPos+" with Tuple: \" + t, e);");
                add("    }");
                add("    updateLargestSetValue("+fieldPos+");");
                add("}");
            }
            addBreak();
        }

        public SetPosString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class GenericSetString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;

        public void prepare() {
            add("@Override");
            add("public void set(int fieldNum, Object val) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        //TODO leverage Java's auto-boxing/unboxing
        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            if (isBoolean()) {
                add("        setPos_"+fieldPos+"((Boolean)val);");
            } else if (isPrimitive() || isBytearray()) {
                add("        setPos_"+fieldPos+"(SchemaTuple.unbox(val, pos_"+fieldPos+"));");
            } else if (isString()) {
                add("        setPos_"+fieldPos+"((String)val);");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("        if (val instanceof SchemaTuple_"+nestedSchemaTupleId+")");
                add("            setPos_"+fieldPos+"((SchemaTuple_"+nestedSchemaTupleId+")val);");
                add("        else");
                add("            setPos_"+fieldPos+"((Tuple)val);");
            }
            add("        break;");
        }

        public void end() {
            add("    default:");
            add("        super.set(fieldNum, val);");
            add("}");
        }

        public GenericSetString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class GenericGetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public Object get(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            if (isPrimitive()) {
                //add("        return checkIfNull_"+fieldPos+"() ? null : SchemaTuple.box(getPos_"+fieldPos+"());");
                add("        return checkIfNull_"+fieldPos+"() ? null : getPos_"+fieldPos+"();");
            } else if (isBytearray()) {
                add("        return pos_"+fieldPos+" == null ? null : SchemaTuple.box(pos_"+fieldPos+");");
            } else {
                add("        return pos_"+fieldPos+";");
            }
        }

        public void end() {
            add("    default:");
            add("        return super.get(fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class GeneralIsNullString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public boolean isNull(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            add("        return checkIfNull_"+fieldPos+"();");
        }

        public void end() {
            add("    default:");
            add("        return super.isNull(fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class GeneralSetNullString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public void setNull(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            add("        setNull_"+fieldPos+"(true);");
            add("        break;");
        }

        public void end() {
            add("    default:");
            add("        super.setNull(fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class CheckIfNullString extends TypeInFunctionStringOut {
        private int nullByte = 0; //the byte_ val
        private int byteIncr = 0; //the mask we're on

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("public boolean checkIfNull_" + fieldPos + "() {");
            if (isPrimitive()) {
                add("    return BytesHelper.getBitByPos(isNull_" + nullByte + ", " + byteIncr++ +");");
                if (byteIncr % 8 == 0) {
                    byteIncr = 0;
                    nullByte++;
                }
            } else if (isTuple()) {
               add("    return pos_" + fieldPos + " == null || pos_" + fieldPos + ".isNull();");
            } else {
               add("    return pos_" + fieldPos + " == null;");
            }
            add("}");
            addBreak();
        }
    }

   static class SetNullString extends TypeInFunctionStringOut {
        private int nullByte = 0; //the byte_ val
        private int byteIncr = 0; //the mask we're on

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("public void setNull_"+fieldPos+"(boolean b) {");
            if (isPrimitive()) {
                add("    isNull_" + nullByte + " = BytesHelper.setBitByPos(isNull_" + nullByte + ", b, " + byteIncr++ + ");")
                if (byteIncr % 8 == 0) {
                    byteIncr = 0;
                    nullByte++;
                }
            } else {
                add("    pos_" + fieldPos + " = null;");
            }
            add("}");
            addBreak();
        }
    }

    //TODO in general, should I be calling t.isNull() on the tuple that is given?
    //TODO should this do something different if t is null?
    static class SetEqualToSchemaTupleSpecificString extends TypeInFunctionStringOut {
        private int id;

        public void prepare() {
            add("@Override");
            add("public SchemaTuple setSpecific(SchemaTuple_"+id+" t) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    setPos_"+fieldPos+"(t.getPos_"+fieldPos+"());");
            if (isPrimitive()) {
                add("    setNull_"+fieldPos+"(t.checkIfNull_"+fieldPos+"());");
            }
        }

        public void end() {
            add("    return super.setSpecific(t);");
            add("}");
            addBreak();
        }

        public SetEqualToSchemaTupleSpecificString(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    //this has to write the null state of all the fields, not just the null bytes, though those
    //will have to be reconstructed
    static class WriteNullsString extends TypeInFunctionStringOut {
        String s = "    boolean[] b = {\n";
        public void prepare() {
            add("public void writeNulls(DataOutput out) throws IOException {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            s += "        checkIfNull_"+fieldPos+"(),\n";
        }

        public void end() {
            s += "        appendIsNull(),\n";
            s = s.substring(0, s.length() - 2) + "\n    };";
            add(s);
            add("    SedesHelper.writeBooleanArray(out, b);");
            add("}");
            addBreak();
        }
    }

   static class ReadString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;
        int ct = 0;

        private int booleans = 0;
        private int booleanBytes = 0;

        public void prepare() {
            add("@Override");
            add("public void readFields(DataInput in) throws IOException {");
            add("    boolean[] b = SedesHelper.readBooleanArray(in, sizeNoAppend() + 1);");
            addBreak();
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (isPrimitive())
                add("    setNull_"+fieldPos+"(b["+fieldPos+"]);");

            if (isBoolean()) {
                if (booleans++ % 8 == 0)
                    booleanBytes++;
            } else if (!isTuple()) {
                add("    if (!b["+fieldPos+"])");
                add("        setPos_"+fieldPos+"(SchemaTuple.read(in, pos_"+fieldPos+"));");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("    if (!b["+fieldPos+"]) {");
                add("        SchemaTuple_"+nestedSchemaTupleId+" st = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("        st.readFields(in);");
                add("        setPos_"+fieldPos+"(st);");
                add("    }");
            }
            ct++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++)
                add("    booleanByte_"+i+" = in.readByte();");
            add("    if(!b["+ct+"])");
            add("        setAppend(SedesHelper.readGenericTuple(in, in.readByte()));");
            add("    updateLargestSetValue(size());");
            add("}");
            addBreak();
        }

        public ReadString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }


    static class WriteString extends TypeInFunctionStringOut {
        public void prepare() {
            add("public void writeElements(DataOutput out) throws IOException {");
            add("    writeNulls(out);");
        }

        private int booleans = 0;
        private int booleanBytes = 0;

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isBoolean()) {
                if (isPrimitive()) {
                    add("    if (!checkIfNull_"+fieldPos+"())");
                } else {
                    add("    if(pos_"+fieldPos+" != null)");
                }
                add("        SchemaTuple.write(out, pos_"+fieldPos+");");
            }

            if (isBoolean() && booleans++ % 8 == 0)
                booleanBytes++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++)
                add("    out.writeByte(booleanByte_"+i+");");
            add("    if (!appendIsNull())");
            add("        SedesHelper.writeGenericTuple(out, getAppend());");
            add("}");
            addBreak();
        }
    }

    //TODO need to include all of the objects from Schema (have it implement it's own getMemorySize()?
    static class MemorySizeString extends TypeInFunctionStringOut {
        private int size = 0;

        String s = "    return SizeUtil.roundToEight(super.getMemorySize() + ";

        public void prepare() {
            add("@Override");
            add("public long getMemorySize() {");
        }

        private int booleans = 0;
        private int primitives = 0;

        //TODO a null array or object variable still takes up space for the pointer, yes?
        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (isInt() || isFloat()) {
                size += 4;
            } else if (isLong() || isDouble()) {
                size += 8;
            } else if (isBytearray()) {
                s += "(pos_"+fieldPos+" == null ? 8 : SizeUtil.roundToEight(12 + pos_"+fieldPos+".length) * 8) + ";
            } else if (isString()) {
                s += "(pos_"+fieldPos+" == null ? 8 : SizeUtil.getPigObjMemSize(pos_"+fieldPos+")) + ";
            } else if (isBoolean()) {
                if (booleans++ % 8 == 0)
                    size++; //accounts for the byte used to store boolean values
            } else {
                s += "(pos_"+fieldPos+" == null ? 8 : pos_"+fieldPos+".getMemorySize()) + ";
            }

            if (isPrimitive() && primitives++ % 8 == 0)
                size++; //accounts for the null byte
        }

        public void end() {
            s += size + ");";
            add(s);
            add("}");
            addBreak();
        }
    }

    static class GetPosString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;

        private int booleanByte = 0;
        private int booleans;

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                add("public "+typeName()+" getPos_"+fieldPos+"() {");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("public SchemaTuple_"+nestedSchemaTupleId+" getPos_"+fieldPos+"() {");
            }
            if (isBoolean()) {
                add("    return (booleanByte_"+ booleanByte + " & (byte)"+CheckIfNullString.masks[booleans]+") > 0;");
                if (booleans++ % 8 == 0)
                    booleanByte++;
            } else {
                add("    return pos_"+fieldPos+";");
            }
            add("}");
            addBreak();
        }

        public GetPosString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class GetSchemaTupleIdentifierString extends TypeInFunctionStringOut {
        private int id;

        public void end() {
            add("@Override");
            add("public int getSchemaTupleIdentifier() {");
            add("    return "+id+";");
            add("}");
            addBreak();
        }

        public GetSchemaTupleIdentifierString(int id) {
            this.id = id;
        }
    }

    static class GetSchemaStringString extends TypeInFunctionStringOut {
        private Schema schema;

        public void end() {
            String schemaString = schema.toString();
            schemaString = schemaString.substring(1, schemaString.length() - 1);
            add("@Override");
            add("public String getSchemaString() {");
            add("    return \""+schemaString+"\";");
            add("}");
            addBreak();
        }

        public GetSchemaStringString(Schema schema) {
            this.schema = schema;
        }
    }

    static class SizeNoAppendString extends TypeInFunctionStringOut {
        int i = 0;

        public void process(int fieldNum, Schema.FieldSchema fS) {
            i++;
        }

        public void end() {
            add("@Override");
            add("public int sizeNoAppend() {");
            add("    return " + i + ";");
            add("}");
            addBreak();
        }
    }
/*
    static class SizeString extends TypeInFunctionStringOut {
        int i = 0;

        public void process(int fieldNum, Schema.FieldSchema fS) {
            i++;
        }

        public void end() {
            add("@Override");
            add("public int size() {");
            add("    return appendSize() + " + i + ";");
            add("}");
            addBreak();
        }
    }
*/
    static class GetTypeString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public byte getType(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    case ("+fieldNum+"): return "+fs.type+";");
        }

        public void end() {
            add("    default:");
            add("        super.getType(fieldNum);");
            add("    }");
            add("}");
            addBreak();
        }
    }

    static class SetEqualToSchemaTupleString extends TypeInFunctionStringOut {
        int id;

        public SetEqualToSchemaTupleString(int id) {
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("public SchemaTuple set(SchemaTuple t, boolean checkClass) throws ExecException {");
            add("    if (checkClass && t instanceof SchemaTuple_"+id+")");
            add("        return setSpecific((SchemaTuple_"+id+")t);");
            addBreak();
            add("    if (t.size() < sizeNoAppend())");
            add("        throw new ExecException(\"Given SchemaTuple does not have enough fields as \"+getClass()+\" (\"+t.size()+\" vs \"+sizeNoAppend()+\")\");");
            addBreak();
            add("    List<Schema.FieldSchema> theirFS = t.getSchema().getFields();");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    if ("+fs.type+" != theirFS.get("+fieldNum+").type)");
            add("        throw new ExecException(\"Given SchemaTuple does not match current\");");
            add("    if (t.isNull("+fieldNum+")) {");
            if (isPrimitive())
                add("        setNull_"+fieldNum+"(true);");
            else
                add("        pos_"+fieldNum+" = null;");
            add("    } else {");
            if (!isBytearray() && !isTuple()) {
                String s = typeName();
                s = s.substring(0,1).toUpperCase() + s.substring(1);
                add("        setPos_"+fieldNum+"(t.get"+s+"("+fieldNum+"));");
            } else if (isBytearray()) {
                add("        setPos_"+fieldNum+"(t.getBytes("+fieldNum+"));");
            } else {
                add("        setPos_"+fieldNum+"((Tuple)t.get("+fieldNum+"));");
            }
            add("    }");
        }

        public void end() {
            add("    appendReset();");
            add("    for (int j = sizeNoAppend(); j < t.size(); j++)");
            add("        append(t.get(j));");
            add("    updateLargestSetValue(size());");
            add("    return this;");
            add("}");
        }
    }

   static class PrimitiveGetString extends PrimitiveSetString {
        public PrimitiveGetString(byte type) {
            super(type);
        }

        public void prepare() {
            add("@Override");
            if (thisType()==DataType.BYTEARRAY)
                add("public byte[] getBytes(int fieldNum) throws ExecException {");
            else
                add("public "+name()+" get"+proper()+"(int fieldNum) throws ExecException {");
            add("    switch(fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (fs.type==thisType()) {
                add("    case ("+fieldNum+"): return getPos_"+fieldNum+"();");
            }
        }

        public void end() {
            add("    default:");
            add("        int diff = fieldNum - sizeNoAppend();");
            add("        if (diff < appendSize())");
            add("            return SchemaTuple.unbox(getAppend(diff), "+defValue()+");");
            add("        throw new ExecException(\"Given field \"+fieldNum+\" not a "+name()+" field!\");");
            add("    }");
            add("}");
        }
    }

    static class PrimitiveSetString extends TypeInFunctionStringOut {
        private byte type;

        public PrimitiveSetString(byte type) {
            this.type = type;
        }

        public byte thisType() {
            return type;
        }

        public String name() {
            return typeName(type);
        }

        public String proper() {
            if (thisType()==DataType.BYTEARRAY)
                return "Bytes";
            return name().substring(0,1).toUpperCase() + name().substring(1);
        }

        public String defValue() {
            switch (type) {
            case (DataType.INTEGER): return "0";
            case (DataType.LONG): return "1L";
            case (DataType.FLOAT): return "1.0f";
            case (DataType.DOUBLE): return "1.0";
            case (DataType.BOOLEAN): return "true";
            case (DataType.CHARARRAY): return "\"\"";
            case (DataType.BYTEARRAY): return "new byte[0]";
            default: throw new RuntimeException("Invalid type for defValue");
            }
        }

        public void prepare() {
            add("@Override");
            add("public void set"+proper()+"(int fieldNum, "+name()+" val) throws ExecException {");
            add("    switch(fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (fs.type==thisType())
                add("    case ("+fieldNum+"): setPos_"+fieldNum+"(val); break;");
        }

        public void end() {
            add("    default:");
            add("        int diff = fieldNum - sizeNoAppend();");
            add("        if (diff < appendSize()) {");
            add("            setAppend(diff, val);");
            add("            break;");
            add("        }");
            add("        throw new ExecException(\"Given field \"+fieldNum+\" not a "+name()+" field!\");");
            add("    }");
            add("}");
        }
    }

    //TODO need to use StringBuilder for all concatenation, not +
    static class TypeInFunctionStringOutFactory {
        private List<TypeInFunctionStringOut> listOfFutureMethods = Lists.newArrayList();
        private int id;

        public TypeInFunctionStringOutFactory(Schema s, int id) {
            this.id = id;

            Queue<Integer> nextNestedSchemaIdForSetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForGetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForGenericSet = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForReadField = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForCompareTo = Lists.newLinkedList();

            List<Queue<Integer>> listOfQueuesForIds = Lists.newArrayList(nextNestedSchemaIdForSetPos, nextNestedSchemaIdForGetPos, nextNestedSchemaIdForGenericSet, nextNestedSchemaIdForReadField, nextNestedSchemaIdForCompareTo);

            listOfFutureMethods.add(new FieldString(listOfQueuesForIds, s)); //has to be run first
            listOfFutureMethods.add(new SetPosString(nextNestedSchemaIdForSetPos));
            listOfFutureMethods.add(new GetPosString(nextNestedSchemaIdForGetPos));
            listOfFutureMethods.add(new GenericSetString(nextNestedSchemaIdForGenericSet));
            listOfFutureMethods.add(new GenericGetString());
            listOfFutureMethods.add(new GeneralIsNullString());
            listOfFutureMethods.add(new GeneralSetNullString());
            listOfFutureMethods.add(new CheckIfNullString());
            listOfFutureMethods.add(new SetNullString());
            listOfFutureMethods.add(new SetEqualToSchemaTupleSpecificString(id));
            listOfFutureMethods.add(new WriteNullsString());
            listOfFutureMethods.add(new ReadString(nextNestedSchemaIdForReadField));
            listOfFutureMethods.add(new WriteString());
            listOfFutureMethods.add(new MemorySizeString());
            listOfFutureMethods.add(new GetSchemaTupleIdentifierString(id));
            listOfFutureMethods.add(new GetSchemaStringString(s));
            listOfFutureMethods.add(new HashCode());
            listOfFutureMethods.add(new SizeNoAppendString());
            listOfFutureMethods.add(new GetTypeString());
            listOfFutureMethods.add(new CompareToString(nextNestedSchemaIdForCompareTo, id));
            listOfFutureMethods.add(new CompareToSpecificString(id));
            listOfFutureMethods.add(new SetEqualToSchemaTupleString(id));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.INTEGER));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.LONG));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.FLOAT));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.DOUBLE));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.BYTEARRAY));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.CHARARRAY));
            listOfFutureMethods.add(new PrimitiveSetString(DataType.BOOLEAN));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.INTEGER));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.LONG));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.FLOAT));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.DOUBLE));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.BYTEARRAY));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.CHARARRAY));
            listOfFutureMethods.add(new PrimitiveGetString(DataType.BOOLEAN));

            for (TypeInFunctionStringOut t : listOfFutureMethods)
                t.prepare();
        }

        public void process(Schema.FieldSchema fs) {
            for (TypeInFunctionStringOut t : listOfFutureMethods)
                t.prepareProcess(fs);
        }

        public String end() {
            StringBuffer head =
                new StringBuffer()
                    .append("import java.util.List;\n")
                    .append("import java.io.DataOutput;\n")
                    .append("import java.io.DataInput;\n")
                    .append("import java.io.IOException;\n")
                    .append("\n")
                    .append("import com.google.common.collect.Lists;\n")
                    .append("\n")
                    .append("import org.apache.pig.data.DataType;\n")
                    .append("import org.apache.pig.data.Tuple;\n")
                    .append("import org.apache.pig.data.SchemaTuple;\n")
                    .append("import org.apache.pig.data.utils.SedesHelper;\n")
                    .append("import org.apache.pig.data.utils.BytesHelper;\n")
                    .append("import org.apache.pig.data.DataByteArray;\n")
                    .append("import org.apache.pig.data.BinInterSedes;\n")
                    .append("import org.apache.pig.impl.util.Utils;\n")
                    .append("import org.apache.pig.impl.logicalLayer.schema.Schema;\n")
                    .append("import org.apache.pig.impl.logicalLayer.FrontendException;\n")
                    .append("import org.apache.pig.backend.executionengine.ExecException;\n")
                    .append("import org.apache.pig.data.SizeUtil;\n")
                    .append("\n")
                    .append("public class SchemaTuple_"+id+" extends SchemaTuple<SchemaTuple_"+id+"> {\n");

            for (TypeInFunctionStringOut t : listOfFutureMethods) {
                t.end();
                head.append(t.getContent());
            }

            return head.append("}").toString();
        }
    }

    static class TypeInFunctionStringOut {
        private int fieldPos = 0;
        private StringBuffer content = new StringBuffer();
        private byte type;

        public void prepare() {}
        public void process(int fieldPos, Schema.FieldSchema fs) {}
        public void end() {}

        public StringBuffer getContent() {
            return content;
        }

        public TypeInFunctionStringOut() {
            add("// this code generated by " + getClass());
            addBreak();
        }

        public StringBuffer spaces(int indent) {
            StringBuffer out = new StringBuffer();
            String space = "    ";
            for (int i = 0; i < indent; i++)
                out.append(space);
            return out;
        }

        public void add(String s) {
            for (String str : s.split("\\n"))
                content.append(spaces(1).append(str).append("\n"));
        }

        public void addBreak() {
            content.append("\n");
        }

        public void prepareProcess(Schema.FieldSchema fs) {
            type = fs.type;

            if (type==DataType.MAP || type==DataType.BAG)
                throw new RuntimeException("Map and Bag currently not supported by SchemaTuple");

            process(fieldPos, fs);
            fieldPos++;
        }

        public boolean isInt() {
            return type == DataType.INTEGER;
        }

        public boolean isLong() {
            return type == DataType.LONG;
        }

        public boolean isFloat() {
            return type == DataType.FLOAT;
        }

        public boolean isDouble() {
            return type == DataType.DOUBLE;
        }

        public boolean isPrimitive() {
            return isInt() || isLong() || isFloat() || isDouble() || isBoolean();
        }

        public boolean isBoolean() {
            return type == DataType.BOOLEAN;
        }

        public boolean isString() {
            return type == DataType.CHARARRAY;
        }

        public boolean isBytearray() {
            return type == DataType.BYTEARRAY;
        }

        public boolean isTuple() {
            return type == DataType.TUPLE;
        }

        public boolean isObject() {
            return !isPrimitive();
        }

        public String typeName() {
            return typeName(type);
        }

        public String typeName(byte type) {
            switch(type) {
                case (DataType.INTEGER): return "int";
                case (DataType.LONG): return "long";
                case (DataType.FLOAT): return "float";
                case (DataType.DOUBLE): return "double";
                case (DataType.BYTEARRAY): return "byte[]";
                case (DataType.CHARARRAY): return "String";
                case (DataType.BOOLEAN): return "boolean";
                default: throw new RuntimeException("Can't return String for given type " + DataType.findTypeName(type));
            }
        }
    }
}
