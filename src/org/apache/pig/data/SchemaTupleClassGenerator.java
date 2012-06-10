package org.apache.pig.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.StructuresHelper.Pair;
import org.apache.pig.data.utils.StructuresHelper.SchemaKey;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

//TODO: need to deal with the raw comparator issue

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SchemaTupleClassGenerator {
    private static final Log LOG = LogFactory.getLog(SchemaTupleClassGenerator.class);

    /**
     * This is the temporary directory into which all generated code is written.
     * This is known and written to statically, generally depending on the
     * lifecycle of the creation of Pig jobs.
     */
    private static File generatedCodeTempDir = Files.createTempDir();

    static {
        generatedCodeTempDir.deleteOnExit();
        LOG.debug("Temporary directory for generated code created: "
                + generatedCodeTempDir.getAbsolutePath());
    }

    protected static File getGenerateCodeTempDir() {
        return generatedCodeTempDir;
    }

    protected static File[] getGeneratedFiles() {
        return generatedCodeTempDir.listFiles();
    }

    protected static File tempFile(String name) {
        return new File(generatedCodeTempDir, name);
    }

    /**
     * This key is used in the job conf to let the various jobs know what code was
     * generated.
     */
    public static final String GENERATED_CLASSES_KEY = "pig.schematuple.classes";

    /**
     * This key must be set to true by the user for code generation to be used.
     * In the future, it may be turned on by default (at least in certain cases),
     * but for now it is too experimental.
     */
    public static final String SHOULD_GENERATE_KEY = "pig.schematuple";

    /**
     * This value is used to distinguish all of the generated code.
     * The general naming scheme used is SchemaTupe_identifier. Note that
     * identifiers are incremented before code is actually generated.
     */
    private static int globalClassIdentifier = 0;

    /**
     * This class actually generates the code for a given Schema.
     * @param schema
     * @param true or false depending on whether it should be appendable
     * @param identifier
     */
    private static void generateSchemaTuple(Schema s, boolean appendable, int id) {
        String codeString = produceCodeString(s, appendable, id);

        String name = "SchemaTuple_" + id;

        LOG.info("Compiling class " + name + " for Schema: " + s);
        compileCodeString(name, codeString);
    }

    private static int generateSchemaTuple(Schema s, boolean appendable) {
        int id = getGlobalClassIdentifier();

        generateSchemaTuple(s, appendable, id);

        return id;
    }

    /**
     * Schemas registered for generation are held here.
     */
    private static Map<SchemaKey, Pair<Integer, Boolean>> schemasToGenerate = Maps.newHashMap();

    /**
     * This sets into motion the generation of all "registered" Schemas. All code will be generated
     * into the temporary directory.
     * @return true of false depending on if there are any files to copy to the distributed cache
     */
    public static boolean generateAllSchemaTuples() {
        boolean filesToShip = false;
        LOG.info("Generating all registered Schemas.");
        for (Map.Entry<SchemaKey, Pair<Integer,Boolean>> entry : schemasToGenerate.entrySet()) {
            Schema s = entry.getKey().get();
            Pair<Integer,Boolean> value = entry.getValue();
            int id = value.getFirst();
            boolean isAppendable = value.getSecond();
            SchemaTupleClassGenerator.generateSchemaTuple(s, isAppendable, id);
            filesToShip = true;
        }
        return filesToShip;
    }

    /**
     * This method "registers" a Schema to be generated. It allows a portions of the code
     * to register a Schema for generation without knowing whether code generation is enabled.
     * A unique ID will be passed back, so in the actual M/R job, code needs to make sure that
     * generation was turned on or else the classes will not be present!
     * @param   udfSchema
     * @param   isAppendable
     * @return  identifier
     */
    public static int registerToGenerateIfPossible(Schema udfSchema, boolean isAppendable) {
        SchemaKey sk = new SchemaKey(udfSchema);
        Pair<Integer, Boolean> pr = schemasToGenerate.get(sk);
        if (pr != null) {
            return pr.getFirst();
        }
        if (!SchemaTupleFactory.isGeneratable(udfSchema)) {
            return -1;
        }
        int id = getGlobalClassIdentifier();
        schemasToGenerate.put(sk, Pair.make(Integer.valueOf(id), isAppendable));
        LOG.info("Registering "+(isAppendable ? "Appendable" : "")+"Schema for generation [" + udfSchema + "] with id [" + id + "]");
        return id;
    }

    /**
     * This method copies all class files present in the local temp directory to the distributed cache.
     * All copied files will have a symlink of their name. No files will be copied if the current
     * job is being run from local mode.
     * @param pigContext
     * @param conf
     */
    public static void copyAllGeneratedToDistributedCache(PigContext pigContext, Configuration conf) {
        LOG.info("Starting process to move generated code to distributed cacche");
        if (pigContext.getExecType() == ExecType.LOCAL) {
            LOG.info("Distributed cache not supported or needed in local mode.");
            return;
        }
        DistributedCache.createSymlink(conf); // we will read using symlinks
        StringBuilder serialized = new StringBuilder();
        boolean first = true;
        // We attempt to copy over every file in the generated code temp directory
        for (File f : getGeneratedFiles()) {
            if (first) {
                first = false;
            } else {
                serialized.append(",");
            }
            String symlink = f.getName(); //the class name will also be the symlink
            serialized.append(symlink);
            Path src = new Path(f.toURI());
            Path dst;
            try {
                dst = FileLocalizer.getTemporaryPath(pigContext);
            } catch (IOException e) {
                throw new RuntimeException("Error getting temporary path in HDFS", e);
            }
            FileSystem fs;
            try {
                fs = dst.getFileSystem(conf);
            } catch (IOException e) {
                throw new RuntimeException("Unable to get FileSystem", e);
            }
            try {
                fs.copyFromLocalFile(src, dst);
            } catch (IOException e) {
                throw new RuntimeException("Unable to copy from local filesystem to HDFS", e);
            }

            String destination = dst.toString() + "#" + symlink;

            try {
                DistributedCache.addCacheFile(new URI(destination), conf);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Unable to add file to distributed cache: " + destination, e);
            }
            LOG.info("File successfully added to the distributed cache: " + symlink);
        }
        String toSer = serialized.toString();
        LOG.info("Setting key [" + GENERATED_CLASSES_KEY + "] with classes to deserialize [" + toSer + "]");
        // we must set a key in the job conf so individual jobs know to resolve the shipped classes
        conf.set(GENERATED_CLASSES_KEY, toSer);
    }

    /**
     * This method copies all generated code that was shipped via
     * SchemaTupleClassGenerator.copyAllGeneratedToDistributedCache into the
     * local directory for generated code. This is done in the actual map/reduce
     * job.
     * @param   conf
     * @throws  IOException
     */
    protected static void copyAllFromDistributedCache(Configuration conf) throws IOException {
        String toDeserialize = conf.get(GENERATED_CLASSES_KEY);
        if (toDeserialize == null) {
            LOG.info("No classes in in key [" + GENERATED_CLASSES_KEY + "] to copy from distributed cache.");
            return;
        }
        LOG.info("Copying files in key ["+GENERATED_CLASSES_KEY+"] from distributed cache: " + toDeserialize);
        for (String s : toDeserialize.split(",")) {
            LOG.info("Attempting to read file: " + s);
            // The string is the symlink into the distributed cache
            FileInputStream fin = new FileInputStream(new File(s));
            FileOutputStream fos = new FileOutputStream(tempFile(s));

            int read;
            byte[] buf = new byte[1024*1024];
            while ((read = fin.read(buf)) > -1) {
                fos.write(buf, 0, read);
            }
            fin.close();
            fos.close();
            LOG.info("Successfully copied file to local directory.");
        }
    }

    /**
     * This method generates the actual SchemaTuple for the given Schema.
     * @param   schema
     * @param   whether the class should be appendable
     * @param   identifier
     * @return  the generated class's implementation
     */
    private static String produceCodeString(Schema s, boolean appendable, int id) {
        TypeInFunctionStringOutFactory f = new TypeInFunctionStringOutFactory(s, id, appendable);

        for (Schema.FieldSchema fs : s.getFields()) {
            f.process(fs);
        }

        return f.end();
    }

    private static int getGlobalClassIdentifier() {
        return globalClassIdentifier++;
    }

    /**
     * This method takes generated code, and compiles it down to a class file. It will output
     * the generated class file to the static temporary directory for generated code. Note
     * that the compiler will use the classpath that Pig is instantiated with, as well as the
     * generated directory.
     *
     * @param String of generated code
     * @param name of class
     */
    //TODO in the future, we can use ASM to generate the bytecode directly.
    private static void compileCodeString(String className, String generatedCodeString) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = Lists.newArrayList(new JavaSourceFromString(className, generatedCodeString));

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

        String tempDir = generatedCodeTempDir.getAbsolutePath();

        String classPath = System.getProperty("java.class.path") + ":" + tempDir;
        LOG.debug("Compiling SchemaTuple code with classpath: " + classPath);

        List<String> optionList = Lists.newArrayList();
        // Adds the current classpath to the compiler along with our generated code
        optionList.add("-classpath");
        optionList.add(classPath);
        optionList.add("-d");
        optionList.add(tempDir);

        if (!compiler.getTask(null, fileManager, diagnostics, optionList, null, compilationUnits).call()) {
            LOG.warn("Error compiling: " + className + ". Printing compilation errors and shutting down.");
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                LOG.warn("Error on line " + diagnostic.getLineNumber() + ": " + diagnostic.getMessage(Locale.US));
            }
            throw new RuntimeException("Unable to compile code string:\n" + generatedCodeString);
        }

        LOG.info("Successfully compiled class: " + className);
    }

    /**
     * This class allows code to be generated directly from a String, instead of having to be
     * on disk.
     */
    private static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.','/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    static class CompareToSpecificString extends TypeInFunctionStringOut {
        private int id;

        public CompareToSpecificString(int id, boolean appendable) {
            super(appendable);
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("protected int compareToSpecific(SchemaTuple_"+id+" t) {");
            if (isAppendable()) {
                add("    int i = compareSizeSpecific(t);");
                add("    if (i != 0) {");
                add("        return i;");
                add("    }");
            } else {
                add("    int i = 0;");
            }
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    i = compareNull(checkIfNull_" + fieldNum + "(), t.checkIfNull_" + fieldNum + "());");
            add("    switch (i) {");
            add("    case(1):");
            add("        return 1;");
            add("    case(-1):");
            add("        return -1;");
            add("    case(0):");
            add("        i = compare(getPos_" + fieldNum + "(), t.getPos_" + fieldNum + "());");
            add("        if (i != 0) {");
            add("            return i;");
            add("        }");
            add("    }");
        }

        public void end() {
            add("    return super.compareToSpecific(t);");
            add("}");
        }
    }

    //TODO clear up how it deals with nulls etc. IE is the logic correct
    static class CompareToString extends TypeInFunctionStringOut {
        private int id;

        public CompareToString(int id) {
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("protected int compareTo(SchemaTuple t, boolean checkType) {");
            add("    if (checkType && t instanceof SchemaTuple_"+id+") {");
            add("        return compareToSpecific((SchemaTuple_"+id+")t);");
            add("    }");
            add("    int i = compareSize(t);");
            add("    if (i != 0) {");
            add("        return i;");
            add("    }");
        }

        boolean compTup = false;
        boolean compStr = false;
        boolean compIsNull = false;
        boolean compByte = false;

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    i = compareNull(checkIfNull_" + fieldNum + "(), t, " + fieldNum + ");");
            add("    switch (i) {");
            add("    case(1):");
            add("        return 1;");
            add("    case(-1):");
            add("        return -1;");
            add("    case(0):");
            add("        i = compare(getPos_" + fieldNum + "(), t, " + fieldNum + ");");
            add("        if (i != 0) {");
            add("            return i;");
            add("        }");
            add("    }");
        }

        public void end() {
            add("    return super.compareTo(t, false);");
            add("}");
        }
    }

    static class HashCode extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public int hashCode() {");
            add("    int h = 17;");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    h = hashCodePiece(h, getPos_" + fieldPos + "(), checkIfNull_" + fieldPos + "());");
        }

        public void end() {
            add("    return h + super.hashCode();");
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
            add("private static Schema schema = staticSchemaGen(\"" + s + "\");");
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
                int id = SchemaTupleClassGenerator.generateSchemaTuple(fs.schema, isAppendable());

                for (Queue<Integer> q : listOfQueuesForIds) {
                    q.add(id);
                }

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

        public FieldString(List<Queue<Integer>> listOfQueuesForIds, Schema schema, boolean appendable) {
            super(appendable);
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
                    add("    booleanByte_" + byteField + " = BytesHelper.setBitByPos(booleanByte_" + byteField + ", v, " + byteIncr++ + ");");

                    if (byteIncr % 8 == 0) {
                        byteIncr = 0;
                        byteField++;
                    }
                }

                add("}");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("public void setPos_"+fieldPos+"(SchemaTuple_"+nestedSchemaTupleId+" t) {");
                add("    pos_" + fieldPos + " = t;");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(SchemaTuple t) {");
                add("    if (pos_"+fieldPos+" == null) {");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    }");
                add("    pos_" + fieldPos + ".proxySetAndCatch(t);");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(Tuple t) {");
                add("    if (pos_"+fieldPos+" == null) {");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    }");
                add("    pos_" + fieldPos + ".proxySetAndCatch(t);");
                add("}");
            }
            addBreak();
        }

        // these methods just serve as a protected proxy for for the protected methods they wrap
        public void end() {
            add("public void proxySetAndCatch(Tuple t) {");
            add("    super.setAndCatch(t);");
            add("}");
            addBreak();
            add("public void proxySetAndCatch(SchemaTuple<?> t) {");
            add("    super.setAndCatch(t);");
            add("}");
            addBreak();
        }

        public SetPosString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class GenericSetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public void set(int fieldNum, Object val) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            add("        if (val == null) {");
            add("            setNull_" + fieldPos + "(true);");
            add("            return;");
            add("        }");
            add("        setPos_"+fieldPos+"(unbox(val, getDummy_"+fieldPos+"()));");
            add("        break;");
        }

        public void end() {
            add("    default:");
            add("        super.set(fieldNum, val);");
            add("    }");
            add("}");
        }
    }

    static class GenericGetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public Object get(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"): return checkIfNull_"+fieldPos+"() ? null : box(getPos_"+fieldPos+"());");
        }

        public void end() {
            add("    default: return super.get(fieldNum);");
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
            add("    case ("+fieldPos+"): return checkIfNull_"+fieldPos+"();");
        }

        public void end() {
            add("    default: return super.isNull(fieldNum);");
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
            add("    case ("+fieldPos+"): setNull_"+fieldPos+"(true); break;");
        }

        public void end() {
            add("    default: super.setNull(fieldNum);");
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
               add("    return pos_" + fieldPos + " == null;");
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
                add("    isNull_" + nullByte + " = BytesHelper.setBitByPos(isNull_" + nullByte + ", b, " + byteIncr++ + ");");
                if (byteIncr % 8 == 0) {
                    byteIncr = 0;
                    nullByte++;
                }
            } else {
                add("    if (b) {");
                add("        pos_" + fieldPos + " = null;");
                add("    }");
            }
            add("}");
            addBreak();
        }
    }

    //TODO should this do something different if t is null?
    static class SetEqualToSchemaTupleSpecificString extends TypeInFunctionStringOut {
        private int id;

        public void prepare() {
            add("@Override");
            add("protected SchemaTuple setSpecific(SchemaTuple_"+id+" t) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    if (t.checkIfNull_" + fieldPos + "()) {");
            add("        setNull_" + fieldPos + "(true);");
            add("    } else {");
            add("        setPos_"+fieldPos+"(t.getPos_"+fieldPos+"());");
            add("    }");
            addBreak();
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
            if (isAppendable()) {
                s += "        appendIsNull(),\n";
            }
            s = s.substring(0, s.length() - 2) + "\n    };";
            add(s);
            add("    SedesHelper.writeBooleanArray(out, b);");
            add("}");
            addBreak();
        }

        public WriteNullsString(boolean appendable) {
            super(appendable);
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
            if (isAppendable()) {
                add("    boolean[] b = SedesHelper.readBooleanArray(in, sizeNoAppend() + 1);");
            } else {
                add("    boolean[] b = SedesHelper.readBooleanArray(in, sizeNoAppend());");
            }
            addBreak();
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (isBoolean()) {
                if (booleans++ % 8 == 0) {
                    booleanBytes++;
                }
            } else if (!isTuple()) {
                add("    if (b["+fieldPos+"]) {");
                add("        setNull_"+fieldPos+"(true);");
                add("    } else {");
                add("        setPos_"+fieldPos+"(read(in, pos_"+fieldPos+"));");
                add("    }");
                addBreak();
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("    if (b["+fieldPos+"]) {");
                add("        setNull_"+fieldPos+"(true);");
                add("    } else {");
                add("        SchemaTuple_"+nestedSchemaTupleId+" st = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("        st.readFields(in);");
                add("        setPos_"+fieldPos+"(st);");
                add("    }");
                addBreak();
            }
            ct++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++)
                add("    booleanByte_"+i+" = in.readByte();");
            if (isAppendable()) {
                add("    if(!b["+ct+"]) {");
                add("        setAppend(SedesHelper.readGenericTuple(in, in.readByte()));");
                add("    }");
            }
            add("}");
            addBreak();
        }

        public ReadString(Queue<Integer> idQueue, boolean appendable) {
            super(appendable);
            this.idQueue = idQueue;
        }
    }


    static class WriteString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("protected void writeElements(DataOutput out) throws IOException {");
            add("    writeNulls(out);");
        }

        private int booleans = 0;
        private int booleanBytes = 0;

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isBoolean()) {
                add("    if (!checkIfNull_"+fieldPos+"()) {");
                add("        write(out, pos_"+fieldPos+");");
                add("    }");
                addBreak();
            }

            if (isBoolean() && booleans++ % 8 == 0)
                booleanBytes++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++) {
                add("    out.writeByte(booleanByte_"+i+");");
            }
            add("    super.writeElements(out);");
            add("}");
            addBreak();
        }
    }

    //TODO need to include all of the objects from Schema (have it implement it's own getMemorySize()?
    static class MemorySizeString extends TypeInFunctionStringOut {
        private int size = 0;

        String s = "    return super.getMemorySize() + SizeUtil.roundToEight(";

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
                if (booleans++ % 8 == 0) {
                    size++; //accounts for the byte used to store boolean values
                }
            } else {
                s += "(pos_"+fieldPos+" == null ? 8 : pos_"+fieldPos+".getMemorySize()) + ";
            }

            if (isPrimitive() && primitives++ % 8 == 0) {
                size++; //accounts for the null byte
            }
        }

        public void end() {
            s += size + ");";
            add(s);
            add("}");
            addBreak();
        }
    }

    static class GetDummyString extends TypeInFunctionStringOut {
        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                add("public "+typeName()+" getDummy_"+fieldPos+"() {");
            } else {
                add("public Tuple getDummy_"+fieldPos+"() {");
            }
            switch (fs.type) {
            case (DataType.INTEGER): add("    return 0;"); break;
            case (DataType.LONG): add("    return 0L;"); break;
            case (DataType.FLOAT): add("    return 0.0f;"); break;
            case (DataType.DOUBLE): add("    return 0.0;"); break;
            case (DataType.BOOLEAN): add("    return true;"); break;
            case (DataType.BYTEARRAY): add("    return (byte[])null;"); break;
            case (DataType.CHARARRAY): add("    return (String)null;"); break;
            case (DataType.TUPLE): add("    return (Tuple)null;"); break;
            }
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
                add("public SchemaTuple_" + nestedSchemaTupleId + " getPos_"+fieldPos+"() {");
            }
            if (isBoolean()) {
                add("    return BytesHelper.getBitByPos(booleanByte_" + booleanByte + ", " + booleans++ + ");");
                if (booleans % 8 == 0) {
                    booleanByte++;
                }
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
            add("@Override");
            add("public String getSchemaString() {");
            add("    return \"" + schema.toString() + "\";");
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
            add("protected int sizeNoAppend() {");
            add("    return " + i + ";");
            add("}");
            addBreak();
        }
    }

    static class SizeString extends TypeInFunctionStringOut {
        int i = 0;

        public void process(int fieldNum, Schema.FieldSchema fS) {
            i++;
        }

        public void end() {
            add("@Override");
            add("public int size() {");
            if (isAppendable()) {
                add("    return appendSize() + " + i + ";");
            } else {
                add("    return " + i + ";");
            }
            add("}");
            addBreak();
        }

        public SizeString(boolean appendable) {
            super(appendable);
        }
    }

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
            add("    default: return super.getType(fieldNum);");
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
            add("protected SchemaTuple set(SchemaTuple t, boolean checkClass) throws ExecException {");
            add("    if (checkClass && t instanceof SchemaTuple_"+id+") {");
            add("        return setSpecific((SchemaTuple_"+id+")t);");
            add("    }");
            addBreak();
            add("    if (t.size() < sizeNoAppend()) {");
            add("        throw new ExecException(\"Given SchemaTuple does not have as many fields as \"+getClass()+\" (\"+t.size()+\" vs \"+sizeNoAppend()+\")\");");
            add("    }");
            addBreak();
            add("    List<Schema.FieldSchema> theirFS = t.getSchema().getFields();");
            addBreak();
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    if ("+fs.type+" != theirFS.get("+fieldNum+").type) {");
            add("        throw new ExecException(\"Given SchemaTuple does not match current in field " + fieldNum + ". Expected type: " + fs.type + ", found: \" + theirFS.get("+fieldNum+").type);");
            add("    }");
            add("    if (t.isNull("+fieldNum+")) {");
            add("        setNull_"+fieldNum+"(true);");
            add("    } else {");
            if (!isTuple()) {
                add("        setPos_"+fieldNum+"(t.get" + proper(fs.type) + "("+fieldNum+"));");
            } else {
                add("        setPos_"+fieldNum+"((Tuple)t.get("+fieldNum+"));");
            }
            add("    }");
            addBreak();
        }

        public void end() {
            add("    return super.set(t, checkClass);");
            add("}");
        }
    }

   static class PrimitiveGetString extends PrimitiveSetString {
        public PrimitiveGetString(byte type) {
            super(type);
        }

        public void prepare() {
            add("@Override");
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
            add("        return super.get" + proper() + "(fieldNum);");
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

        public String proper() {
            return proper(thisType());
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
            add("    default: super.set"+proper()+"(fieldNum, val);");
            add("    }");
            add("}");
        }
    }

    //TODO need to use StringBuilder for all concatenation, not +
    static class TypeInFunctionStringOutFactory {
        private List<TypeInFunctionStringOut> listOfFutureMethods = Lists.newArrayList();
        private int id;
        private boolean appendable;

        public TypeInFunctionStringOutFactory(Schema s, int id, boolean appendable) {
            this.id = id;
            this.appendable = appendable;

            Queue<Integer> nextNestedSchemaIdForSetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForGetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForReadField = Lists.newLinkedList();

            List<Queue<Integer>> listOfQueuesForIds = Lists.newArrayList(nextNestedSchemaIdForSetPos, nextNestedSchemaIdForGetPos, nextNestedSchemaIdForReadField);

            listOfFutureMethods.add(new FieldString(listOfQueuesForIds, s, appendable)); //has to be run first
            listOfFutureMethods.add(new SetPosString(nextNestedSchemaIdForSetPos));
            listOfFutureMethods.add(new GetPosString(nextNestedSchemaIdForGetPos));
            listOfFutureMethods.add(new GetDummyString());
            listOfFutureMethods.add(new GenericSetString());
            listOfFutureMethods.add(new GenericGetString());
            listOfFutureMethods.add(new GeneralIsNullString());
            listOfFutureMethods.add(new GeneralSetNullString());
            listOfFutureMethods.add(new CheckIfNullString());
            listOfFutureMethods.add(new SetNullString());
            listOfFutureMethods.add(new SetEqualToSchemaTupleSpecificString(id));
            listOfFutureMethods.add(new WriteNullsString(appendable));
            listOfFutureMethods.add(new ReadString(nextNestedSchemaIdForReadField, appendable));
            listOfFutureMethods.add(new WriteString());
            listOfFutureMethods.add(new SizeString(appendable));
            listOfFutureMethods.add(new MemorySizeString());
            listOfFutureMethods.add(new GetSchemaTupleIdentifierString(id));
            listOfFutureMethods.add(new GetSchemaStringString(s));
            listOfFutureMethods.add(new HashCode());
            listOfFutureMethods.add(new SizeNoAppendString());
            listOfFutureMethods.add(new GetTypeString());
            listOfFutureMethods.add(new CompareToString(id));
            listOfFutureMethods.add(new CompareToSpecificString(id, appendable));
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

            for (TypeInFunctionStringOut t : listOfFutureMethods) {
                t.prepare();
            }
        }

        public void process(Schema.FieldSchema fs) {
            for (TypeInFunctionStringOut t : listOfFutureMethods)
                t.prepareProcess(fs);
        }

        public String end() {
            StringBuilder head =
                new StringBuilder()
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
                    .append("import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;\n")
                    .append("\n");

            if (appendable) {
                head.append("public class SchemaTuple_"+id+" extends AppendableSchemaTuple<SchemaTuple_"+id+"> {\n");
            } else {
                head.append("public class SchemaTuple_"+id+" extends SchemaTuple<SchemaTuple_"+id+"> {\n");
            }

            for (TypeInFunctionStringOut t : listOfFutureMethods) {
                t.end();
                head.append(t.getContent());
            }

            head.append("\n")
                .append("    @Override\n")
                .append("    public SchemaTupleQuickGenerator<SchemaTuple_" + id + "> getQuickGenerator() {\n")
                .append("        return new SchemaTupleQuickGenerator<SchemaTuple_" + id + ">() {\n")
                .append("            @Override\n")
                .append("            public SchemaTuple_" + id + " make() {\n")
                .append("                return new SchemaTuple_" + id + "();\n")
                .append("            }\n")
                .append("        };\n")
                .append("    }\n");

            return head.append("}").toString();
        }
    }

    static class TypeInFunctionStringOut {
        private int fieldPos = 0;
        private StringBuilder content = new StringBuilder();
        private byte type;

        public void prepare() {}
        public void process(int fieldPos, Schema.FieldSchema fs) {}
        public void end() {}

        public int appendable = -1;

        public StringBuilder getContent() {
            return content;
        }

        public TypeInFunctionStringOut() {
            add("// this code generated by " + getClass());
            addBreak();
        }

        public boolean isAppendable() {
            if (appendable == -1) {
                throw new RuntimeException("Need to be given appendable status in " + getClass());
            }
            return appendable == 1;
        }

        public TypeInFunctionStringOut(boolean appendable) {
            super();
            this.appendable = appendable ? 1 : 0;
        }

        public StringBuilder spaces(int indent) {
            StringBuilder out = new StringBuilder();
            String space = "    ";
            for (int i = 0; i < indent; i++) {
                out.append(space);
            }
            return out;
        }

        public void add(String s) {
            for (String str : s.split("\\n")) {
                content.append(spaces(1).append(str).append("\n"));
            }
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

        public String proper(byte type) {
            String s = typeName(type);
            return type == DataType.BYTEARRAY ? "Bytes" : s.substring(0,1).toUpperCase() + s.substring(1);
        }
    }
}