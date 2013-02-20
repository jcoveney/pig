package org.apache.pig.builtin;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.ICONST_2;
import static org.objectweb.asm.Opcodes.ICONST_3;
import static org.objectweb.asm.Opcodes.ICONST_4;
import static org.objectweb.asm.Opcodes.ICONST_5;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_6;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;

//TODO need to add support for ANY Pig type!
//TODO statically cache the generated code based on the input Strings
public class InvokerGenerator extends EvalFunc<Object> {
    private String className_;
    private String methodName_;
    private String[] argumentTypes_;

    private boolean isInitialized = false;

    private InvokerFunction generatedFunction;
    private Schema outputSchema;

    private static int uniqueId = 0;

    private static final Map<Class<?>, Byte> returnTypeMap = new HashMap<Class<?>, Byte>() {{
       put(String.class, DataType.CHARARRAY);
       put(Integer.class, DataType.INTEGER);
       put(Long.class, DataType.LONG);
       put(Float.class, DataType.FLOAT);
       put(Double.class, DataType.DOUBLE);
       put(Boolean.class, DataType.BOOLEAN);
       //put(byte[].class, DataType.BYTEARRAY);
       put(Integer.TYPE, DataType.INTEGER);
       put(Long.TYPE, DataType.LONG);
       put(Float.TYPE, DataType.FLOAT);
       put(Double.TYPE, DataType.DOUBLE);
       put(Boolean.TYPE, DataType.BOOLEAN);

    }};

    private static final Map<Class<?>, Class<?>> inverseTypeMap = new HashMap<Class<?>, Class<?>>() {{
       put(Integer.class, Integer.TYPE);
       put(Long.class, Long.TYPE);
       put(Float.class, Float.TYPE);
       put(Double.class, Double.TYPE);
       put(Boolean.class, Boolean.TYPE);
       put(Integer.TYPE, Integer.class);
       put(Long.TYPE, Long.class);
       put(Float.TYPE, Float.class);
       put(Double.TYPE, Double.class);
       put(Boolean.TYPE, Boolean.class);
    }};

    private static final Map<Class<?>, String> primitiveSignature = new HashMap<Class<?>, String>() {{
        put(Integer.TYPE, "I");
        put(Long.TYPE, "J");
        put(Float.TYPE, "F");
        put(Double.TYPE, "D");
        put(Boolean.TYPE, "Z");
    }};

    private static final Map<String,Class<?>> nameToClassObjectMap = new HashMap<String,Class<?>>() {{
        put("String",String.class);
        put("Integer", Integer.class);
        put("int", Integer.TYPE);
        put("Long", Long.class);
        put("long", Long.TYPE);
        put("Float", Float.class);
        put("float", Float.TYPE);
        put("Double", Double.class);
        put("double", Double.TYPE);
        put("Boolean", Boolean.class);
        put("boolean", Boolean.TYPE);
        //put("byte[]", byte[].class);
        put("java.lang.String",String.class);
        put("java.lang.Integer", Integer.class);
        put("java.lang.Long", Long.class);
        put("java.lang.Float", Float.class);
        put("java.lang.Double", Double.class);
        put("java.lang.Boolean", Boolean.class);
    }};

    public InvokerGenerator(String className, String methodName, String argumentTypes) {
        className_ = className;
        methodName_ = methodName;
        argumentTypes_ = argumentTypes.split(",");
        if ("".equals(argumentTypes)) {
        	argumentTypes_ = new String[0]; 
        }
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        if (!isInitialized)
            initialize();

        return generatedFunction.eval(input);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (!isInitialized)
            initialize();

        return outputSchema;
    }

    private static int getUniqueId() {
        return uniqueId++;
    }

    protected void initialize() {
        Class<?> clazz;
        try {
            clazz = PigContext.resolveClassName(className_); //TODO I should probably be using this for all of the Class<?> resolution
        } catch (IOException e) {
            throw new RuntimeException("Given className not found: " + className_, e);
        }

        Class<?>[] arguments = getArgumentClassArray(argumentTypes_);

        Method method;
        try {
            method = clazz.getMethod(methodName_, arguments); //must match exactly
        } catch (SecurityException e) {
            throw new RuntimeException("Not allowed to call given method: " + methodName_, e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Given method name does not exist: " + methodName_, e);
        }
        boolean isStatic = Modifier.isStatic(method.getModifiers());

        Class<?> returnClazz = method.getReturnType();

        Byte type = returnTypeMap.get(returnClazz);

        if (type == null) {
            throw new RuntimeException("Function returns invalid type: " + returnClazz);
        }

        outputSchema = new Schema();
        outputSchema.add(new Schema.FieldSchema(null, type));

        generatedFunction = generateInvokerFunction("InvokerFunction_"+getUniqueId(), method, isStatic, arguments);

        isInitialized = true;
    }

    private Class<?>[] getArgumentClassArray(String[] argumentTypes) {
        Class<?>[] arguments = new Class<?>[argumentTypes.length];
        for (int i = 0; i < argumentTypes.length; i++) {
            Class<?> clazz = nameToClassObjectMap.get(argumentTypes[i]);
            if (clazz == null) {
                throw new RuntimeException("Invalid argument type given: " + argumentTypes[i]);
            }
            arguments[i] = clazz;
        }
        return arguments;
    }

    private InvokerFunction generateInvokerFunction(String className, Method method, boolean isStatic, Class<?>[] arguments) {
        byte[] byteCode = generateInvokerFunctionBytecode(className, method, isStatic, arguments);

        return ByteClassLoader.getInvokerFunction(className, byteCode);
    }

    private byte[] generateInvokerFunctionBytecode(String className, Method method, boolean isStatic, Class<?>[] arguments) {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, className, null, "java/lang/Object", new String[] { "org/apache/pig/builtin/InvokerFunction" });

        makeConstructor(cw);

        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "eval", "(Lorg/apache/pig/data/Tuple;)Ljava/lang/Object;", null, new String[] { "java/io/IOException" });
        mv.visitCode();

        int next = 2;
        //this will get the arguments from the Tuple, cast them, and astore them
        int begin = 0;
        if (!isStatic)
            loadAndStoreArgument(mv, begin++, next++, getMethodStyleName(method.getDeclaringClass()));

        for (int i = 0; i < arguments.length; i++)
            loadAndStoreArgument(mv, i + begin, next++, getMethodStyleName(getObjectVersion(arguments[i])));

        //puts the arguments on the stack
        next = 2;

        if (!isStatic)
            mv.visitVarInsn(ALOAD, next++); //put the method receiver on the stack

        for (Class<?> arg : arguments) {
            mv.visitVarInsn(ALOAD, next++);
            boxIfPrimitive(mv, arg);
        }
        String signature = buildSignatureString(arguments, method.getReturnType());
        mv.visitMethodInsn(isStatic ? INVOKESTATIC : INVOKEVIRTUAL, getMethodStyleName(method.getDeclaringClass()), method.getName(), signature);
        mv.visitInsn(ARETURN);
        mv.visitMaxs(2, (isStatic ? 2 : 3) + arguments.length);
        mv.visitEnd();

        cw.visitEnd();

        return cw.toByteArray();
    }

    private String buildSignatureString(Class<?>[] arguments, Class<?> returnClazz) {
        String sig = "(";
        for (Class<?> arg : arguments) {
            if (!arg.isPrimitive())
                sig += "L" + getMethodStyleName(arg) + ";";
            else
                sig += getMethodStyleName(arg);
        }
        sig += ")";

        if (!returnClazz.isPrimitive())
            sig += "L" + getMethodStyleName(returnClazz) + ";";
        else
            sig += getMethodStyleName(returnClazz);

        return sig;

    }

    private Class<?> getObjectVersion(Class<?> clazz) {
        if (clazz.isPrimitive())
            return inverseTypeMap.get(clazz);
        return clazz;

    }

    private String getMethodStyleName(Class<?> clazz) {
        if (!clazz.isPrimitive())
            return clazz.getCanonicalName().replaceAll("\\.","/");
        return primitiveSignature.get(clazz);
    }

    private void boxIfPrimitive(MethodVisitor mv, Class<?> clazz) {
        if (!clazz.isPrimitive())
            return;
        String methodName = clazz.getSimpleName()+"Value";
        mv.visitMethodInsn(INVOKEVIRTUAL, getMethodStyleName(inverseTypeMap.get(clazz)), methodName, "()"+getMethodStyleName(clazz));
    }

    private void loadAndStoreArgument(MethodVisitor mv, int loadIdx, int storeIdx, String castName) {
        mv.visitVarInsn(ALOAD, 1); //loads the 1st argument
        addConst(mv, loadIdx);
        mv.visitMethodInsn(INVOKEINTERFACE, "org/apache/pig/data/Tuple", "get", "(I)Ljava/lang/Object;");
        mv.visitTypeInsn(CHECKCAST, castName);
        mv.visitVarInsn(ASTORE, storeIdx);
    }

    private void addConst(MethodVisitor mv, int idx) {
        switch (idx) {
            case(0): mv.visitInsn(ICONST_0); break;
            case(1): mv.visitInsn(ICONST_1); break;
            case(2): mv.visitInsn(ICONST_2); break;
            case(3): mv.visitInsn(ICONST_3); break;
            case(4): mv.visitInsn(ICONST_4); break;
            case(5): mv.visitInsn(ICONST_5); break;
            default:
                throw new RuntimeException("Invalid index given to addConst: " + idx);
        }
    }

    private void makeConstructor(ClassWriter cw) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    static class ByteClassLoader extends ClassLoader {
        private byte[] buf;

        public ByteClassLoader(byte[] buf) {
            this.buf = buf;
        }

        public Class<InvokerFunction> findClass(String name) {
            return (Class<InvokerFunction>)defineClass(name, buf, 0, buf.length);
        }

        public static InvokerFunction getInvokerFunction(String name, byte[] buf) {
            try {
            	//TODO remove
            	try {
					OutputStream os = new FileOutputStream(new File(name + ".class"));
					os.write(buf);
					os.close();
				} catch (Exception e) {
					throw new RuntimeException("AHHHH", e);
				}
            	//TODO remove
                return new ByteClassLoader(buf).findClass(name).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}