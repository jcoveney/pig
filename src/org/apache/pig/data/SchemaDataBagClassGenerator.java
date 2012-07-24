package org.apache.pig.data;

import java.io.File;
import java.util.List;

import org.apache.pig.data.SchemaTupleClassGenerator.TypeInFunctionStringOut;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

public class SchemaDataBagClassGenerator {
    static class TypeInFunctionStringOutFactory {
        private List<TypeInFunctionStringOut> listOfFutureMethods = Lists.newArrayList();

        public TypeInFunctionStringOutFactory(Schema s, int id, int schemaTupleId, String contextAnnotations, File codeDir) {

        }
    }

    static class DefineFields extends TypeInFunctionStringOut {
        public void process(int fieldNum, Schema.FieldSchema fs) {
            String columnName = proper(fs.type) + "SpillableColumn";
            add(columnName + " pos_" + fieldNum + " = BagFactory.get" + columnName + "()");
            //TODO if it is a Tuple or Bag, recursively gen
        }
    }

    static class IsSpecificSchemaTuple extends TypeInFunctionStringOut {
        int schemaTupleId;

        public IsSpecificSchemaTuple(int schemaTupleId) {
            this.schemaTupleId = schemaTupleId;
        }

        public void prepare() {
            add("@Override");
            add("public boolean isSpecificSchemaTuple(Tuple t) {");
            add("    return t instanceof SchemaTuple_" + schemaTupleId + ";");
            add("}");
        }
    }

    static class GetSchemaTupleBagIdentifier extends TypeInFunctionStringOut {
        int bagId;

        public GetSchemaTupleBagIdentifier(int bagId) {
            this.bagId = bagId;
        }

        public void prepare() {
            add("@Override");
            add("public int getSchemaTupleBagIdentifier() {");
            add("    return " + bagId + ";");
            add("}");
        }
    }

    static class GetIterator extends TypeInFunctionStringOut {
        int schemaTupleId;
        int total;
        List<String> declarations = Lists.newArrayList();

        public GetIterator(int schemaTupleId) {
            this.schemaTupleId = schemaTupleId;
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            total++;
            String name = proper(fs.type) + "Iterator";
            declarations.add("    " + name + " iterator_" + fieldNum + " = pos_ " + fieldNum + ".iterator();");
        }

        public void end() {
            add("@Override");
            add("public Iterator<Tuple> iterator() {");
            addBreak();
            for (String declaration : declarations) {
            add(declaration);
            }
            addBreak();
            add("    return new Iterator<Tuple>() {");
            add("        @Override");
            add("        public boolean hasNext() {");
            add("            boolean retVal = iterator_0.hasNext()");
            add("            if (!retVal) {");
            for (int i = 0; i < total; i++) {
            add("                iterator_" + i + ".finish();");
            }
            add("            }");
            add("            return retVal;");
            add("        }");
            addBreak();
            add("        @Override");
            add("        public Tuple next() {");
            add("            SchemaTuple_" + schemaTupleId + " t = new SchemaTuple_" + schemaTupleId + "();");
            for (int i = 0; i < total; i++) {
            add("            t.setPos_" + i + "(iterator_" + i + ".next());");
            }
            add("            return t;");
            add("        }");
            add("    };");
            add("}");
        }
    }

    static class GeneratedCodeAddGeneral extends TypeInFunctionStringOut {

    }

    static class GeneratedCodeAdd extends TypeInFunctionStringOut {

    }

    static class GeneratedCodeAddSpecific extends TypeInFunctionStringOut {

    }

    static class GetColumns extends TypeInFunctionStringOut {

    }

    static class SetColumns extends TypeInFunctionStringOut {

    }

}
