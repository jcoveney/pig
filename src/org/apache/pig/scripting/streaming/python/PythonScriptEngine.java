package org.apache.pig.scripting.streaming.python;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.StreamingUdfException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PythonScriptEngine extends ScriptEngine {
    
    private static final Log log = LogFactory.getLog(PythonScriptEngine.class);

    private static Joiner NEWLINE_JOINER = Joiner.on("\n");
    
    @Override
    public void registerFunctions(String path, String namespace,
            PigContext pigContext) throws IOException {
        
        String fileName = path.substring(0, path.length() - ".py".length());
        log.debug("Path: " + path + " FileName: " + fileName + " Namespace: " + namespace);
        File f = new File(path);

        if (!f.canRead()) {
            throw new IOException("Can't read file: " + path);
        }
        
        FileInputStream fin = new FileInputStream(f);
        List<String[]> functions = getFunctions(fin);
        for(String[] functionInfo : functions) {
            String name = functionInfo[0];
            String schemaString = functionInfo[1];
            String schemaLineNumber = functionInfo[2];
            String alias = (namespace == null ? "" : namespace + NAMESPACE_SEPARATOR) + name;
            log.debug("Registering Function: " + alias);
            pigContext.registerFunction(alias, new FuncSpec("StreamingUDF", new String[] {"python", fileName, name, schemaString, schemaLineNumber}));
        }
        fin.close();
    }
        
    public static void validateScript(String scriptPath) throws IOException {
        // TODO: copy validate_python.py into place so it doesn't need to be in current dir
        Process p = Runtime.getRuntime().exec("python validate_python.py " + scriptPath);
        BufferedReader stdError = new BufferedReader(new 
                InputStreamReader(p.getErrorStream()));
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        
        List<String> lines = Lists.newArrayList();
        String currentLine = null;
        while ((currentLine = stdError.readLine()) != null) {
            lines.add(currentLine);
        }
        
        if (! lines.isEmpty()) {
            Integer lineNumber;
            String errorMessage;
            try {
                lineNumber = new Integer(lines.get(0));
                errorMessage = NEWLINE_JOINER.join(lines.subList(1, lines.size()));
            } catch (NumberFormatException nfe) {
                lineNumber = null;
                errorMessage = NEWLINE_JOINER.join(lines);
            }
            
            throw new StreamingUdfException(errorMessage, lineNumber);
        }
    }

    @Override
    protected Map<String, List<PigStats>> main(PigContext context,
            String scriptFile) throws IOException {
        log.warn("ScriptFile: " + scriptFile);
        registerFunctions(scriptFile, null, context);
        return getPigStatsMap();
    }

    @Override
    protected String getScriptingLang() {
        return "streaming_python";
    }

    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        throw new IOException("Unsupported Operation");
    }
    
    private static final Pattern pSchema = Pattern.compile("^\\s*\\W+outputSchema.*");
    private static final Pattern pDef = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");

    private static List<String[]> getFunctions(InputStream is) throws IOException {
        List<String[]> functions = new ArrayList<String[]>();
        InputStreamReader in = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(in);
        String line = br.readLine();
        String schemaString = null;
        String schemaLineNumber = null;
        int lineNumber = 1;
        while (line != null) {
            if (pSchema.matcher(line).matches()) {
                int start = line.indexOf("(") + 2; //account for brackets
                int end = line.lastIndexOf(")") - 1;
                schemaString = line.substring(start,end).trim();
                schemaLineNumber = "" + lineNumber;
            } else if (pDef.matcher(line).matches()) {
                int start = line.indexOf("def ") + "def ".length();
                int end = line.indexOf('(');
                String functionName = line.substring(start, end).trim();
                if (schemaString != null) {
                    String[] funcInfo = {functionName, schemaString, "" + schemaLineNumber};
                    functions.add(funcInfo);
                    schemaString = null;
                }
            }
            line = br.readLine();
            lineNumber++;
        }
        return functions;
    }
}
