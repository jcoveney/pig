package org.apache.pig.builtin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.streaming.HandlerFactory;
import org.apache.pig.impl.streaming.InputHandler;
import org.apache.pig.impl.streaming.OutputHandler;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

@SuppressWarnings("deprecation")
public class StreamingUDF extends EvalFunc<Object> {
    private static final Log log = LogFactory.getLog(StreamingUDF.class);

    private static final String PYTHON_CONTROLLER_JAR_PATH = "/python/streaming/controller.py"; //Relative to root of pig jar.
    private static final String PYTHON_PIG_UTIL_PATH = "/python/streaming/pig_util.py"; //Relative to root of pig jar.

    //Indexes for arguments being passed to external process
    private static final int UDF_LANGUAGE = 0;
    private static final int PATH_TO_CONTROLLER_FILE = 1;
    private static final int UDF_FILE_NAME = 2; //Name of file where UDF function is defined
    private static final int UDF_FILE_PATH = 3; //Path to directory containing file where UDF function is defined
    private static final int UDF_NAME = 4; //Name of UDF function being called.
    private static final int PATH_TO_FILE_CACHE = 5; //Directory where required files (like pig_util) are cached on cluster nodes.
    private static final int STD_OUT_OUTPUT_PATH = 6; //File for output from when user writes to standard output.
    private static final int STD_ERR_OUTPUT_PATH = 7; //File for output from when user writes to standard error.
    private static final int CONTROLLER_LOG_FILE_PATH = 8; //Controller log file logs progress through the controller script not user code.
    private static final int IS_ILLUSTRATE = 9; //Controller captures output differently in illustrate vs running.

    //Illustrate will set the static flag telling udf to start capturing its output.  It's up to each
    //instance to react to it and set its own flag.
    private static boolean captureOutput = false;
    private boolean instancedCapturingOutput = false;
    
    private String language;
    private String filePath;
    private String funcName;
    private Schema schema;
    private boolean initialized = false;

    private Process process; // Handle to the external process
    private ProcessErrorThread stderrThread; // thread to get process stderr
    private ProcessInputThread stdinThread; // thread to send input to process
    private ProcessOutputThread stdoutThread; //thread to read output from process
    
    private InputHandler inputHandler;
    private OutputHandler outputHandler;

    private BlockingQueue<Tuple> inputQueue;
    private BlockingQueue<Object> outputQueue;

    private DataOutputStream stdin; // stdin of the process
    private InputStream stdout; // stdout of the process
    private InputStream stderr; // stderr of the process

    private static final Object ERROR_OUTPUT = new Object();
    private static final Object NULL_OBJECT = new Object(); //BlockingQueue can't have null.  Use place holder object instead.
    
    private static Map<String, String> outputFileNames = new HashMap<String, String>();
    private static String runId = UUID.randomUUID().toString(); //Unique ID for this run to ensure udf output files aren't corrupted from previous runs.
    
    private volatile StreamingUdfException outerrThreadsError;
    
    public static final String TURN_ON_OUTPUT_CAPTURING = "TURN_ON_OUTPUT_CAPTURING";

    public StreamingUDF(String language, String filePath, String funcName, String outputSchemaString, String schemaLineNumber) throws StreamingUdfOutputSchemaException, ExecException {
        this.language = language;
        this.filePath = filePath;
        this.funcName = funcName;
        try {
            this.schema = Utils.getSchemaFromString(outputSchemaString);
        } catch (ParserException pe) {
            throw new StreamingUdfOutputSchemaException(pe.getMessage(), Integer.valueOf(schemaLineNumber));
        }
    }

    public static void startCapturingOutput() {
        StreamingUDF.captureOutput = true;
    }
    
    @Override
    public Object exec(Tuple input) throws IOException {
        if (!initialized) {
            initialize();
            initialized = true;
        }
        return getOutput(input);
    }

    private void initialize() throws ExecException, IOException {
        inputQueue = new ArrayBlockingQueue<Tuple>(1);
        outputQueue = new ArrayBlockingQueue<Object>(2);
        StreamingCommand sc = startUdfController();
        createInputHandlers(sc);
        setStreams();
        startThreads();
    }

    private StreamingCommand startUdfController() throws IOException {
        StreamingCommand sc = new StreamingCommand(null, constructCommand());
        ProcessBuilder processBuilder = StreamingUtil.createProcess(sc);
        process = processBuilder.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new ProcessKiller(process) ) );
        return sc;
    }

    private String[] constructCommand() throws IOException {
        String[] command = new String[10];
        Configuration conf = UDFContext.getUDFContext().getJobConf();

        String jarPath = conf.get("mapred.jar");
        String jobDir;
        if (jarPath != null) {
        	jobDir = new File(jarPath).getParent();
        } else {
        	jobDir = "";
        }
        
        String userlogDir = System.getProperty("hadoop.log.dir") + "/userlogs";
        String jobId = conf.get("mapred.job.id");
        String taskId = conf.get("mapred.task.id");
        log.debug("JobId: " + jobId);
        log.debug("TaskId: " + taskId);
        
        String controllerLogFileName, outFileName, errOutFileName;
        Boolean isIllustrate;
        //Check if we're running on the cluster - this can be determined by the presence of userlogs.
        if ( (new File(userlogDir)).exists() ) {
        	//TODO: For some reason Hawk Hadoop writes logs to a directory without the job id.  There
        	//should be a smarter way to handle this.
        	String taskLogDir;
        	if ( (new File(userlogDir + "/" + jobId).exists()) ) {
        		taskLogDir = userlogDir + "/" + jobId + "/" + taskId;
        	} else {
        		taskLogDir = userlogDir + "/" + taskId;
        	}
        	
            controllerLogFileName = taskLogDir + "/" + funcName + "_python.log";
            outFileName = taskLogDir + "/" + funcName + ".out";
            errOutFileName = taskLogDir + "/" + funcName + ".err";
            isIllustrate = false;
        } else {
            controllerLogFileName = "/tmp/" + taskId + "_" + funcName + "_python.log";
            outFileName = "/tmp/cpython_" + this.funcName + "_" + runId + ".out";
            errOutFileName = "/tmp/cpython_" + this.funcName + "_" + runId + ".err";
            isIllustrate = true;
        }
        outputFileNames.put(this.funcName, outFileName); //Used by illustrate to find the latest output for this function.

        command[UDF_LANGUAGE] = language;
        command[PATH_TO_CONTROLLER_FILE] = getControllerPath(jobDir);
        int lastSeparator = filePath.lastIndexOf(File.separator) + 1;
        command[UDF_FILE_NAME] = filePath.substring(lastSeparator);
        String udfFilePath = "\"\"";
        if (lastSeparator > 1) {
            udfFilePath =  filePath.substring(0, lastSeparator - 1);
        }
        command[UDF_FILE_PATH] = udfFilePath;
        command[UDF_NAME] = funcName;
        command[PATH_TO_FILE_CACHE] = "\"" + jobDir + filePath.substring(0, lastSeparator) + "\"";
        command[STD_OUT_OUTPUT_PATH] = outFileName;
        command[STD_ERR_OUTPUT_PATH] = errOutFileName;
        command[CONTROLLER_LOG_FILE_PATH] = controllerLogFileName;
        command[IS_ILLUSTRATE] = isIllustrate.toString();
        return command;
    }

    private void createInputHandlers(StreamingCommand sc) throws ExecException {
        this.inputHandler = HandlerFactory.createInputHandler(sc);
        this.outputHandler = HandlerFactory.createOutputHandler(sc);
    }

    private void setStreams() throws IOException { 
        stdout = new DataInputStream(new BufferedInputStream(process
                .getInputStream()));
        outputHandler.bindTo("", new BufferedPositionedInputStream(stdout),
                0, Long.MAX_VALUE);
        
        stdin = new DataOutputStream(new BufferedOutputStream(process
                .getOutputStream()));
        inputHandler.bindTo(stdin); 
        
        stderr = new DataInputStream(new BufferedInputStream(process
                .getErrorStream()));
    }

    private void startThreads() {
        stdinThread = new ProcessInputThread();
        stdinThread.start();
        
        stdoutThread = new ProcessOutputThread();
        stdoutThread.start();
        
        stderrThread = new ProcessErrorThread();
        stderrThread.start();
    }

    /**
     * Find the path to the controller file for the streaming language.
     *
     * First check path to job jar and if the file is not found (like in the
     * case of running hadoop in standalone mode) write the necessary files
     * to temporary files and return that path.
     *
     * @param language
     * @param jarPath
     * @return
     * @throws IOException
     */
    private String getControllerPath(String jarPath) throws IOException {
        if (language.toLowerCase().equals("python")) {
            String controllerPath = jarPath + PYTHON_CONTROLLER_JAR_PATH;
            File controller = new File(controllerPath);
            if (!controller.exists()) {
                File controllerFile = File.createTempFile("controller", ".py");
                writeFile(Launcher.class.getResourceAsStream(PYTHON_CONTROLLER_JAR_PATH), controllerFile);
                controllerFile.deleteOnExit();
                File pigUtilFile = new File(controllerFile.getParent() + "/pig_util.py");
                pigUtilFile.deleteOnExit();
                writeFile(Launcher.class.getResourceAsStream(PYTHON_PIG_UTIL_PATH), pigUtilFile);
                controllerPath = controllerFile.getAbsolutePath();
            }
            return controllerPath;
        } else {
            throw new ExecException("Invalid language: " + language);
        }
    }

    /**
     * Returns a list of file names (relative to root of pig jar) of files that need to be
     * included in the jar shipped to the cluster.
     *
     * Will need to be smarter as more languages are added and the controller files are large.
     *
     * @return
     */
    public static List<String> getResourcesForJar() {
        List<String> files = new ArrayList<String>();
        files.add(PYTHON_CONTROLLER_JAR_PATH);
        files.add(PYTHON_PIG_UTIL_PATH);
        return files;
    }

    private Object getOutput(Tuple input) throws ExecException {
        if (outputQueue == null) {
            throw new ExecException("Process has already been shut down.  No way to retrieve output for input: " + input);
        }

        if (captureOutput && !instancedCapturingOutput) {
            Tuple t = DefaultTupleFactory.getInstance().newTuple(TURN_ON_OUTPUT_CAPTURING);
            try {
                inputQueue.put(t);
            } catch (InterruptedException e) {
                throw new ExecException("Failed adding capture input flag to inputQueue");
            }
            instancedCapturingOutput = true;
        }

        try {
            if (this.getInputSchema().size() == 0) {
                //When nothing is passed into the UDF the tuple 
                //being sent is the full tuple for the relation.
                //We want it to be nothing (since that's what the user wrote).
                input = TupleFactory.getInstance().newTuple(0);
            }
            inputQueue.put(input);
        } catch (Exception e) {
            throw new ExecException("Failed adding input to inputQueue", e);
        }
        Object o = null;
        try {
            if (outputQueue != null) {
                o = outputQueue.take();
                if (o == NULL_OBJECT) {
                    o = null;
                }
            }
        } catch (Exception e) {
            throw new ExecException("Problem getting output", e);
        }

        if (o == ERROR_OUTPUT) {
            outputQueue = null;
            if (outerrThreadsError == null) {
                outerrThreadsError = new StreamingUdfException(this.language, "Problem with streaming udf.  Can't recreate exception.");
            }
            throw outerrThreadsError;
        }

        return o;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return this.schema;
    }

    public static Map<String, String> getUdfOutput() throws IOException {
        Map<String, String> udfFuncNameToOutput = new HashMap<String,String>();
        for (Map.Entry<String, String> funcToOutputFileName : outputFileNames.entrySet()) {
            String udfOutput = "";
            File udfLogFile = new File(funcToOutputFileName.getValue());
            FileReader fr = new FileReader(udfLogFile);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            while (line != null) {
                udfOutput += "\t" + line + "\n";
                line = br.readLine();
            }
            udfFuncNameToOutput.put(funcToOutputFileName.getKey(), udfOutput);
        }
        return udfFuncNameToOutput;
    }

    private void writeFile(InputStream in, File f) throws IOException {
        FileOutputStream out = new FileOutputStream(f, false);
        byte buf[]=new byte[1024];
        int len;
        while ((len=in.read(buf)) > 0)
            out.write(buf,0,len);
        out.close();
        in.close();
    }

    /**
     * The thread which consumes input and feeds it to the the Process
     */
    class ProcessInputThread extends Thread {
        ProcessInputThread() {
            setDaemon(true);
        }

        public void run() {
            try {
                log.debug("Starting PIT");
                while (true) {
                    Tuple inputTuple = inputQueue.take();
                    inputHandler.putNext(inputTuple, true, true);
                    try {
                        stdin.flush();
                    } catch(Exception e) {
                        return;
                    }
                }
            } catch (Exception e) {
                log.error(e);
            }
        }
    }
    
    private static final int WAIT_FOR_ERROR_LENGTH = 500;
    private static final int MAX_WAIT_FOR_ERROR_ATTEMPTS = 5;
    
    /**
     * The thread which consumes output from process
     */
    class ProcessOutputThread extends Thread {
        ProcessOutputThread() {
            setDaemon(true);
        }

        public void run() {
            Object o = null;
            try{
                log.debug("Starting POT");
                o = outputHandler.getNext(schema.getField(0));
                while (o != OutputHandler.END_OF_OUTPUT) {
                    if (o != null)
                        outputQueue.put(o);
                    else
                        outputQueue.put(NULL_OBJECT);
                    o = outputHandler.getNext(schema.getField(0));
                }
            } catch(Exception e) {
                if (outputQueue != null) {
                    try {
                        //Give error thread a chance to check the standard error output
                        //for an exception message.
                        int attempt = 0;
                        while (stderrThread.isAlive() && attempt < MAX_WAIT_FOR_ERROR_ATTEMPTS) {
                            Thread.sleep(WAIT_FOR_ERROR_LENGTH);
                            attempt++;
                        }
                        //Only write this if no other error.  Don't want to overwrite
                        //an error from the error thread.
                        if (outerrThreadsError == null) {
                            outerrThreadsError = new StreamingUdfException(language, "Error deserializing output.  Please check that the declared outputSchema for function " +
                                                                        funcName + " matches the data type being returned.", e);
                        }
                        outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
                    } catch(InterruptedException ie) {
                        log.error(ie);
                    }
                }
            }
        }
    }

    class ProcessErrorThread extends Thread {
        public ProcessErrorThread() {
            setDaemon(true);
        }

        public void run() {
            try {
                log.debug("Starting PET");
                Integer lineNumber = null;
                String error = "";
                String errInput;
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stderr));
                while ((errInput = reader.readLine()) != null) {
                    //First line of error stream is usually the line number of error.
                    //If its not a number just treat it as first line of error message.
                    if (lineNumber == null) {
                        try {
                            lineNumber = Integer.valueOf(errInput);
                        } catch (NumberFormatException nfe) {
                            error += errInput + "\n";
                        }
                    } else {
                        error += errInput + "\n";
                    }
                }
                outerrThreadsError = new StreamingUdfException(language, error, lineNumber);
                if (outputQueue != null) {
                    outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
                }
                if (stderr != null) {
                    stderr.close();
                    stderr = null;
                }
            } catch (IOException e) {
                log.debug("Process Ended");
            } catch (Exception e) {
                log.error("standard error problem", e);
            }
        }
    }
    
    public class ProcessKiller implements Runnable {
        private Process process;
        public ProcessKiller(Process process) {
            this.process = process;
        }
        public void run() {
            process.destroy();
        }
    }
}
