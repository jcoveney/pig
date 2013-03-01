package org.apache.pig.impl.streaming;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.io.FileLocalizer;

public class StreamingUtil {
    private static final String BASH = "bash";
    private static final String PATH = "PATH";
    
    /**
     * Create an external process for StreamingCommand command.
     * 
     * @param command
     * @return
     */
    public static ProcessBuilder createProcess(StreamingCommand command) {
        String[] argv = command.getCommandArgs();
        String argvAsString = "";
        for (String arg : argv) {
            argvAsString += arg;
            argvAsString += " ";
        }
        
        // Set the actual command to run with 'bash -c exec ...'
        List<String> cmdArgs = new ArrayList<String>();
        cmdArgs.add(BASH);
        cmdArgs.add("-c");
        StringBuffer sb = new StringBuffer();
        sb.append("exec ");
        sb.append(argvAsString);
        cmdArgs.add(sb.toString());

        // Start the external process
        ProcessBuilder processBuilder = new ProcessBuilder(cmdArgs
                .toArray(new String[cmdArgs.size()]));
        setupEnvironment(processBuilder);
        return processBuilder;
    }
    
    /**
     * Set up the run-time environment of the managed process.
     * 
     * @param pb
     *            {@link ProcessBuilder} used to exec the process
     */
    private static void setupEnvironment(ProcessBuilder pb) {
        String separator = ":";
        Map<String, String> env = pb.environment();

        // Add the current-working-directory to the $PATH
        File dir = pb.directory();
        String cwd = (dir != null) ? dir.getAbsolutePath() : System
                .getProperty("user.dir");

        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
            String unixCwd = FileLocalizer.parseCygPath(cwd, FileLocalizer.STYLE_UNIX);
            if (unixCwd == null)
                throw new RuntimeException(
                        "Can not convert Windows path to Unix path under cygwin");
            cwd = unixCwd;
        }

        String envPath = env.get(PATH);
        if (envPath == null) {
            envPath = cwd;
        } else {
            envPath = envPath + separator + cwd;
        }
        env.put(PATH, envPath);
    }
}
