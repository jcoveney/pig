package org.apache.pig.builtin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;

public class StreamingUdfException extends ExecException {
    private Log log = LogFactory.getLog(StreamingUdfException.class);
    
    static final long serialVersionUID = 1;
    private String message;
    private String language;
    private Integer lineNumber;

    public StreamingUdfException() {
    }

    public StreamingUdfException(String message) {
        this.message = message;
    }

    public StreamingUdfException(String message, Integer lineNumber) {
        this.message = message;
        this.lineNumber = lineNumber;
    }

    public StreamingUdfException(String language, String message, Throwable cause) {
        this.language = language;
        this.message = message + "\n" + cause.getMessage() + "\n";
    }

    public StreamingUdfException(String language, String message) {
        this(language, message, (Integer) null);
    }

    public StreamingUdfException(String language, String message, Integer lineNumber) {
        this.language = language;
        this.message = message;
        this.lineNumber = lineNumber;
    }

    public String getLanguage() {
        return language;
    }

    public Integer getLineNumber() {
        return lineNumber;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
    
    @Override
    public String toString() {
        //Don't modify this without also modifying Launcher.getExceptionFromStrings!
        String s = getClass().getName();
        String message = getMessage();
        String lineNumber = this.getLineNumber() == null ? "" : "" + this.getLineNumber();
        return (message != null) ? (s + ": " + "LINE " + lineNumber + ": " + message) : s;

    }
}