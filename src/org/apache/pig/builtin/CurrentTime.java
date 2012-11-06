package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.DateTime;

public class CurrentTime extends EvalFunc<DateTime> {
    private DateTime dateTime;
    private boolean isInitialized = false;

    /**
     * This is a default constructor for Pig reflection purposes. It should
     * never actually be used.
     */
    public CurrentTime() {}

    @Override
    public DateTime exec(Tuple input) throws IOException {
        if (!isInitialized) {
            String dateTimeValue = UDFContext.getUDFContext().getJobConf().get("pig.job.submitted.timestamp");
            if (dateTimeValue == null) {
                throw new ExecException("pig.job.submitted.timestamp was not set!");
            }
            dateTime = new DateTime(Long.parseLong(dateTimeValue));
            isInitialized  = true;
        }
        return dateTime;
    }
}