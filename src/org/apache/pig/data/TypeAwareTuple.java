package org.apache.pig.data;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.pig.backend.executionengine.ExecException;

public interface TypeAwareTuple extends Tuple {

    public void setInt(int idx, int val) throws ExecException;
    public void setFloat(int idx, float val) throws ExecException;
    public void setDouble(int idx, double val) throws ExecException;
    public void setLong(int idx, long val) throws ExecException;
    public void setString(int idx, String val) throws ExecException;
    public void setBoolean(int idx, boolean val) throws ExecException;
    public void setBigInteger(int idx, boolean val) throws ExecException;
    public void setBigDecimal(int idx, boolean val) throws ExecException;

    public Integer getInteger(int idx) throws ExecException;
    public Float getFloat(int idx) throws ExecException;
    public Double getDouble(int idx) throws ExecException;
    public Long getLong(int idx) throws ExecException;
    public String getString(int idx) throws ExecException;
    public Boolean getBoolean(int idx) throws ExecException;
    public BigInteger getBigInteger(int idx) throws ExecException;
    public BigDecimal getBigDecimal(int idx) throws ExecException;


}
