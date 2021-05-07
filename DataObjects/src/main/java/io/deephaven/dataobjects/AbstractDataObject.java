/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import java.util.Date;
import java.io.Externalizable;

public interface AbstractDataObject extends Externalizable {

    public DataObjectColumnSet getColumnSet();

    public String getString(int index);
    public int getInt(int index);
    public double getDouble(int index);
    public long getLong(int index);
    public byte getByte(int index);
    public Date getDate(int index);

    public String getString(String name);
    public int getInt(String name);
    public double getDouble(String name);
    public long getLong(String name);
    public byte getByte(String name);
    public Date getDate(String name);

    public String getString(int index[]);
    public int getInt(int index[]);
    public double getDouble(int index[]);
    public long getLong(int index[]);
    public byte getByte(int index[]);
    public Date getDate(int index[]);

    public void setString(int index, String data);
    public void setInt(int index, int data);
    public void setDouble(int index, double data);
    public void setLong(int index, long data);
    public void setByte(int index, byte data);
    public void setDate(int index, Date data);

    public void setString(String name, String data);
    public void setInt(String name, int data);
    public void setDouble(String name, double data);
    public void setLong(String name, long data);
    public void setByte(String name, byte data);
    public void setDate(String name, Date data);

    public Object getValue(int index);
    public Object getValue(int index[]);
    public void setValue(int index, Object data);

    public Object getValue(String name);
    public void setValue(String name, Object data);

    public Object clone();
}
