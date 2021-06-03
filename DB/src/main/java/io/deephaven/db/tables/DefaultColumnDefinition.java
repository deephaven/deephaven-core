/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import java.io.*;
import io.deephaven.base.CompareUtils;
import io.deephaven.dataobjects.*;
import io.deephaven.dataobjects.persistence.*;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.base.log.*;
import io.deephaven.base.*;

public class DefaultColumnDefinition implements Externalizable, DataObjectStreamConstants, LogOutputAppendable, Copyable<DefaultColumnDefinition> {

    private static final long serialVersionUID = 5684394895950293942L;

    public static final int COLUMNTYPE_NORMAL=1;
    public static final int COLUMNTYPE_GROUPING=2;
    public static final int COLUMNTYPE_PARTITIONING=4;
    public static final int COLUMNTYPE_VIRTUAL=8;

    public static final int ENCODING_ISO_8859_1=1;
    public static final int ENCODING_UTF_8=2;
    public static final int ENCODING_US_ASCII=4;
    public static final int ENCODING_UTF_16=8;
    public static final int ENCODING_UTF_16BE=16;
    public static final int ENCODING_UTF_16LE=32;

    public DefaultColumnDefinition() {
        super();
    }

    public DefaultColumnDefinition(DefaultColumnDefinition source) {
        copyValues(source);
    }

    public DefaultColumnDefinition(String name){
        super();

        this.name=name;
    }

    public static final String columnSetName_ = "ColumnDefinition";
    public static String getColumnSetName() { return columnSetName_; }
    public static DataObjectColumnSet getColumnSetStatic() {
        return DataObjectColumnSetManager.getInstance().getColumnSet(columnSetName_);
    }
    public DataObjectColumnSet columnSet_=null;
    public DataObjectColumnSet getColumnSet() {
        if (columnSet_==null){
            columnSet_=getColumnSetStatic();
        }

        return columnSet_;
    }

    protected String name;
    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name=name;
    }

    protected Class dataType;
    public Class getDataType() {
        return dataType;
    }

    void setDataType(Class dataType) {
        this.dataType=dataType;
    }

    protected Class componentType;
    public Class getComponentType() {
        return componentType;
    }

    void setComponentType(Class componentType) {
        this.componentType=componentType;
    }

    protected int columnType=Integer.MIN_VALUE;
    public int getColumnType() {
        return columnType;
    }

    void setColumnType(int columnType) {
        this.columnType=columnType;
    }

    protected Boolean isVarSizeString;
    public Boolean getIsVarSizeString() {
        return isVarSizeString;
    }

    void setIsVarSizeString(Boolean isVarSizeString) {
        this.isVarSizeString=isVarSizeString;
    }

    protected int encoding=Integer.MIN_VALUE;
    public int getEncoding() {
        return encoding;
    }

    void setEncoding(int encoding) {
        this.encoding=encoding;
    }

    protected String objectCodecClass;
    public String getObjectCodecClass() {
        return objectCodecClass;
    }

    void setObjectCodecClass(String objectCodecClass) {
        this.objectCodecClass=objectCodecClass;
    }

    protected String objectCodecArguments;
    public String getObjectCodecArguments() {
        return objectCodecArguments;
    }

    void setObjectCodecArguments(String objectCodecArguments) {
        this.objectCodecArguments=objectCodecArguments;
    }

    protected int objectWidth=Integer.MIN_VALUE;
    public int getObjectWidth() {
        return objectWidth;
    }

    void setObjectWidth(int objectWidth) {
        this.objectWidth=objectWidth;
    }

    @Override
    public void copyValues(DefaultColumnDefinition x) {
        name = x.name;
        dataType = x.dataType;
        componentType = x.componentType;
        columnType = x.columnType;
        isVarSizeString = x.isVarSizeString;
        encoding = x.encoding;
        objectCodecClass = x.objectCodecClass;
        objectCodecArguments = x.objectCodecArguments;
        objectWidth = x.objectWidth;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ColumnDefinition : ");

        builder.append("name=").append(name);
        builder.append("|dataType=").append(dataType);
        builder.append("|componentType=").append(componentType);
        builder.append("|columnType=").append(columnType);
        builder.append("|isVarSizeString=").append(isVarSizeString);
        builder.append("|encoding=").append(encoding);
        builder.append("|objectCodecClass=").append(objectCodecClass);
        builder.append("|objectCodecArguments=").append(objectCodecArguments);
        builder.append("|objectWidth=").append(objectWidth);

        return builder.toString();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        logOutput.append("ColumnDefinition : ");

        logOutput.append("name=").append(String.valueOf(name));
        logOutput.append("|dataType=").append(String.valueOf(dataType));
        logOutput.append("|componentType=").append(String.valueOf(componentType));
        logOutput.append("|columnType=").append(columnType);
        logOutput.append("|isVarSizeString=").append(isVarSizeString);
        logOutput.append("|encoding=").append(encoding);
        logOutput.append("|objectCodecClass=").append(String.valueOf(objectCodecClass));
        logOutput.append("|objectCodecArguments=").append(String.valueOf(objectCodecArguments));
        logOutput.append("|objectWidth=").append(objectWidth);

        return logOutput;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultColumnDefinition) {
            DefaultColumnDefinition castObj = (DefaultColumnDefinition) obj;
            return CompareUtils.equals(name, castObj.name);
        }
        return false;
    }

    public int hashCode() {
        return HashCodeUtil.toHashCode(name);
    }

    @Override
    public DefaultColumnDefinition clone() {
        return new DefaultColumnDefinition(this);
    }

    @Override
    public DefaultColumnDefinition safeClone() {
        return clone();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF(); name = "\0".equals(name) ? null : name;
        dataType = (Class)in.readObject();
        componentType = (Class)in.readObject();
        columnType = in.readInt();
        isVarSizeString = (Boolean)in.readObject();
        encoding = in.readInt();
        objectCodecClass = in.readUTF(); objectCodecClass = "\0".equals(objectCodecClass) ? null : objectCodecClass;
        objectCodecArguments = in.readUTF(); objectCodecArguments = "\0".equals(objectCodecArguments) ? null : objectCodecArguments;
        objectWidth = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (name == null) { out.writeUTF("\0"); } else { out.writeUTF(name); }
        out.writeObject(dataType);
        out.writeObject(componentType);
        out.writeInt(columnType);
        out.writeObject(isVarSizeString);
        out.writeInt(encoding);
        if (objectCodecClass == null) { out.writeUTF("\0"); } else { out.writeUTF(objectCodecClass); }
        if (objectCodecArguments == null) { out.writeUTF("\0"); } else { out.writeUTF(objectCodecArguments); }
        out.writeInt(objectWidth);
    }
}
