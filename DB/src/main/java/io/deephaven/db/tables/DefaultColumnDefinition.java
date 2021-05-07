/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import java.util.Date;
import java.io.*;
import io.deephaven.base.CompareUtils;
import io.deephaven.dataobjects.*;
import io.deephaven.dataobjects.persistence.*;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.base.log.*;
import io.deephaven.base.*;

public class DefaultColumnDefinition implements AbstractDataObject, Externalizable, DataObjectStreamConstants, LogOutputAppendable, Copyable<DefaultColumnDefinition> {

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

    public static final int INDEX_OF_NAME = 0;
    protected String name;
    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name=name;
    }

    public static final int INDEX_OF_DATATYPE = 1;
    protected Class dataType;
    public Class getDataType() {
        return dataType;
    }

    void setDataType(Class dataType) {
        this.dataType=dataType;
    }

    public static final int INDEX_OF_COMPONENTTYPE = 2;
    protected Class componentType;
    public Class getComponentType() {
        return componentType;
    }

    void setComponentType(Class componentType) {
        this.componentType=componentType;
    }

    public static final int INDEX_OF_COLUMNTYPE = 3;
    protected int columnType=Integer.MIN_VALUE;
    public int getColumnType() {
        return columnType;
    }

    void setColumnType(int columnType) {
        this.columnType=columnType;
    }

    public static final int INDEX_OF_ISVARSIZESTRING = 4;
    protected Boolean isVarSizeString;
    public Boolean getIsVarSizeString() {
        return isVarSizeString;
    }

    void setIsVarSizeString(Boolean isVarSizeString) {
        this.isVarSizeString=isVarSizeString;
    }

    public static final int INDEX_OF_ENCODING = 5;
    protected int encoding=Integer.MIN_VALUE;
    public int getEncoding() {
        return encoding;
    }

    void setEncoding(int encoding) {
        this.encoding=encoding;
    }

    public static final int INDEX_OF_OBJECTCODECCLASS = 6;
    protected String objectCodecClass;
    public String getObjectCodecClass() {
        return objectCodecClass;
    }

    void setObjectCodecClass(String objectCodecClass) {
        this.objectCodecClass=objectCodecClass;
    }

    public static final int INDEX_OF_OBJECTCODECARGUMENTS = 7;
    protected String objectCodecArguments;
    public String getObjectCodecArguments() {
        return objectCodecArguments;
    }

    void setObjectCodecArguments(String objectCodecArguments) {
        this.objectCodecArguments=objectCodecArguments;
    }

    public static final int INDEX_OF_OBJECTWIDTH = 8;
    protected int objectWidth=Integer.MIN_VALUE;
    public int getObjectWidth() {
        return objectWidth;
    }

    void setObjectWidth(int objectWidth) {
        this.objectWidth=objectWidth;
    }

    public String getString(int index) {
        switch (index){
            case 0: return name;
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 6: return objectCodecClass;
            case 7: return objectCodecArguments;
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public int getInt(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 3: return columnType;
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 5: return encoding;
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 8: return objectWidth;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public double getDouble(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public long getLong(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public byte getByte(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public Date getDate(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public String getString(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return null;

        return ado.getString(index[index.length-1]);
    }

    public int getInt(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return Integer.MIN_VALUE;

        return ado.getInt(index[index.length-1]);
    }

    public double getDouble(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return Double.NaN;

        return ado.getDouble(index[index.length-1]);
    }

    public long getLong(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return Long.MIN_VALUE;

        return ado.getLong(index[index.length-1]);
    }

    public byte getByte(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return Byte.MIN_VALUE;

        return ado.getByte(index[index.length-1]);
    }

    public Date getDate(int[] index) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return null;

        return ado.getDate(index[index.length-1]);
    }

    public String getString(String name) {
        return getString(getColumnSet().getColumnIndex(name));
    }

    public int getInt(String name) {
        return getInt(getColumnSet().getColumnIndex(name));
    }

    public double getDouble(String name) {
        return getDouble(getColumnSet().getColumnIndex(name));
    }

    public long getLong(String name) {
        return getLong(getColumnSet().getColumnIndex(name));
    }

    public byte getByte(String name) {
        return getByte(getColumnSet().getColumnIndex(name));
    }

    public Date getDate(String name) {
        return getDate(getColumnSet().getColumnIndex(name));
    }

    public void setString(int index, String data) {
        switch (index){
            case 0: this.name=data;  break;
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 6: this.objectCodecClass=data;  break;
            case 7: this.objectCodecArguments=data;  break;
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setInt(int index, int data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 3: this.columnType=data;  break;
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 5: this.encoding=data;  break;
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 8: this.objectWidth=data;  break;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setDouble(int index, double data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setLong(int index, long data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setByte(int index, byte data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setDate(int index, Date data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 4: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 5: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 6: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 7: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 8: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setString(String name, String value) {
        setString(getColumnSet().getColumnIndex(name), value);
    }

    public void setInt(String name, int value) {
        setInt(getColumnSet().getColumnIndex(name), value);
    }

    public void setDouble(String name, double value) {
        setDouble(getColumnSet().getColumnIndex(name), value);
    }

    public void setLong(String name, long value) {
        setLong(getColumnSet().getColumnIndex(name), value);
    }

    public void setByte(String name, byte value) {
        setByte(getColumnSet().getColumnIndex(name), value);
    }

    public void setDate(String name, Date value) {
        setDate(getColumnSet().getColumnIndex(name), value);
    }

    public Object getValue(int index) {
        switch (index){
            case 0: return name;
            case 1: return dataType;
            case 2: return componentType;
            case 3: return (columnType==Integer.MIN_VALUE) ? null : columnType;
            case 4: return isVarSizeString;
            case 5: return (encoding==Integer.MIN_VALUE) ? null : encoding;
            case 6: return objectCodecClass;
            case 7: return objectCodecArguments;
            case 8: return (objectWidth==Integer.MIN_VALUE) ? null : objectWidth;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public Object getValue(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        if (ado==null) return null;

        return ado.getValue(index[index.length-1]);
    }

    public Object getValue(String name) {
        return getValue(getColumnSet().getColumnIndex(name));
    }

    public void setValue(int index, Object data) {
        switch (index){
            case 0: this.name=(String)data;  break;
            case 1: this.dataType=(Class)data;  break;
            case 2: this.componentType=(Class)data;  break;
            case 3: this.columnType= (data==null) ? Integer.MIN_VALUE : (Integer)data;  break;
            case 4: this.isVarSizeString=(Boolean)data;  break;
            case 5: this.encoding= (data==null) ? Integer.MIN_VALUE : (Integer)data;  break;
            case 6: this.objectCodecClass=(String)data;  break;
            case 7: this.objectCodecArguments=(String)data;  break;
            case 8: this.objectWidth= (data==null) ? Integer.MIN_VALUE : (Integer)data;  break;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setValue(String name, Object value) {
        setValue(getColumnSet().getColumnIndex(name), value);
    }

    private AbstractDataObject getRefObject(int index[]){   //assumes the last one is a field....
        AbstractDataObject ado=this;

        for (int i=0; i<index.length-1; i++){
            if (ado==null){
                return null;
            }

            ado=(AbstractDataObject)ado.getValue(index[i]);
        }

        return ado;
    }

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
            return
                CompareUtils.equals(name, castObj.name);
        } else if (obj instanceof AbstractDataObjectImpl) {
            AbstractDataObjectImpl castObj = (AbstractDataObjectImpl) obj;
            return
                CompareUtils.equals(name, castObj.getString("Name"));
        }
        return false;
    }

    public int hashCode() {
        return
            HashCodeUtil.toHashCode(name);
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
        ColumnsetConversionSchema conversionSchema = null;

        if (in instanceof PersistentInputStream) {
            conversionSchema = ((PersistentInputStream)in).getConversionSchema(getColumnSet().getName());
        }
        else if (in instanceof DataObjectInputStream.WrappedObjectInputStream) {
            DataObjectInputStream childStream = ((DataObjectInputStream.WrappedObjectInputStream)in).getWObjectInputStream();

            if (childStream instanceof PersistentInputStream) {
                conversionSchema = ((PersistentInputStream)childStream).getConversionSchema(getColumnSet().getName());
            }
        }

        if (conversionSchema != null) {
            conversionSchema.readExternalADO(in, this);
       } else {
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
