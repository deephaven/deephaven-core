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

public class DefaultTableDefinition implements AbstractDataObject, Externalizable, DataObjectStreamConstants, LogOutputAppendable, Copyable<DefaultTableDefinition> {

    private static final long serialVersionUID = 5684394895950293942L;

    

    public static final int STORAGETYPE_INMEMORY=1;
    public static final int STORAGETYPE_NESTEDPARTITIONEDONDISK=2;
    public static final int STORAGETYPE_SPLAYEDONDISK=4;

    public DefaultTableDefinition() {
        super();
    }

    public DefaultTableDefinition(DefaultTableDefinition source) {
        copyValues(source);
    }

    public DefaultTableDefinition(String namespace, String name){
        super();

        this.namespace=namespace;
        this.name=name;
    }

    public static final String columnSetName_ = "TableDefinition";
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

    public static final int INDEX_OF_NAMESPACE = 0;
    protected String namespace;
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace=namespace;
    }

    public static final int INDEX_OF_NAME = 1;
    protected String name;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name=name;
    }

    public static final int INDEX_OF_COLUMNS = 2;
    protected io.deephaven.db.tables.ColumnDefinition[] columns;
    public io.deephaven.db.tables.ColumnDefinition[] getColumns() {
        return columns;
    }

    public void setColumns(io.deephaven.db.tables.ColumnDefinition[] columns) {
        this.columns=columns;
    }

    public static final int INDEX_OF_STORAGETYPE = 3;
    protected int storageType=Integer.MIN_VALUE;
    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType=storageType;
    }

    public String getString(int index) {
        switch (index){
            case 0: return namespace;
            case 1: return name;
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public int getInt(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 3: return storageType;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public double getDouble(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public long getLong(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public byte getByte(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public Date getDate(int index) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
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
            case 0: this.namespace=data;  break;
            case 1: this.name=data;  break;
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=String");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setInt(int index, int data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=int");
            case 3: this.storageType=data;  break;
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setDouble(int index, double data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=double");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setLong(int index, long data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=long");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setByte(int index, byte data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=byte");
            default : throw new IllegalArgumentException("index=" + index + " is not in the columnset");
        }
    }

    public void setDate(int index, Date data) {
        switch (index){
            case 0: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 1: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 2: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
            case 3: throw new IllegalArgumentException("index=" + index + " is not of type=Date");
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
            case 0: return namespace;
            case 1: return name;
            case 2: return columns;
            case 3: return (storageType==Integer.MIN_VALUE) ? null : storageType;
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
            case 0: this.namespace=(String)data;  break;
            case 1: this.name=(String)data;  break;
            case 2: this.columns=(io.deephaven.db.tables.ColumnDefinition[])data;  break;
            case 3: this.storageType= (data==null) ? Integer.MIN_VALUE : (Integer)data;  break;
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

    public void copyValues(DefaultTableDefinition x) {
        namespace = x.namespace;
        name = x.name;
        columns = x.columns;
        storageType = x.storageType;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TableDefinition : ");

        builder.append("namespace=").append(namespace);
        builder.append("|name=").append(name);
        builder.append("|storageType=").append(storageType);

        return builder.toString();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        logOutput.append("TableDefinition : ");

        logOutput.append("namespace=").append(String.valueOf(namespace));
        logOutput.append("|name=").append(String.valueOf(name));
        logOutput.append("|storageType=").append(storageType);

        return logOutput;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultTableDefinition) {
            DefaultTableDefinition castObj = (DefaultTableDefinition) obj;
            return
                CompareUtils.equals(namespace, castObj.namespace) &&
                CompareUtils.equals(name, castObj.name);
        } else if (obj instanceof AbstractDataObjectImpl) {
            AbstractDataObjectImpl castObj = (AbstractDataObjectImpl) obj;
            return
                CompareUtils.equals(namespace, castObj.getString("Namespace")) &&
                CompareUtils.equals(name, castObj.getString("Name"));
        }
        return false;
    }

    public int hashCode() {
        return
            HashCodeUtil.toHashCode(namespace) ^
            HashCodeUtil.toHashCode(name);
    }

    @Override
    public DefaultTableDefinition clone() {
        return new DefaultTableDefinition(this);
    }

    @Override
    public DefaultTableDefinition safeClone() {
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
            namespace = in.readUTF(); namespace = "\0".equals(namespace) ? null : namespace;
            name = in.readUTF(); name = "\0".equals(name) ? null : name;
            columns = (io.deephaven.db.tables.ColumnDefinition[])in.readObject();
            storageType = in.readInt();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (namespace == null) { out.writeUTF("\0"); } else { out.writeUTF(namespace); }
        if (name == null) { out.writeUTF("\0"); } else { out.writeUTF(name); }
        out.writeObject(columns);
        out.writeInt(storageType);
    }

}
