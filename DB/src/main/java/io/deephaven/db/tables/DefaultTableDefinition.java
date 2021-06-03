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

public class DefaultTableDefinition implements Externalizable, DataObjectStreamConstants, LogOutputAppendable, Copyable<DefaultTableDefinition> {

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

    protected String namespace;
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace=namespace;
    }

    protected String name;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name=name;
    }

    protected io.deephaven.db.tables.ColumnDefinition[] columns;
    public io.deephaven.db.tables.ColumnDefinition[] getColumns() {
        return columns;
    }

    public void setColumns(io.deephaven.db.tables.ColumnDefinition[] columns) {
        this.columns=columns;
    }

    protected int storageType=Integer.MIN_VALUE;
    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType=storageType;
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
        namespace = in.readUTF(); namespace = "\0".equals(namespace) ? null : namespace;
        name = in.readUTF(); name = "\0".equals(name) ? null : name;
        columns = (io.deephaven.db.tables.ColumnDefinition[])in.readObject();
        storageType = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (namespace == null) { out.writeUTF("\0"); } else { out.writeUTF(namespace); }
        if (name == null) { out.writeUTF("\0"); } else { out.writeUTF(name); }
        out.writeObject(columns);
        out.writeInt(storageType);
    }
}
