/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import java.io.*;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.base.log.*;
import io.deephaven.base.*;

public class DefaultTableDefinition implements Externalizable, LogOutputAppendable, Copyable<DefaultTableDefinition> {

    private static final long serialVersionUID = 5684394895950293942L;

    public DefaultTableDefinition() {
        super();
    }

    public DefaultTableDefinition(DefaultTableDefinition source) {
        copyValues(source);
    }

    protected io.deephaven.db.tables.ColumnDefinition[] columns;
    public io.deephaven.db.tables.ColumnDefinition[] getColumns() {
        return columns;
    }

    public void setColumns(io.deephaven.db.tables.ColumnDefinition[] columns) {
        this.columns=columns;
    }

    public void copyValues(DefaultTableDefinition x) {
        columns = x.columns;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        logOutput.append("TableDefinition");
        return logOutput;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultTableDefinition) {
            DefaultTableDefinition castObj = (DefaultTableDefinition) obj;
            if (columns.length != castObj.columns.length) {
                return false;
            }
            for (int i = 0; i < columns.length; ++i) {
                if (!columns[i].equals(castObj.columns[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        return HashCodeUtil.combineHashCodes(columns);
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
        columns = (io.deephaven.db.tables.ColumnDefinition[])in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(columns);
    }
}
