/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.dataobjects.AbstractDataObject;
import io.deephaven.dataobjects.DataObjectColumn;
import io.deephaven.dataobjects.DataObjectColumnSet;
import io.deephaven.dataobjects.DataObjectColumnSetManager;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Date;

/**
 * To be used by the ADO.readExternal()
 */
public class ColumnsetConversionSchema implements DataObjectStreamConstants {

    private final int[] oldColTypes;
    private final int[] newIndices;
    private final boolean[] isKey;

    public ColumnsetConversionSchema(@NotNull final DataObjectColumnSet oldColumnSet) {
        final DataObjectColumn[] oldCols = oldColumnSet.getColumns();
        oldColTypes = new int[oldCols.length];
        newIndices = new int[oldCols.length];
        isKey = new boolean [oldCols.length];

        final DataObjectColumnSet newColumnSet = DataObjectColumnSetManager.getInstance().getColumnSet(oldColumnSet.getName());
        for (int i=0;i<oldCols.length;i++) {
            final DataObjectColumn col = oldCols[i];
            final Class type = col.getType();
            newIndices[i] = newColumnSet.getColumnIndex(col.getName());
            isKey[i] = newIndices[i]!=-1 && newColumnSet.getColumn(newIndices[i]).isKey();

            final String serialiseIt;
            final boolean serializeAsObject;

            if (newIndices[i]== -1){
                //this will work unless you delete a String column that has shouldBeSerialized=false.  if we do that we'll need to figure this out in the future
                serialiseIt = col.shouldBeSerialized() ? "true" : "false";
                serializeAsObject = col.serializeAsObject();
            }
            else{
                serialiseIt=newColumnSet.getColumn(newIndices[i]).getAttributeValue("shouldBeSerialized");
                serializeAsObject = "true".equalsIgnoreCase(newColumnSet.getColumn(newIndices[i]).getAttributeValue("serializeAsObject", "false"));
            }

            if ("false".equalsIgnoreCase(serialiseIt)) newIndices[i] = -1;
            else if (serializeAsObject) oldColTypes[i] = DataObjectStreamConstants.OBJECT_TYPE;
            else if (type == String.class) oldColTypes[i] = DataObjectStreamConstants.STRING_TYPE;
            else if (type==Double.class) oldColTypes[i] = DataObjectStreamConstants.DOUBLE_TYPE;
            else if (type==Integer.class) oldColTypes[i] = DataObjectStreamConstants.INTEGER_TYPE;
            else if (type==Long.class || type==Date.class) oldColTypes[i] = DataObjectStreamConstants.LONG_TYPE;
            else if (type==Byte.class) oldColTypes[i] = DataObjectStreamConstants.BYTE_TYPE;
            else if ("true".equalsIgnoreCase(serialiseIt) || type.isArray() || type==Boolean.class) oldColTypes[i] = DataObjectStreamConstants.OBJECT_TYPE;
            else newIndices[i] = -1;
        }        
    }

    public void readExternalADO(@NotNull final ObjectInput in, @NotNull final AbstractDataObject obj) throws IOException, ClassNotFoundException {
        for (int i = 0; i < newIndices.length; i++) {
            int index = newIndices[i];
            switch (oldColTypes[i]) {
                case 0: break; // skip object fields that are not serialized
                case DataObjectStreamConstants.STRING_TYPE:
                    String s = in.readUTF();
                    // intern key to speed up CompareUtils.equals (which does '==' before doing '.equals')
                    if (index != -1 && !"\0".equals(s)) { obj.setString(index, isKey[i] ? s.intern() : s); }
                    break;
                case DataObjectStreamConstants.INTEGER_TYPE:
                    int intVal = in.readInt();
                    if (index != -1) { obj.setInt(index, intVal); }
                    break;
                case DataObjectStreamConstants.DOUBLE_TYPE:
                    double doubleVal = in.readDouble();
                    if (index != -1) { obj.setDouble(index, doubleVal); }
                    break;
                case DataObjectStreamConstants.LONG_TYPE:
                    long longVal = in.readLong();
                    if (index != -1) { obj.setLong(index, longVal); }
                    break;
                case DataObjectStreamConstants.BYTE_TYPE:
                    byte byteVal = in.readByte();
                    if (index != -1) { obj.setByte(index, byteVal); }
                    break;
                case DataObjectStreamConstants.OBJECT_TYPE:
                    Object objVal = in.readObject();
                    if (index != -1) {
                        obj.setValue(index, objVal); }
                    break;
                default: throw new IllegalStateException("Error during ADO deserialization: should never get here.");
            }
        }
    }
}
