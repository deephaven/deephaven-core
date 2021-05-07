/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.dataobjects.persistence.PersistentInputStream;
import io.deephaven.datastructures.util.CollectionUtil;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AbstractDataObjectImpl implements AbstractDataObject, Externalizable {

    protected static Map<String,DataObjectInfo> dataObjectInfos_ = new ConcurrentHashMap<String,DataObjectInfo>();

    protected Object[] objects_;
    protected int[] ints_;
    protected double[] doubles_;
    protected long[] longs_;
    protected byte[] bytes_;

    protected DataObjectInfo dataObjectInfo_;

    public AbstractDataObjectImpl(){
        //for externalizable...
    }

    public AbstractDataObjectImpl(String columnSetName) {
        dataObjectInfo_=dataObjectInfos_.get(columnSetName);

        if (dataObjectInfo_==null) {
            dataObjectInfo_=new DataObjectInfo(columnSetName);

            dataObjectInfos_.put(columnSetName, dataObjectInfo_);
        }

        initData();
    }

    public AbstractDataObjectImpl(DataObjectColumnSet columnSet) {
        dataObjectInfo_=dataObjectInfos_.get(columnSet.getName());

        if (dataObjectInfo_==null) {
            dataObjectInfo_=new DataObjectInfo(columnSet);

            dataObjectInfos_.put(columnSet.getName(), dataObjectInfo_);
        }

        initData();
    }

    public AbstractDataObjectImpl(DataObjectColumnSet columnSet, Object[] columnValues) {
        this(columnSet);

        if (columnSet.getColumns().length != columnValues.length)
            throw new IllegalArgumentException("columnSet.getColumns().length != columnValues.length");

        for (int i=0;i<columnValues.length;i++)
            if (columnValues[i] != null)
                setValue(i, columnValues[i]);
    }

    private void initData() {
        objects_=0==dataObjectInfo_.numberObjects?CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY:new Object[dataObjectInfo_.numberObjects];
        ints_=0==dataObjectInfo_.numberInts?CollectionUtil.ZERO_LENGTH_INT_ARRAY:new int[dataObjectInfo_.numberInts];
        doubles_=0==dataObjectInfo_.numberDoubles?CollectionUtil.ZERO_LENGTH_DOUBLE_ARRAY:new double[dataObjectInfo_.numberDoubles];
        longs_=0==dataObjectInfo_.numberLongs?CollectionUtil.ZERO_LENGTH_LONG_ARRAY:new long[dataObjectInfo_.numberLongs];
        bytes_=0==dataObjectInfo_.numberBytes?CollectionUtil.ZERO_LENGTH_BYTE_ARRAY:new byte[dataObjectInfo_.numberBytes];

        clear();
    }

    public void clear() {
        Arrays.fill(objects_, null);
        Arrays.fill(ints_, Integer.MIN_VALUE);
        Arrays.fill(doubles_, Double.NaN);
        Arrays.fill(longs_, Long.MIN_VALUE);
        Arrays.fill(bytes_, Byte.MIN_VALUE);
    }

    public DataObjectColumnSet getColumnSet() {
        return dataObjectInfo_.columnSet_;
    }

    //--------------------------------------------------------------------------------------------

    public String getString(int index) {
        return (String) objects_[dataObjectInfo_.indexMap_[index]];
    }

    public Object getObject(int index) {
        return objects_[dataObjectInfo_.indexMap_[index]];
    }

    public int getInt(int index) {
        return ints_[dataObjectInfo_.indexMap_[index]];
    }

    public double getDouble(int index) {
        return doubles_[dataObjectInfo_.indexMap_[index]];
    }

    public long getLong(int index) {
        return longs_[dataObjectInfo_.indexMap_[index]];
    }

    public Date getDate(int index) {
        long theLong = getLong(index);

        return (theLong==Long.MIN_VALUE) ? null : new Date(theLong);
    }

    public byte getByte(int index) {
        return bytes_[dataObjectInfo_.indexMap_[index]];
    }

    public Object getValue(int index) {
        Class type=dataObjectInfo_.columnSet_.getColumn(index).getType();

        if (type==String.class) {
            return getString(index);
        } else if (type==Integer.class) {
            if(getInt(index) != Integer.MIN_VALUE)
                return new Integer(getInt(index));
        } else if (type==Double.class) {
            if(!Double.isNaN(getDouble(index)))
                return new Double(getDouble(index));
        } else if (type==Long.class) {
            if(getLong(index) != Long.MIN_VALUE)
                return new Long(getLong(index));
        } else if (type==Byte.class) {
            if(getByte(index) != Byte.MIN_VALUE)
                return new Byte(getByte(index));
        } else if (type==Date.class) {
            return getDate(index);
        } else return objects_[dataObjectInfo_.indexMap_[index]];

        return null;
    }

    //--------------------------------------------------------------------------------------------

    public String getString(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getString(index);
    }

    public int getInt(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getInt(index);
    }

    public double getDouble(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getDouble(index);
    }

    public long getLong(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getLong(index);
    }

    public Date getDate(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getDate(index);
    }

    public byte getByte(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        return getByte(index);
    }

    public Object getValue(String name) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        if (index==-1)
            throw new IllegalArgumentException("Column " + name + " not found in column set " + getColumnSet().getName());

        return getValue(index);
    }

    //--------------------------------------------------------------------------------------------

    public String getString(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?null:ado.getString(index[index.length-1]);
    }

    public int getInt(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?Integer.MIN_VALUE:ado.getInt(index[index.length-1]);
    }

    public double getDouble(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?Double.MIN_VALUE:ado.getDouble(index[index.length-1]);
    }

    public long getLong(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?Long.MIN_VALUE:ado.getLong(index[index.length-1]);
    }

    public Date getDate(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?null:ado.getDate(index[index.length-1]);
    }

    public byte getByte(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?Byte.MIN_VALUE:ado.getByte(index[index.length-1]);
    }

    public Object getValue(int index[]) {
        AbstractDataObject ado = getRefObject(index);

        return (ado == null)?null:ado.getValue(index[index.length-1]);
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


    //--------------------------------------------------------------------------------------------

    public void setValue(int index, Object value) {
        if (value != null) {
            Class type=value.getClass();

            if (type==String.class) {
                setString(index, value.toString());
            }
            else if (type==Integer.class) {
                setInt(index, ((Number)value).intValue());
            }
            else if (type==Double.class) {
                setDouble(index, ((Number)value).doubleValue());
            }
            else if (type==Long.class) {
                setLong(index, ((Number)value).longValue());
            }
            else if (type==Byte.class) {
                setByte(index, ((Number)value).byteValue());
            }
            else if (type==Date.class) {
                setDate(index, ((Date)value));
            } else {
                setObject(index, value);
            }
        } else {
            DataObjectColumn col = this.getColumnSet().getColumn(index);
            Class type = col.getType();
            if (type == String.class)
                setString(index, null);
        }
    }

    private void setObject(int index, Object value) {
        objects_[dataObjectInfo_.indexMap_[index]]=value;
    }

    public void setValues(Object[] values) {
        int nCols = dataObjectInfo_.columnSet_.getSize();
        if (values.length != nCols)
            throw new IllegalArgumentException("Column set=" + getColumnSet().getName() + "; Need " + nCols + " cols in values[].");
        for (int i=0;i<nCols;i++)
            if (values[i] != null)
                setValue(i, values[i]);
    }

    /** Used by JNI **/
    public void setDirectInts(ByteBuffer ints) {
        if (ints_.length > 0) {
            ints.order(ByteOrder.nativeOrder());
            ints.asIntBuffer().get(ints_);
        }
    }

    /** Used by JNI **/
    public void setDirectDoubles(ByteBuffer doubles) {
        if (doubles_.length > 0) {
            doubles.order(ByteOrder.nativeOrder());
            doubles.asDoubleBuffer().get(doubles_);
        }
    }

    /** Used by JNI **/
    public void setDirectLongs(ByteBuffer longs) {
        if (longs_.length > 0) {
            longs.order(ByteOrder.nativeOrder());
            longs.asLongBuffer().get(longs_);
        }
    }

    /** Used by JNI **/
    public void setDirectBytes(ByteBuffer bytes) {
        if (bytes_.length > 0) {
            bytes.get(bytes_);
        }
    }

    /** Used by JNI **/
    public void setDirectString(int index, String s) {
        objects_[index]=s;
    }

    /** Used by JNI **/
    private ByteBuffer createBuffer(int size) {
        ByteBuffer result = ByteBuffer.allocateDirect(size);
        result.order(ByteOrder.nativeOrder());
        return result;
    }

    /** Used by JNI **/
    public IntBuffer getDirectInts() {
        return createBuffer(ints_.length * 4).asIntBuffer().put(ints_);
    }

    /** Used by JNI **/
    public LongBuffer getDirectLongs() {
        return createBuffer(longs_.length * 8).asLongBuffer().put(longs_);
    }

    /** Used by JNI **/
    public ByteBuffer getDirectBytes() {
        return createBuffer(bytes_.length).put(bytes_);
    }

    /** Used by JNI **/
    public DoubleBuffer getDirectDoubles() {
        return createBuffer(doubles_.length * 8).asDoubleBuffer().put(doubles_);
    }

    /** Used by JNI **/
    public String getDirectString(int index) {
        return (String) objects_[index];
    }

    public void setValue(String name, Object value) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setValue(index, value);
    }

    public void setString(int index, String s) {
        objects_[dataObjectInfo_.indexMap_[index]]=s;
    }

    public void setInt(int index, int i) {
        ints_[dataObjectInfo_.indexMap_[index]]=i;
    }

    public void setDouble(int index, double d) {
        doubles_[dataObjectInfo_.indexMap_[index]]=d;
    }

    public void setLong(int index, long l) {
        longs_[dataObjectInfo_.indexMap_[index]]=l;
    }

    public void setDate(int index, Date d) {
        longs_[dataObjectInfo_.indexMap_[index]]=(d==null) ? Long.MIN_VALUE : d.getTime();
    }

    public void setByte(int index, byte b) {
        bytes_[dataObjectInfo_.indexMap_[index]]=b;
    }

    public void setString(String name, String s) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setString(index, s);
    }

    public void setInt(String name, int i) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setInt(index, i);
    }

    public void setDouble(String name, double d) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setDouble(index, d);
    }

    public void setLong(String name, long l) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setLong(index, l);
    }

    public void setDate(String name, Date d) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setDate(index, d);
    }

    public void setByte(String name, byte b) {
        int index=dataObjectInfo_.columnSet_.getColumnIndex(name);

        setByte(index, b);
    }

    //--------------------------------------------------------------------------------------------

    public Object[] getObjects() {
        return objects_;
    }

    public int[] getInts() {
        return ints_;
    }

    public double[] getDoubles() {
        return doubles_;
    }

    public long[] getLongs() {
        return longs_;
    }

    public byte[] getBytes() {
        return bytes_;
    }

    //--------------------------------------------------------------------------------------------

    public Object[] getValues() {
        return new Object[]{objects_, ints_, doubles_, longs_, bytes_};
    }

    public void setValues(Object obj) {
        Object data[]=(Object[])obj;

        objects_=(Object[])data[0];
        ints_=(int[])data[1];
        doubles_=(double[])data[2];
        longs_=(long[])data[3];
        bytes_=(byte[])data[4];
    }

    public void copyValues(AbstractDataObjectImpl dataObj) {
        System.arraycopy(dataObj.getDoubles(), 0, doubles_, 0, doubles_.length);
        System.arraycopy(dataObj.getInts(), 0, ints_, 0, ints_.length);
        System.arraycopy(dataObj.getObjects(), 0, objects_, 0, objects_.length);
        System.arraycopy(dataObj.getLongs(), 0, longs_, 0, longs_.length);
        System.arraycopy(dataObj.getBytes(), 0, bytes_, 0, bytes_.length);
    }

    //--------------------------------------------------------------------------------------------

    public String toString() {

        StringBuffer ret=new StringBuffer(dataObjectInfo_.columnSet_.getName());

        ret.append(" : ");

        int sz=dataObjectInfo_.columnSet_.getSize();

        for (int i=0; i<sz; i++) {
            ret.append(dataObjectInfo_.columnSet_.getColumn(i).getAbbreviation());
            ret.append("=");
            ret.append(getValue(i));

            if (i!=sz-1) {
                ret.append("|");
            }
        }

        return ret.toString();
    }

    public boolean isValidForHashing() {
        DataObjectColumn keys[] = getColumnSet().getKeys();
        if (0 == keys.length) {
            return true;
        }

        for (int i=0; i<keys.length; i++) {
            Class type=keys[i].getType();
            int index = keys[i].getIndex();

            if (type == String.class) {
                if (getString(index) == null)
                    return false;
            } else if (type == Integer.class) {
                if (getInt(index) == Integer.MIN_VALUE)
                    return false;
            } else if (type == Double.class) {
                if (Double.isNaN(getDouble(index)))
                    return false;
            } else if (type == Long.class || type == Date.class) {
                if (getLong(index) == Long.MIN_VALUE)
                    return false;
            } else if (type == Byte.class) {
                if (getByte(index) == Byte.MIN_VALUE)
                    return false;
            } else if (getObject(index) == null)
                return false;
        }

        return true;
    }

    public int hashCode() {
        DataObjectColumn keys[] = getColumnSet().getKeys();

        //special case with keys == null for Aggregated ADO's with no aggregation keys
        if (keys == null) return -1;
        if (keys.length==0){
            return super.hashCode();
        }

        int hash;
        int result = 0;

        for (int i=0; i < keys.length; i++) {
            Class type=keys[i].getType();

            if (type==String.class){
                hash= HashCodeUtil.toHashCode(getString(keys[i].getIndex()));
            }
            else if (type==Integer.class){
                hash = getInt(keys[i].getIndex());
            }
            else if (type==Double.class){
                hash= HashCodeUtil.toHashCode(getDouble(keys[i].getIndex()));
            }
            else if (type==Long.class || type==Date.class){
                hash= HashCodeUtil.toHashCode(getLong(keys[i].getIndex()));
            }
            else if (type==Byte.class){
                hash = getByte(keys[i].getIndex());
            }
            else hash= HashCodeUtil.toHashCode(getObject(keys[i].getIndex()));

            result = result ^ hash;
        }
        return result;
    }

    public boolean equals(Object obj) {
        if (obj==null){
            return false;
        }

        final double DOUBLE_EPSILON = 0.0000000001;

        AbstractDataObject ado = (AbstractDataObject)obj;

        DataObjectColumn keys[] = getColumnSet().getKeys();

        //special case with keys == null for Aggregated ADO's with no aggregation keys
        if (keys == null) return true;

        if (keys.length==0){
            return super.equals(obj);
        }

        for (int i=0; i < keys.length; i++) {
            Class type=keys[i].getType();

            if (type==String.class){
                String str1 = getString(keys[i].getIndex());
                String str2 = ado.getString(keys[i].getIndex());
                if ((str1 == null ^ str2 == null) || (str1 != null && str2 != null && !str1.equals(str2))){
                    return false;
                }
            }
            else if (type==Integer.class){
                if (getInt(keys[i].getIndex())!=ado.getInt(keys[i].getIndex())){
                    return false;
                }
            }
            else if (type==Double.class){
                double val1 = getDouble(keys[i].getIndex());
                double val2 = ado.getDouble(keys[i].getIndex());
                if ((Double.isNaN(val1) ^ Double.isNaN(val2)) || (Math.abs(val1 - val2) > DOUBLE_EPSILON)) {
                    return false;
                }
            }
            else if (type==Long.class || type==Date.class){
                if (getLong(keys[i].getIndex())!=ado.getLong(keys[i].getIndex())){
                    return false;
                }
            }
            else if (type==Byte.class){
                if (getByte(keys[i].getIndex())!=ado.getByte(keys[i].getIndex())){
                    return false;
                }
            }
            else {
                Object obj1 = getObject(keys[i].getIndex());
                Object obj2 = ado.getValue(keys[i].getIndex());
                if ((obj1 == null ^ obj2 == null) || (obj1 != null && obj2 != null && !obj1.equals(obj2))){
                    return false;
                }
            }
        }
        return true;
    }

    public Object clone() {
        AbstractDataObjectImpl newObj = new AbstractDataObjectImpl(getColumnSet());
        newObj.copyValues(this);
        return newObj;
    }

    //--------------------------------------------------------------------------------------------

    protected class DataObjectInfo implements Serializable {
        public int numberObjects=0, numberInts=0, numberDoubles=0, numberLongs=0, numberBytes=0;
        public int indexMap_[];
        public DataObjectColumnSet columnSet_;

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeObject(columnSet_);
            out.writeObject(new Integer(numberObjects));
            out.writeObject(new Integer(numberInts));
            out.writeObject(new Integer(numberDoubles));
            out.writeObject(new Integer(numberLongs));
            out.writeObject(new Integer(numberBytes));
            out.writeObject(indexMap_);
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            columnSet_=(DataObjectColumnSet)in.readObject();
            numberObjects=((Integer)in.readObject()).intValue();
            numberInts=((Integer)in.readObject()).intValue();
            numberDoubles=((Integer)in.readObject()).intValue();
            numberLongs=((Integer)in.readObject()).intValue();
            numberBytes=((Integer)in.readObject()).intValue();
            indexMap_=(int[])in.readObject();
        }

        public DataObjectInfo(String columnSetName) {
            this(DataObjectColumnSetManager.getInstance().getColumnSet(columnSetName));
        }

        public DataObjectInfo(DataObjectColumnSet columnSet) {
            columnSet_=columnSet;

            int sz=columnSet_.getSize();

            indexMap_=new int[sz];

            for (int i=0; i<sz; i++) {
                Class type=columnSet_.getColumn(i).getType();

                if (type==Integer.class) {
                    indexMap_[i]=numberInts;

                    numberInts++;
                }
                else if (type==Double.class) {
                    indexMap_[i]=numberDoubles;

                    numberDoubles++;
                }
                else if ((type==Long.class) || (type==Date.class)) {
                    indexMap_[i]=numberLongs;

                    numberLongs++;
                }
                else if (type==Byte.class) {
                    indexMap_[i]=numberBytes;

                    numberBytes++;
                } else {
                    indexMap_[i]=numberObjects;

                    numberObjects++;
                }

            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        String columnSetName=in.readUTF();

        dataObjectInfo_=dataObjectInfos_.get(columnSetName);

        if (dataObjectInfo_==null) {
            dataObjectInfo_=new DataObjectInfo(columnSetName);

            dataObjectInfos_.put(columnSetName, dataObjectInfo_);
        }

        objects_=(Object[])in.readObject();
        ints_=(int[])in.readObject();
        doubles_=(double[])in.readObject();
        longs_=(long[])in.readObject();
        bytes_=(byte[])in.readObject();

        if (in instanceof PersistentInputStream){
            DataObjectColumnSet oldColumnSet = ((PersistentInputStream)in).getColumnSet(columnSetName);

            convertFromOldColumnSet(oldColumnSet);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(dataObjectInfo_.columnSet_.getName());

        out.writeObject(objects_);
        out.writeObject(ints_);
        out.writeObject(doubles_);
        out.writeObject(longs_);
        out.writeObject(bytes_);
    }

    protected void convertFromOldColumnSet(DataObjectColumnSet oldColumnSet){
        AbstractDataObjectImpl ado = new AbstractDataObjectImpl();
        ado.dataObjectInfo_=new DataObjectInfo(oldColumnSet);

        ado.objects_=objects_;
        ado.ints_=ints_;
        ado.doubles_=doubles_;
        ado.longs_=longs_;
        ado.bytes_=bytes_;

        initData();

        int sz=dataObjectInfo_.columnSet_.getSize();

        for (int i=0; i<sz; i++){
            setValue(i, ado.getValue(dataObjectInfo_.columnSet_.getColumn(i).getName()));
        }
    }
}
