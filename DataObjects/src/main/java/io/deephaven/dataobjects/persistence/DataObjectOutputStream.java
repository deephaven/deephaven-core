/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.io.streams.ZipOutputStream;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.dataobjects.AbstractDataObject;
import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectShortHashMap;

import java.io.*;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class DataObjectOutputStream implements DataObjectStreamConstants, Flushable, Closeable, DataOutput, ObjectOutput {

    private ObjectOutputStream out_;

    //the length of the string beyond which a string will be saved as LONG_STRING_TYPE
    private static final int LONG_STRING_THRESHOLD = 32000;

    private TObjectByteHashMap classMap_ = new TObjectByteHashMap();
    private TObjectShortHashMap enumCodeMap_ = new TObjectShortHashMap();

    public DataObjectOutputStream(OutputStream out) throws IOException {
        this(out, false);
    }

    public DataObjectOutputStream(OutputStream out, boolean useCompression) throws IOException {
        if (useCompression) {
            out = new ZipOutputStream(out);
        }
        out_=new ObjectOutputStream(out){
            protected void writeStreamHeader() {

            }
        };
    }

    public void write(byte b[]) throws IOException {
        out_.write(b);
    }

    public void write(byte b[], int off, int len) throws IOException {
        out_.write(b, off, len);
    }

    public void write(int b) throws IOException {
        out_.write(b);
    }

    public void writeBoolean(boolean v) throws IOException {
        out_.writeBoolean(v);
    }

    public void writeByte(int v) throws IOException {
        out_.writeByte(v);
    }

    public void writeBytes(String s) throws IOException {
        out_.writeBytes(s);
    }

    public void writeChar(int v) throws IOException {
        out_.writeChar(v);
    }

    public void writeChars(String s) throws IOException {
        out_.writeChars(s);
    }

    public void writeDouble(double v) throws IOException {
        out_.writeDouble(v);
    }

    public void writeFloat(float v) throws IOException {
        out_.writeFloat(v);
    }

    public void writeInt(int v) throws IOException {
        out_.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        out_.writeLong(v);
    }

    public void writeShort(int v) throws IOException {
        out_.writeShort(v);
    }

    public void writeUTF(String str) throws IOException {
        out_.writeUTF(str);
    }

    public void writeObject(Object obj) throws IOException {
        if (obj==null){
            writeByte(NULL_TYPE);

            return;
        }

        Class type=obj.getClass();

        if (obj instanceof Externalizable){
            byte classByte = classMap_.get(type);

            if (classByte==0){
                Assert.assertion(classMap_.size() < Byte.MAX_VALUE,"classMap_.size() < Byte.MAX_VALUE",
                        classMap_.size(),"classMap_.size()");

                classByte=(byte)(classMap_.size()+1);

                classMap_.put(type, classByte);

                out_.writeByte(ADD_CLASS);
                out_.writeByte(classByte);
                out_.writeUTF(type.getName());

                ((Externalizable)obj).writeExternal(this);
            }
            else{
                out_.writeByte(classByte);
                ((Externalizable)obj).writeExternal(this);
            }
        } else if (obj instanceof Enum) {
            short enumTypeCode = enumCodeMap_.get(type);
            if (enumTypeCode == 0) {
                enumTypeCode = registerNewEnumType(type);
                out_.writeByte(ADD_ENUM_LONG);
                out_.writeShort(enumTypeCode);
                out_.writeUTF(type.getName());
            } else {
                out_.writeByte(KNOWN_ENUM_LONG);
                out_.writeShort(enumTypeCode);
            }
            int ordinal = ((Enum)obj).ordinal();
            out_.writeShort(ordinal);

        } else if (type==Object[].class){
            out_.writeByte(ARRAY_TYPE);

            Object[] objs = (Object[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                writeObject(objs[i]);
            }
        }
        else if (type==String[].class){
            out_.writeByte(STRING_ARRAY_TYPE);

            String[] objs = (String[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                writeObject(objs[i]);
            }
        }
        else if (type== AbstractDataObject[].class){
            out_.writeByte(ADO_ARRAY_TYPE);

            AbstractDataObject[] objs = (AbstractDataObject[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                writeObject(objs[i]);
            }
        }
        else if (type==int[].class){
            out_.writeByte(INT_ARRAY_TYPE);

            int[] objs = (int[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                out_.writeInt(objs[i]);
            }
        }
        else if (type==double[].class){
            out_.writeByte(DOUBLE_ARRAY_TYPE);

            double[] objs = (double[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                out_.writeDouble(objs[i]);
            }
        }
        else if (type==float[].class){
            out_.writeByte(FLOAT_ARRAY_TYPE);

            float[] objs = (float[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                out_.writeFloat(objs[i]);
            }
        }
        else if (type==long[].class){
            out_.writeByte(LONG_ARRAY_TYPE);

            long[] objs = (long[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                out_.writeLong(objs[i]);
            }
        }
        else if (type==byte[].class){
            out_.writeByte(BYTE_ARRAY_TYPE);

            byte[] objs = (byte[])obj;
            int len = objs.length;
            out_.writeInt(len);

            for (int i = 0; i < len; i++) {
                out_.writeByte(objs[i]);
            }
        }
        else if (type==String.class){
            String str = (String) obj;
            if (str.length() > LONG_STRING_THRESHOLD) {
                out_.writeByte(LONG_STRING_TYPE);
                final byte[] bytes = str.getBytes();
                out_.writeInt(bytes.length);
                out_.write(bytes);
            } else {
                out_.writeByte(STRING_TYPE);
                out_.writeUTF(str);
            }
        }
        else if (type==Integer.class) {
            out_.writeByte(INTEGER_TYPE);
            out_.writeInt(((Integer)obj).intValue());
        }
        else if (type==Double.class) {
            out_.writeByte(DOUBLE_TYPE);
            out_.writeDouble(((Double)obj).doubleValue());
        }
        else if (type==Float.class) {
            out_.writeByte(FLOAT_TYPE);
            out_.writeFloat(((Float)obj).floatValue());
        }
        else if (type==Long.class) {
            out_.writeByte(LONG_TYPE);
            out_.writeLong(((Long)obj).longValue());
        }
        else if (type==Byte.class) {
            out_.writeByte(BYTE_TYPE);
            out_.writeByte(((Byte)obj).byteValue());
        }
        else if (type==Boolean.class) {
            out_.writeByte(BOOLEAN_TYPE);
            out_.writeBoolean((Boolean)obj);
        }
        else if (type==Date.class) {
            out_.writeByte(DATE_TYPE);
            out_.writeLong(((Date)obj).getTime());
        }
        else if (Map.class.isAssignableFrom(type)){
            out_.writeByte((LinkedHashMap.class.isAssignableFrom(type)) ? LINKED_MAP_TYPE : MAP_TYPE);

            out_.writeInt(((Map)obj).size());

            for (Iterator it=((Map)obj).entrySet().iterator(); it.hasNext();){
                Map.Entry entry = (Map.Entry)it.next();

                writeObject(entry.getKey());
                writeObject(entry.getValue());
            }
        }
        else if (Set.class.isAssignableFrom(type)){
            out_.writeByte((LinkedHashSet.class.isAssignableFrom(type)) ? LINKED_SET_TYPE : SET_TYPE);

            out_.writeInt(((Set)obj).size());

            for (Iterator it=((Set)obj).iterator(); it.hasNext();){
                writeObject(it.next());
            }
        }
        else{
            out_.writeByte(OBJECT_TYPE);
            out_.writeObject(obj);

            out_.reset();
        }
    }

    private short registerNewEnumType(Class type) {
        Require.requirement(enumCodeMap_.size() < Short.MAX_VALUE,"enumCodeMap_.size() < Short.MAX_VALUE",
                enumCodeMap_.size(),"enumCodeMap_.size()");
        short result = (short) (enumCodeMap_.size() + 1);
        enumCodeMap_.put(type,result);
        return result;
    }

    public void close() throws IOException {
        out_.close();
    }

    public void flush() throws IOException {
        out_.flush();
    }
}
