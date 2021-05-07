/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.dataobjects.AbstractDataObject;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.streams.ZipInputStream;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;

import io.deephaven.internal.log.LoggerFactory;
import java.io.*;
import java.util.*;

public class DataObjectInputStream implements DataObjectStreamConstants, Closeable, DataInput, ObjectInput {

    static {
        // force the conversion table to be loaded before we ever instantiate a DataObjectInputStream
        DataObjectInputConversions.getObjectConverter("");
    }

    public interface ObjectConverter {
        Object readExternalizedObject(ObjectInput in) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException;
        ObjectStreamClass convertClassDescriptor(ObjectStreamClass descriptor) throws ClassNotFoundException;
        ObjectStreamClass convertResolveClassDescriptor(ObjectStreamClass descriptor) throws ClassNotFoundException;
    }

    private static class DefaultObjectConverter implements ObjectConverter {
        private final Class theClass;
        DefaultObjectConverter(Class theClass) {
            this.theClass = theClass;
        }
        public Object readExternalizedObject(ObjectInput in) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
            Object obj = theClass.newInstance();
            ((Externalizable)obj).readExternal(in);
            return obj;
        }
        public ObjectStreamClass convertClassDescriptor(ObjectStreamClass descriptor) {
            return descriptor;
        }
        public ObjectStreamClass convertResolveClassDescriptor(ObjectStreamClass descriptor) {
            return descriptor;
        }
    }

    protected WrappedObjectInputStream in_;

    private TByteObjectHashMap readerMap_ = new TByteObjectHashMap();
    private TShortObjectHashMap enumTypeCodeToOrdinalReverseMaps_ = new TShortObjectHashMap();

    private static final Logger log = LoggerFactory.getLogger(DataObjectInputStream.class);
    private static final String EMPTY_STRING = "";

    public DataObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
        this(in, false, classLoader);
    }

    public DataObjectInputStream(InputStream in, boolean useCompression, ClassLoader classLoader) throws IOException {
        if (useCompression) {
            in = new ZipInputStream(in);
        }

        in_=new WrappedObjectInputStream(in, classLoader);
    }

    public DataObjectInputStream(InputStream in) throws IOException {
        this(in, false);
    }

    public DataObjectInputStream(InputStream in, boolean useCompression) throws IOException {
        this(in, useCompression, null);
    }

    protected ObjectStreamClass readClassDescriptorOverride(ObjectStreamClass input) throws ClassNotFoundException {
        ObjectConverter converter = DataObjectInputConversions.getObjectConverter(input.getName());
        if ( converter != null ) {
            return  converter.convertClassDescriptor(input);
        }
        return input;
    }

    protected ObjectStreamClass resolveClassDescriptorOverride(ObjectStreamClass input) throws ClassNotFoundException {
        ObjectConverter converter = DataObjectInputConversions.getObjectConverter(input.getName());
        if ( converter != null ) {
            return  converter.convertResolveClassDescriptor(input);
        }
        return input;
    }

    public boolean readBoolean() throws IOException {
        return in_.readBoolean();
    }

    public byte readByte() throws IOException {
        return in_.readByte();
    }

    public char readChar() throws IOException {
        return in_.readChar();
    }

    public double readDouble() throws IOException {
        return in_.readDouble();
    }

    public float readFloat() throws IOException {
        return in_.readFloat();
    }

    public int readInt() throws IOException {
        return in_.readInt();
    }

    public String readLine() throws IOException {
        return null;  //i don't support this....
    }

    public long readLong() throws IOException {
        return in_.readLong();
    }

    public short readShort() throws IOException {
        return in_.readShort();
    }

    public int readUnsignedByte() throws IOException {
        return in_.readUnsignedByte();
    }

    public int readUnsignedShort() throws IOException {
        return in_.readUnsignedShort();
    }

    public String readUTF() throws IOException {
        return in_.readUTF();
    }

    public Object readObject() throws ClassNotFoundException, IOException {
        byte type = in_.readByte();

        if (type == ADD_CLASS) {
            type=in_.readByte();
            String typeString = in_.readUTF();
            readerMap_.put(type, getReaderForType(typeString));
        }

        switch(type) {
            case NULL_TYPE:
                return null;
            case STRING_TYPE: {
                String s = in_.readUTF();
                return s.length() == 0 ? EMPTY_STRING : s;
            }
            case LONG_STRING_TYPE:
                int len = in_.readInt();
                byte[] buffer = new byte[len];
                in_.readFully(buffer);
                return new String(buffer);
            case INTEGER_TYPE:
                return new Integer(in_.readInt());
            case DOUBLE_TYPE:
                return new Double(in_.readDouble());
            case FLOAT_TYPE:
                return new Float(in_.readFloat());
            case LONG_TYPE:
                return new Long(in_.readLong());
            case BYTE_TYPE:
                return new Byte(in_.readByte());
            case BOOLEAN_TYPE:
                return Boolean.valueOf(in_.readBoolean());
            case DATE_TYPE:
                return new Date(in_.readLong());
            case OBJECT_TYPE :
                return in_.readObject();
            case ARRAY_TYPE:
                Object[] objs = new Object[in_.readInt()];
                for (int i=0; i<objs.length; i++)
                    objs[i]=readObject();
                return objs;
            case STRING_ARRAY_TYPE:
                String[] strObjs = new String[in_.readInt()];
                for (int i=0; i<strObjs.length; i++) {
                    String s=(String)readObject();
                    if (s!=null) strObjs[i]=s;
                }
                return strObjs;
            case ADO_ARRAY_TYPE:
                AbstractDataObject[] adoObjs = new AbstractDataObject[in_.readInt()];
                for (int i=0; i<adoObjs.length; i++)
                    adoObjs[i] = (AbstractDataObject)readObject();
                return adoObjs;
            case INT_ARRAY_TYPE:
                int[] intArray = new int[in_.readInt()];
                for (int i=0; i<intArray.length; i++)
                    intArray[i]=in_.readInt();
                return intArray;
            case DOUBLE_ARRAY_TYPE:
                double[] doubleArray = new double[in_.readInt()];
                for (int i=0; i<doubleArray.length; i++)
                    doubleArray[i]=in_.readDouble();
                return doubleArray;
            case FLOAT_ARRAY_TYPE:
                float[] floatArray = new float[in_.readInt()];
                for (int i=0; i<floatArray.length; i++)
                    floatArray[i]=in_.readFloat();
                return floatArray;
            case LONG_ARRAY_TYPE:
                long[] longArray = new long[in_.readInt()];
                for (int i=0; i<longArray.length; i++)
                    longArray[i]=in_.readLong();
                return longArray;
            case BYTE_ARRAY_TYPE:
                byte[] byteArray = new byte[in_.readInt()];
                for (int i=0; i<byteArray.length; i++)
                    byteArray[i]=in_.readByte();
                return byteArray;
            case MAP_TYPE:
            case LINKED_MAP_TYPE:
                int sz = in_.readInt();
                HashMap map = (type==MAP_TYPE) ? new HashMap(sz) : new LinkedHashMap(sz);
                for (int i=0; i<sz; i++)
                    map.put(readObject(), readObject());
                return map;
            case SET_TYPE:
            case LINKED_SET_TYPE:
                sz = in_.readInt();
                HashSet set = (type==SET_TYPE) ? new HashSet(sz) : new LinkedHashSet(sz);
                for (int i=0; i<sz; i++)
                    set.add(readObject());
                return set;
            case ADD_ENUM:
                short enumTypeCode = in_.readShort();
                String typeString = in_.readUTF();
                registerEnumType(enumTypeCode,typeString);
                byte ordinal = in_.readByte();
                return ordinalToEnum(ordinal,enumTypeCode);
            case KNOWN_ENUM:
                enumTypeCode = in_.readShort();
                ordinal = in_.readByte();
                return ordinalToEnum(ordinal,enumTypeCode);
            case ADD_ENUM_LONG:
                enumTypeCode = in_.readShort();
                typeString = in_.readUTF();
                registerEnumType(enumTypeCode,typeString);
                short ordinalShort = in_.readShort();
                return ordinalToEnum(ordinalShort, enumTypeCode);
            case KNOWN_ENUM_LONG:
                enumTypeCode = in_.readShort();
                ordinalShort = in_.readShort();
                return ordinalToEnum(ordinalShort, enumTypeCode);
            default:
                try {
                    return ((ObjectConverter) readerMap_.get(type)).readExternalizedObject(this);
                } catch (Exception e) {
                    log.error().append("Failed to read externalized object: ").append(e).endl();
                    // This is a serious error with the stream, but we handle corrupt stream errors up the chain, so log it
                    //  and throw it up the chain instead of killing the server
                    throw new StreamCorruptedException("Corrupt stream detected");
                }
        }
    }

    private Object ordinalToEnum(short ordinal, short enumTypeCode) {
        return ((TShortObjectHashMap)enumTypeCodeToOrdinalReverseMaps_.get(enumTypeCode)).get(ordinal);
    }

    private void registerEnumType(short enumTypeCode, String typeString) throws ClassNotFoundException {
        Class <? extends Enum> enumType = Class.forName(typeString).asSubclass(Enum.class);
        Enum[] enumValues = enumType.getEnumConstants();
        TShortObjectHashMap ordinalToEnum = new TShortObjectHashMap(enumValues.length);
        enumTypeCodeToOrdinalReverseMaps_.put(enumTypeCode, ordinalToEnum);
        for (Enum enumValue : enumValues) {
            ordinalToEnum.put((short) enumValue.ordinal(), enumValue);
        }
    }

    protected ObjectConverter getReaderForType(String typeString) throws ClassNotFoundException {
        ObjectConverter reader = DataObjectInputConversions.getObjectConverter(typeString);
        return reader != null ? reader : new DefaultObjectConverter(getClassForType(typeString));
    }

    protected Class getClassForType(String typeString) throws ClassNotFoundException {
        return Class.forName(typeString);
    }

    public void readFully(byte b[]) throws IOException {
        in_.readFully(b);
    }

    public void readFully(byte b[], int off, int len) throws IOException {
        in_.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return in_.skipBytes(n);
    }

    public int available() throws IOException {
        return in_.available();
    }

    public void close() throws IOException {
        in_.close();
    }

    public int read() throws IOException {
        return in_.read();
    }

    public int read(byte b[]) throws IOException {
        return in_.read(b);
    }

    public int read(byte b[], int off, int len) throws IOException {
        return in_.read(b, off, len);
    }

    public long skip(long n) throws IOException {
        return in_.skip(n);
    }

    public class WrappedObjectInputStream extends CustomClassLoaderObjectInputStream<ClassLoader> {

        WrappedObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in, classLoader);
        }

        WrappedObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        protected void readStreamHeader() {
        }

        protected ObjectStreamClass readClassDescriptor()  throws IOException, ClassNotFoundException {
            ObjectStreamClass result = super.readClassDescriptor();
            return readClassDescriptorOverride(result);
        }

        public DataObjectInputStream getWObjectInputStream(){
            return DataObjectInputStream.this;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
            return super.resolveClass(resolveClassDescriptorOverride(desc));
        }
    }
}
