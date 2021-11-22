package io.deephaven.parquet.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.*;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.TypeConverter;


import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Reader {
    public static final class DummyRecordConverter extends RecordMaterializer<Object> {

        private Object a;
        private GroupConverter root;

        public DummyRecordConverter(MessageType schema) {
            this.root = (GroupConverter) schema.convertWith(new TypeConverter<Converter>() {

                @Override
                public Converter convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType) {
                    String name = primitiveType.getName();
                    return new PrimitiveConverter() {

                        @Override
                        public void addBinary(Binary value) {
                            System.out.print(" " + name + " = " + value);
                        }

                        @Override
                        public void addBoolean(boolean value) {
                            System.out.print(" " + name + " = " + value);
                        }

                        @Override
                        public void addDouble(double value) {
                            System.out.print(" " + name + " = " + value);

                        }

                        @Override
                        public void addFloat(float value) {
                            System.out.print(" " + name + " = " + value);

                        }

                        @Override
                        public void addInt(int value) {
                            System.out.print(" " + name + " = " + value);

                        }

                        @Override
                        public void addLong(long value) {
                            System.out.print(" " + name + " = " + value);

                        }
                    };
                }

                @Override
                public Converter convertGroupType(List<GroupType> path, GroupType groupType,
                        final List<Converter> converters) {
                    String name = groupType.getName();
                    return new GroupConverter() {

                        public Converter getConverter(int fieldIndex) {
                            return converters.get(fieldIndex);
                        }

                        public void start() {
                            System.out.print(name + "{");
                        }

                        public void end() {
                            System.out.println("}" + name);

                        }

                    };
                }

                @Override
                public Converter convertMessageType(MessageType messageType, List<Converter> children) {
                    return convertGroupType(null, messageType, children);
                }
            });
        }

        @Override
        public Object getCurrentRecord() {
            return a;
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }

    }


    static class SillyReadSupport extends ReadSupport {

        public ReadContext init(InitContext context) {
            return new ReadContext(context.getFileSchema(), Collections.emptyMap());
        }

        @Override
        public RecordMaterializer prepareForRead(Configuration configuration, Map keyValueMetaData,
                MessageType fileSchema, ReadContext readContext) {
            return new DummyRecordConverter(fileSchema);
        }
    }

    public static void main(String[] args) throws IOException {
        ParquetReader pr = ParquetReader.builder(new SillyReadSupport(), new Path(args[0])).build();
        Object r = pr.read();
        pr.read();
        pr.read();
    }
}
