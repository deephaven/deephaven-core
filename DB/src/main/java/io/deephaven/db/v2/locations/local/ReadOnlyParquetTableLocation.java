package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.CodecLookup;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.util.file.TrackedFileHandleFactory;
import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.parquet.*;
import io.deephaven.db.v2.locations.parquet.topage.*;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.RowGroupReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class ReadOnlyParquetTableLocation extends AbstractTableLocation<TableKey, ParquetColumnLocation<Attributes.Values>> implements ParquetFormatTableLocation<ParquetColumnLocation<Attributes.Values>> {

    private final Map<String, String> keyValueMetaData;
    private final RowGroupReader rowGroupReader;
    private final Map<String, String[]> columns;

    private final SeekableChannelsProvider cachedChannelProvider = new CachedChannelProvider(new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance()),
            Configuration.getInstance().getIntegerForClassWithDefault(ReadOnlyParquetTableLocation.class, "maxChannels", 100));

    private final File parquetFile;

    private static class GroupingFile {
        RowGroupReader reader;
        Map<String, String> keyValueMetaData;
    }

    private final Map<String, GroupingFile> groupingFiles = new ConcurrentHashMap<>();
    private final Set<String> grouping = new HashSet<>();
    private final ParquetInstructions readInstructions;

    ReadOnlyParquetTableLocation(
            @NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey,
            final File parquetFile,
            final boolean supportsSubscriptions,
            final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, supportsSubscriptions);
        this.readInstructions = readInstructions;
        this.parquetFile = parquetFile;
        try {
            ParquetFileReader parquetFileReader = new ParquetFileReader(parquetFile.getPath(), cachedChannelProvider, -1);
            if (parquetFileReader.fileMetaData.getRow_groups().isEmpty()) {
                rowGroupReader = null;
            } else {
                rowGroupReader = parquetFileReader.getRowGroup(0);
            }

            columns = new HashMap<>();
            for (ColumnDescriptor column : parquetFileReader.getSchema().getColumns()) {
                String[] path = column.getPath();
                if (path.length > 1) {
                    columns.put(path[0], path);
                }
            }

            keyValueMetaData = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData).getFileMetaData().getKeyValueMetaData();
            String grouping = keyValueMetaData.get(ParquetTableWriter.GROUPING);
            if (grouping != null) {
                this.grouping.addAll(Arrays.asList(grouping.split(",")));
            }
        } catch (IOException except) {
            throw new TableDataException("Can't read parquet file", except);
        }

        handleUpdate(rowGroupReader == null ? 0 : rowGroupReader.numRows(), parquetFile.lastModified());
    }

    @Override
    public void refresh() {
    }

    private static final ColumnDefinition<Long> FIRST_KEY_COL_DEF = ColumnDefinition.ofLong("__firstKey__");
    private static final ColumnDefinition<Long> LAST_KEY_COL_DEF = ColumnDefinition.ofLong("__lastKey__");

    @NotNull
    @Override
    protected ParquetColumnLocation<Attributes.Values> makeColumnLocation(@NotNull String colName) {
        final String name = readInstructions.getParquetColumnNameFromColumnNameOrDefault(colName);

        ColumnChunkPageStore.MetaDataCreator getMetaData = null;

        if (grouping.contains(name)) {
            GroupingFile groupingFile = getGroupingFile(name);

            getMetaData = columnDefinition -> {

                ParquetColumnLocation.MetaDataTableFactory metaDataTable =
                        new ParquetColumnLocation.MetaDataTableFactory(
                                this.<Attributes.Values>makeColumnCreator(ParquetTableWriter.GROUPING_KEY, groupingFile.reader, groupingFile.keyValueMetaData, null).get(columnDefinition).pageStore,
                                this.<Attributes.UnorderedKeyIndices>makeColumnCreator(ParquetTableWriter.BEGIN_POS, groupingFile.reader, groupingFile.keyValueMetaData, null).get(FIRST_KEY_COL_DEF).pageStore,
                                this.<Attributes.UnorderedKeyIndices>makeColumnCreator(ParquetTableWriter.END_POS, groupingFile.reader, groupingFile.keyValueMetaData, null).get(LAST_KEY_COL_DEF).pageStore);

                return metaDataTable::get;
            };
        }

        ColumnChunkPageStore.Creator<Attributes.Values> creator = makeColumnCreator(name, rowGroupReader, keyValueMetaData,
                getMetaData);

        return new ParquetColumnLocation<>(this, name, creator);
    }

    private GroupingFile getGroupingFile(String name) {
        return groupingFiles.computeIfAbsent(name,(n)->{
            GroupingFile groupingFile = new GroupingFile();
            final Function<String, String> defaultGroupingFilenameByColumnName =
                    ParquetTableWriter.defaultGroupingFileName(parquetFile.getAbsolutePath());
            try {
                ParquetFileReader parquetFileReader = new ParquetFileReader(defaultGroupingFilenameByColumnName.apply(n), cachedChannelProvider, -1);
                groupingFile.reader = parquetFileReader.getRowGroup(0);
                groupingFile.keyValueMetaData = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData).getFileMetaData().getKeyValueMetaData();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return groupingFile;
        });
    }

    @NotNull
    private <ATTR extends Attributes.Any> ColumnChunkPageStore.Creator<ATTR> makeColumnCreator(
            @NotNull String colName,
            @NotNull RowGroupReader rowGroupReader,
            @NotNull Map<String, String> keyValueMetaData,
            ColumnChunkPageStore.MetaDataCreator getMetadata) {
        final String name = readInstructions.getParquetColumnNameFromColumnNameOrDefault(colName);
        String [] nameList = columns.get(name);
        final ColumnChunkReader columnChunkReader = rowGroupReader.getColumnChunk(nameList == null ?
                Collections.singletonList(name) : Arrays.asList(nameList));

        return ColumnChunkPageStore.creator(columnChunkReader, (@NotNull ColumnDefinition columnDefinition) -> {
            if (columnChunkReader == null) {
                return null;
            }

            final PrimitiveType type = columnChunkReader.getType();
            final LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            final String codecFromInstructions = readInstructions.getCodecName(columnDefinition.getName());
            final String codecName = (codecFromInstructions != null)
                    ? codecFromInstructions
                    : keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + name)
                    ;
            final String specialTypeName = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + name);

            final boolean isArray = columnChunkReader.getMaxRl() > 0;
            final boolean isCodec = CodecLookup.explicitCodecPresent(codecName);

            if (isArray && columnChunkReader.getMaxRl() > 1) {
                throw new TableDataException("No support for nested repeated parquet columns.");
            }

            try {
                // Note that is null for a StringSet.  ToStringSetPage.create specifically doesn't take this parameter.
                final Class<?> dataType = columnDefinition.getDataType();
                final Class<?> componentType = columnDefinition.getComponentType();
                final Class<?> pageType = isArray ? componentType : dataType;

                ToPage<ATTR, ?> toPage = null;

                if (logicalTypeAnnotation != null) {
                    toPage = logicalTypeAnnotation.accept(new LogicalTypeVisitor<ATTR>(name, columnChunkReader, pageType)).orElse(null);
                }

                if (toPage == null) {
                    final PrimitiveType.PrimitiveTypeName typeName = type.getPrimitiveTypeName();
                    switch (typeName) {
                        case BOOLEAN:
                            toPage = ToBooleanAsBytePage.create(pageType);
                            break;
                        case INT32:
                            toPage = ToIntPage.create(pageType);
                            break;
                        case INT64:
                            toPage = ToLongPage.create(pageType);
                            break;
                        case INT96:
                            toPage = ToDBDateTimePageFromInt96.create(pageType);
                            break;
                        case DOUBLE:
                            toPage = ToDoublePage.create(pageType);
                            break;
                        case FLOAT:
                            toPage = ToFloatPage.create(pageType);
                            break;
                        case BINARY:
                        case FIXED_LEN_BYTE_ARRAY:
                            //noinspection rawtypes
                            final ObjectCodec codec;
                            if (isCodec) {
                                final String codecArgs = (codecFromInstructions != null)
                                        ? readInstructions.getCodecArgs(columnDefinition.getName())
                                        : keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + name)
                                        ;
                                codec = CodecCache.DEFAULT.getCodec(codecName, codecArgs);
                            } else {
                                final String codecArgs = (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                        ? Integer.toString(type.getTypeLength())
                                        : null
                                        ;
                                codec = CodecCache.DEFAULT.getCodec(SimpleByteArrayCodec.class.getName(), codecArgs);
                            }
                            //noinspection unchecked
                            toPage = ToObjectPage.create(dataType, codec, columnChunkReader.getDictionary());
                            break;
                        default:
                    }
                }

                if (toPage == null) {
                    throw new TableDataException("Unsupported parquet column type " + type.getPrimitiveTypeName() +
                            " with logical type " + logicalTypeAnnotation);
                }

                if (Objects.equals(specialTypeName, ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                    Assert.assertion(isArray, "isArray");
                    toPage = ToStringSetPage.create(dataType, toPage);
                } else if (isArray) {
                    Assert.assertion(!isCodec, "!isCodec");
                    if (DbArrayBase.class.isAssignableFrom(dataType)) {
                        toPage = ToDbArrayPage.create(dataType, componentType, toPage);
                    } else if (dataType.isArray()) {
                        toPage = ToArrayPage.create(dataType, componentType, toPage);
                    }
                }

                return toPage;

            } catch (IOException except) {
                throw new TableDataException("IO exception accessing column " + name, except);
            } catch (RuntimeException except) {
                throw new TableDataException("Unexpected exception accessing column " + name, except);
            }
        }, getMetadata);
    }

    @Override
    public @NotNull final ParquetColumnLocation<Attributes.Values> getColumnLocation(@NotNull CharSequence argName) {
        final String name = readInstructions.getParquetColumnNameFromColumnNameOrDefault(argName.toString());
        return super.getColumnLocation(name.subSequence(0, name.length()));
    }

    private static class LogicalTypeVisitor<ATTR extends Attributes.Any> implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ToPage<ATTR, ?>> {

        private final String name;
        private final ColumnChunkReader columnChunkReader;
        private final Class<?> componentType;

        LogicalTypeVisitor(@NotNull String name, @NotNull ColumnChunkReader columnChunkReader, Class<?> componentType) {
            this.name = name;
            this.columnChunkReader = columnChunkReader;
            this.componentType = componentType;
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            try {
                return Optional.of(ToStringPage.create(componentType, columnChunkReader.getDictionary()));
            } catch (IOException except) {
                throw new TableDataException("IO exception accessing string column " + name, except);
            }
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            if (timestampLogicalType.isAdjustedToUTC()) {
                return Optional.of(ToDBDateTimePage.create(componentType, timestampLogicalType.getUnit()));
            }

            throw new TableDataException("Timestamp column is not UTC or is not nanoseconds " + name);
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {

                if (intLogicalType.isSigned()) {
                    switch (intLogicalType.getBitWidth()) {
                        case 8:
                            return Optional.of(ToBytePageFromInt.create(componentType));
                        case 16:
                            return Optional.of(ToShortPageFromInt.create(componentType));
                        case 32:
                            return Optional.of(ToIntPage.create(componentType));
                        case 64:
                            return Optional.of(ToLongPage.create(componentType));
                    }
                } else {
                    switch (intLogicalType.getBitWidth()) {
                        case 8:
                        case 16:
                            return Optional.of(ToCharPageFromInt.create(componentType));
                        case 32:
                            return Optional.of(ToLongPage.create(componentType));
                    }
                }

            return Optional.empty();
        }
    }
}
