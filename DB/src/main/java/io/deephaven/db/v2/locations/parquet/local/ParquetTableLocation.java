package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.CodecLookup;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.util.file.TrackedFileHandleFactory;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.impl.AbstractTableLocation;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.locations.parquet.topage.*;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.RowGroupReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private final File parquetFile;
    private final ParquetInstructions readInstructions;

    private final Map<String, String> keyValueMetaData;
    private final RowGroupReader[] rowGroupReaders;
    private final Map<String, String[]> parquetColumnNameToPath;

    private final Set<String> groupingParquetColumnNames = new HashSet<>();

    private final SeekableChannelsProvider cachedChannelProvider = new CachedChannelProvider(new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance()),
            Configuration.getInstance().getIntegerForClassWithDefault(ParquetTableLocation.class, "maxChannels", 100));

    ParquetTableLocation(@NotNull final TableKey tableKey,
                         @NotNull final TableLocationKey tableLocationKey,
                         final File parquetFile,
                         final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, false);
        this.readInstructions = readInstructions;
        this.parquetFile = parquetFile;
        int totalRows = 0;
        try {
            ParquetFileReader parquetFileReader = new ParquetFileReader(parquetFile.getPath(), cachedChannelProvider, -1);

            final int rowGroupCount = parquetFileReader.fileMetaData.getRow_groups().size();
            rowGroupReaders = new RowGroupReader[rowGroupCount];
            for (int rgi = 0; rgi < rowGroupCount; ++rgi) {
                final RowGroupReader reader = parquetFileReader.getRowGroup(rgi);
                rowGroupReaders[rgi] = reader;
                totalRows += reader.numRows();
            }

            parquetColumnNameToPath = new HashMap<>();
            for (final ColumnDescriptor column : parquetFileReader.getSchema().getColumns()) {
                final String[] path = column.getPath();
                if (path.length > 1) {
                    parquetColumnNameToPath.put(path[0], path);
                }
            }

            keyValueMetaData = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData).getFileMetaData().getKeyValueMetaData();
            String grouping = keyValueMetaData.get(ParquetTableWriter.GROUPING);
            if (grouping != null) {
                groupingParquetColumnNames.addAll(Arrays.asList(grouping.split(",")));
            }
        } catch (IOException e) {
            throw new TableDataException("Can't read parquet file " + parquetFile, e);
        }

        handleUpdate(totalRows, parquetFile.lastModified());
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {
    }

    File getParquetFile() {
        return parquetFile;
    }

    SeekableChannelsProvider getChannelProvider() {
        return cachedChannelProvider;
    }

    @Override
    public @NotNull
    final ColumnLocation getColumnLocation(@NotNull final CharSequence columnName) {
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName.toString());
        return super.getColumnLocation(parquetColumnName);
    }

    @NotNull
    @Override
    protected ParquetColumnLocation<Values> makeColumnLocation(@NotNull final String parquetColumnName) {
        // NB: We have the *parquet* column name here, because we overload getColumnLocation to remap its input
        //noinspection unchecked
        final ColumnChunkPageStore.Creator<Values>[] creators = new ColumnChunkPageStore.Creator[rowGroupReaders.length];
        int nullCreatorCount = 0;
        for (int rgi = 0; rgi < rowGroupReaders.length; ++rgi) {
            nullCreatorCount += (creators[rgi] = makeColumnCreator(parquetColumnName, rowGroupReaders[rgi], keyValueMetaData)) == null ? 1 : 0;
        }
        // NB: We cannot have column chunks in only *some* row groups within a file. At least, StackOverflow and Wes seem
        //     to think we can't, and that's good enough for me. See:
        //     https://stackoverflow.com/questions/57564621/can-we-have-different-schema-per-row-group-in-the-same-parquet-file
        if (nullCreatorCount != 0 && nullCreatorCount != creators.length) {
            throw new TableDataException("Inconsistent existence in " + parquetFile + " for column " + parquetColumnName);
        }
        return new ParquetColumnLocation<>(this, parquetColumnName, nullCreatorCount > 0 ? null : creators, groupingParquetColumnNames.contains(parquetColumnName));
    }

    <ATTR extends Any> ColumnChunkPageStore.Creator<ATTR> makeColumnCreator(
            @NotNull final String parquetColumnName,
            @NotNull final RowGroupReader rowGroupReader,
            @NotNull final Map<String, String> keyValueMetaData) {
        final String[] nameList = parquetColumnNameToPath.get(parquetColumnName);
        final ColumnChunkReader columnChunkReader = rowGroupReader.getColumnChunk(
                nameList == null ? Collections.singletonList(parquetColumnName) : Arrays.asList(nameList));
        if (columnChunkReader == null) {
            return null;
        }
        return ColumnChunkPageStore.makeCreator(columnChunkReader, (@NotNull ColumnDefinition columnDefinition) -> {
            final PrimitiveType type = columnChunkReader.getType();
            final LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            final String codecFromInstructions = readInstructions.getCodecName(columnDefinition.getName());
            final String codecName = (codecFromInstructions != null)
                    ? codecFromInstructions
                    : keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + parquetColumnName);
            final String specialTypeName = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + parquetColumnName);

            final boolean isArray = columnChunkReader.getMaxRl() > 0;
            final boolean isCodec = CodecLookup.explicitCodecPresent(codecName);

            if (isArray && columnChunkReader.getMaxRl() > 1) {
                throw new TableDataException("No support for nested repeated parquet columns.");
            }

            try {
                // Note that componentType is null for a StringSet. ToStringSetPage.create specifically doesn't take this parameter.
                final Class<?> dataType = columnDefinition.getDataType();
                final Class<?> componentType = columnDefinition.getComponentType();
                final Class<?> pageType = isArray ? componentType : dataType;

                ToPage<ATTR, ?> toPage = null;

                if (logicalTypeAnnotation != null) {
                    toPage = logicalTypeAnnotation.accept(new LogicalTypeVisitor<ATTR>(parquetColumnName, columnChunkReader, pageType)).orElse(null);
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
                                        : keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + parquetColumnName);
                                codec = CodecCache.DEFAULT.getCodec(codecName, codecArgs);
                            } else {
                                final String codecArgs = (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                        ? Integer.toString(type.getTypeLength())
                                        : null;
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
                throw new TableDataException("IO exception accessing column " + parquetColumnName, except);
            } catch (RuntimeException except) {
                throw new TableDataException("Unexpected exception accessing column " + parquetColumnName, except);
            }
        });
    }

    private static class LogicalTypeVisitor<ATTR extends Any> implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ToPage<ATTR, ?>> {

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
