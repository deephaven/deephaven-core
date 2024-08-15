//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.AbstractColumnLocation;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.parquet.base.BigDecimalParquetBytesCodec;
import io.deephaven.parquet.base.BigIntegerParquetBytesCodec;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.metadata.CodecInfo;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.parquet.table.pagestore.PageCache;
import io.deephaven.parquet.table.pagestore.topage.*;
import io.deephaven.parquet.table.region.*;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import io.deephaven.vector.Vector;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.parquet.base.BigDecimalParquetBytesCodec.verifyPrecisionAndScale;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

final class ParquetColumnLocation<ATTR extends Values> extends AbstractColumnLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private static final int INITIAL_PAGE_CACHE_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(ParquetColumnLocation.class, "initialPageCacheSize", 128);
    private static final int MAX_PAGE_CACHE_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(ParquetColumnLocation.class, "maxPageCacheSize", 8192);

    private final String parquetColumnName;
    /**
     * Factory object needed for deferred initialization of the remaining fields. Reference serves as a barrier to
     * ensure visibility of the derived fields.
     */
    private volatile ColumnChunkReader[] columnChunkReaders;

    // We should consider moving this to column level if needed. Column-location level likely allows more parallelism.
    private volatile PageCache<ATTR> pageCache;

    private ColumnChunkPageStore<ATTR>[] pageStores;
    private Supplier<Chunk<ATTR>>[] dictionaryChunkSuppliers;
    private ColumnChunkPageStore<DictionaryKeys>[] dictionaryKeysPageStores;

    /**
     * Construct a new {@link ParquetColumnLocation} for the specified {@link ParquetTableLocation} and column name.
     *
     * @param tableLocation The table location enclosing this column location
     * @param parquetColumnName The Parquet file column name
     * @param columnChunkReaders The {@link ColumnChunkReader column chunk readers} for this location
     */
    ParquetColumnLocation(
            @NotNull final ParquetTableLocation tableLocation,
            @NotNull final String columnName,
            @NotNull final String parquetColumnName,
            @Nullable final ColumnChunkReader[] columnChunkReaders) {
        super(tableLocation, columnName);
        this.parquetColumnName = parquetColumnName;
        this.columnChunkReaders = columnChunkReaders;
    }

    private PageCache<ATTR> ensurePageCache() {
        PageCache<ATTR> localPageCache;
        if ((localPageCache = pageCache) != null) {
            return localPageCache;
        }

        synchronized (this) {
            if ((localPageCache = pageCache) != null) {
                return localPageCache;
            }
            return pageCache = new PageCache<>(INITIAL_PAGE_CACHE_SIZE, MAX_PAGE_CACHE_SIZE);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // AbstractColumnLocation implementation
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public boolean exists() {
        // If we see a null columnChunkReaders array, either we don't exist or we are guaranteed to
        // see a non-null
        // pageStores array
        return columnChunkReaders != null || pageStores != null;
    }

    private ParquetTableLocation tl() {
        return (ParquetTableLocation) getTableLocation();
    }

    private <SOURCE, REGION_TYPE> REGION_TYPE makeColumnRegion(
            @NotNull final Function<ColumnDefinition<?>, SOURCE[]> sourceArrayFactory,
            @NotNull final ColumnDefinition<?> columnDefinition,
            @NotNull final LongFunction<REGION_TYPE> nullRegionFactory,
            @NotNull final Function<SOURCE, REGION_TYPE> singleRegionFactory,
            @NotNull final Function<Stream<REGION_TYPE>, REGION_TYPE> multiRegionFactory) {
        final SOURCE[] sources = sourceArrayFactory.apply(columnDefinition);
        return sources.length == 1
                ? makeSingleColumnRegion(sources[0], nullRegionFactory, singleRegionFactory)
                : multiRegionFactory.apply(Arrays.stream(sources).map(
                        source -> makeSingleColumnRegion(source, nullRegionFactory, singleRegionFactory)));
    }

    private <SOURCE, REGION_TYPE> REGION_TYPE makeSingleColumnRegion(final SOURCE source,
            @NotNull final LongFunction<REGION_TYPE> nullRegionFactory,
            @NotNull final Function<SOURCE, REGION_TYPE> singleRegionFactory) {
        return source == null ? nullRegionFactory.apply(tl().getRegionParameters().regionMask)
                : singleRegionFactory.apply(source);
    }

    @Override
    public ColumnRegionChar<Values> makeColumnRegionChar(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionChar<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionChar::createNull, ParquetColumnRegionChar::new,
                rs -> new ColumnRegionChar.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionChar[]::new)));
    }

    @Override
    public ColumnRegionByte<Values> makeColumnRegionByte(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionByte<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionByte::createNull, ParquetColumnRegionByte::new,
                rs -> new ColumnRegionByte.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionByte[]::new)));
    }

    @Override
    public ColumnRegionShort<Values> makeColumnRegionShort(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionShort<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionShort::createNull, ParquetColumnRegionShort::new,
                rs -> new ColumnRegionShort.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionShort[]::new)));
    }

    @Override
    public ColumnRegionInt<Values> makeColumnRegionInt(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionInt<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionInt::createNull, ParquetColumnRegionInt::new,
                rs -> new ColumnRegionInt.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionInt[]::new)));
    }

    @Override
    public ColumnRegionLong<Values> makeColumnRegionLong(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionLong<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionLong::createNull, ParquetColumnRegionLong::new,
                rs -> new ColumnRegionLong.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionLong[]::new)));
    }

    @Override
    public ColumnRegionFloat<Values> makeColumnRegionFloat(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionFloat<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionFloat::createNull, ParquetColumnRegionFloat::new,
                rs -> new ColumnRegionFloat.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionFloat[]::new)));
    }

    @Override
    public ColumnRegionDouble<Values> makeColumnRegionDouble(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        return (ColumnRegionDouble<Values>) makeColumnRegion(this::getPageStores, columnDefinition,
                ColumnRegionDouble::createNull, ParquetColumnRegionDouble::new,
                rs -> new ColumnRegionDouble.StaticPageStore<>(tl().getRegionParameters(),
                        rs.toArray(ColumnRegionDouble[]::new)));
    }

    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(
            @NotNull final ColumnDefinition<TYPE> columnDefinition) {
        final Class<TYPE> dataType = columnDefinition.getDataType();
        final ColumnChunkPageStore<ATTR>[] sources = getPageStores(columnDefinition);
        final ColumnChunkPageStore<DictionaryKeys>[] dictKeySources =
                getDictionaryKeysPageStores(columnDefinition);
        final Supplier<Chunk<ATTR>>[] dictionaryChunkSuppliers =
                getDictionaryChunkSuppliers(columnDefinition);
        if (sources.length == 1) {
            // noinspection unchecked
            return (ColumnRegionObject<TYPE, Values>) makeSingleColumnRegionObject(dataType,
                    sources[0], dictKeySources[0], dictionaryChunkSuppliers[0]);
        }
        // noinspection unchecked
        return (ColumnRegionObject<TYPE, Values>) new ColumnRegionObject.StaticPageStore<TYPE, ATTR>(
                tl().getRegionParameters(),
                IntStream.range(0, sources.length)
                        .mapToObj(ri -> makeSingleColumnRegionObject(dataType, sources[ri],
                                dictKeySources[ri], dictionaryChunkSuppliers[ri]))
                        .toArray(ColumnRegionObject[]::new));
    }

    private <TYPE> ColumnRegionObject<TYPE, ATTR> makeSingleColumnRegionObject(
            @NotNull final Class<TYPE> dataType,
            @Nullable final ColumnChunkPageStore<ATTR> source,
            @Nullable final ColumnChunkPageStore<DictionaryKeys> dictKeySource,
            @Nullable final Supplier<Chunk<ATTR>> dictValuesSupplier) {
        if (source == null) {
            return ColumnRegionObject.createNull(tl().getRegionParameters().regionMask);
        }
        return new ParquetColumnRegionObject<>(source,
                () -> new ParquetColumnRegionLong<>(Require.neqNull(dictKeySource, "dictKeySource")),
                () -> ColumnRegionChunkDictionary.create(tl().getRegionParameters().regionMask,
                        dataType, Require.neqNull(dictValuesSupplier, "dictValuesSupplier")));
    }

    /**
     * Get the {@link ColumnChunkPageStore page stores} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page stores
     */
    @NotNull
    public ColumnChunkPageStore<ATTR>[] getPageStores(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return pageStores;
    }

    /**
     * Get suppliers to access the {@link Chunk dictionary chunks} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The dictionary values chunk suppliers, or null if none exist
     */
    public Supplier<Chunk<ATTR>>[] getDictionaryChunkSuppliers(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryChunkSuppliers;
    }

    /**
     * Get the {@link ColumnChunkPageStore page stores} backing the indices for this column location. Only usable when
     * there are dictionaries.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page stores
     */
    private ColumnChunkPageStore<DictionaryKeys>[] getDictionaryKeysPageStores(
            @NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryKeysPageStores;
    }

    @SuppressWarnings("unchecked")
    private void fetchValues(@NotNull final ColumnDefinition<?> columnDefinition) {
        if (columnChunkReaders == null) {
            return;
        }
        synchronized (this) {
            if (columnChunkReaders == null) {
                return;
            }

            final int pageStoreCount = columnChunkReaders.length;
            pageStores = new ColumnChunkPageStore[pageStoreCount];
            dictionaryChunkSuppliers = new Supplier[pageStoreCount];
            dictionaryKeysPageStores = new ColumnChunkPageStore[pageStoreCount];
            for (int psi = 0; psi < pageStoreCount; ++psi) {
                final ColumnChunkReader columnChunkReader = columnChunkReaders[psi];
                try {
                    final ColumnChunkPageStore.CreatorResult<ATTR> creatorResult =
                            ColumnChunkPageStore.create(
                                    ensurePageCache(),
                                    columnChunkReader,
                                    tl().getRegionParameters().regionMask,
                                    makeToPage(tl().getColumnTypes().get(parquetColumnName),
                                            tl().getReadInstructions(), parquetColumnName, columnChunkReader,
                                            columnDefinition),
                                    columnDefinition);
                    pageStores[psi] = creatorResult.pageStore;
                    dictionaryChunkSuppliers[psi] = creatorResult.dictionaryChunkSupplier;
                    dictionaryKeysPageStores[psi] = creatorResult.dictionaryKeysPageStore;
                } catch (IOException e) {
                    throw new TableDataException(
                            "Failed to read parquet file for " + this + ", row group " + psi, e);
                }
            }

            columnChunkReaders = null;
        }
    }

    private static <ATTR extends Any, RESULT> ToPage<ATTR, RESULT> makeToPage(
            @Nullable final ColumnTypeInfo columnTypeInfo,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final String parquetColumnName,
            @NotNull final ColumnChunkReader columnChunkReader,
            @NotNull final ColumnDefinition<?> columnDefinition) {
        final PrimitiveType type = columnChunkReader.getType();
        final LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        final String codecFromInstructions =
                readInstructions.getCodecName(columnDefinition.getName());
        final String codecName = (codecFromInstructions != null)
                ? codecFromInstructions
                : columnTypeInfo == null ? null
                        : columnTypeInfo.codec().map(CodecInfo::codecName).orElse(null);
        final ColumnTypeInfo.SpecialType specialTypeName =
                columnTypeInfo == null ? null : columnTypeInfo.specialType().orElse(null);

        final boolean isArray = columnChunkReader.getMaxRl() > 0;
        final boolean isCodec = CodecLookup.explicitCodecPresent(codecName);

        if (isArray && columnChunkReader.getMaxRl() > 1) {
            throw new TableDataException("No support for nested repeated parquet columns.");
        }

        try {
            // Note that componentType is null for a StringSet. ToStringSetPage.create specifically
            // doesn't take this parameter.
            final Class<?> dataType = columnDefinition.getDataType();
            final Class<?> componentType = columnDefinition.getComponentType();
            final Class<?> pageType = isArray ? componentType : dataType;

            ToPage<ATTR, ?> toPage = null;

            if (!isCodec && logicalTypeAnnotation != null) {
                toPage = logicalTypeAnnotation.accept(
                        new LogicalTypeVisitor<ATTR>(parquetColumnName, columnChunkReader, pageType))
                        .orElse(null);
            }

            if (toPage == null) {
                final PrimitiveType.PrimitiveTypeName typeName = type.getPrimitiveTypeName();
                switch (typeName) {
                    case BOOLEAN:
                        if (pageType == Boolean.class) {
                            toPage = ToBooleanAsBytePage.create(pageType);
                        } else if (pageType == byte.class) {
                            toPage = ToBytePage.createFromBoolean(pageType);
                        } else if (pageType == short.class) {
                            toPage = ToShortPage.createFromBoolean(pageType);
                        } else if (pageType == int.class) {
                            toPage = ToIntPage.createFromBoolean(pageType);
                        } else if (pageType == long.class) {
                            toPage = ToLongPage.createFromBoolean(pageType);
                        } else {
                            throw new IllegalArgumentException(
                                    "Cannot convert parquet BOOLEAN primitive column to " + pageType);
                        }
                        break;
                    case INT32:
                        if (pageType == int.class) {
                            toPage = ToIntPage.create(pageType);
                        } else if (pageType == long.class) {
                            toPage = ToLongPage.createFromInt(pageType);
                        } else {
                            throw new IllegalArgumentException("Cannot convert parquet INT32 column to " + pageType);
                        }
                        break;
                    case INT64:
                        if (pageType == long.class) {
                            toPage = ToLongPage.create(pageType);
                        } else {
                            throw new IllegalArgumentException("Cannot convert parquet INT64 column to " + pageType);
                        }
                        break;
                    case INT96:
                        if (pageType == Instant.class) {
                            toPage = ToInstantPage.createFromInt96(pageType);
                        } else {
                            throw new IllegalArgumentException("Cannot convert parquet INT96 column to " + pageType);
                        }
                        break;
                    case DOUBLE:
                        if (pageType == double.class) {
                            toPage = ToDoublePage.create(pageType);
                        } else {
                            throw new IllegalArgumentException("Cannot convert parquet DOUBLE column to " + pageType);
                        }
                        break;
                    case FLOAT:
                        if (pageType == float.class) {
                            toPage = ToFloatPage.create(pageType);
                        } else if (pageType == double.class) {
                            toPage = ToDoublePage.createFromFloat(pageType);
                        } else {
                            throw new IllegalArgumentException("Cannot convert parquet FLOAT column to " + pageType);
                        }
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        // noinspection rawtypes
                        final ObjectCodec codec;
                        if (isCodec) {
                            final String codecArgs = codecFromInstructions != null
                                    ? readInstructions.getCodecArgs(columnDefinition.getName())
                                    : columnTypeInfo.codec().flatMap(CodecInfo::codecArg).orElse(null);
                            codec = CodecCache.DEFAULT.getCodec(codecName, codecArgs);
                        } else {
                            final String codecArgs =
                                    (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                            ? Integer.toString(type.getTypeLength())
                                            : null;
                            codec = CodecCache.DEFAULT
                                    .getCodec(SimpleByteArrayCodec.class.getName(), codecArgs);
                        }
                        // noinspection unchecked
                        toPage = ToObjectPage.create(dataType, codec,
                                columnChunkReader.getDictionarySupplier());
                        break;
                    default:
                }
            }

            if (toPage == null) {
                throw new IllegalArgumentException(
                        "Unsupported parquet column type " + type.getPrimitiveTypeName() +
                                " with logical type " + logicalTypeAnnotation + " and page type " + pageType);
            }

            if (specialTypeName == ColumnTypeInfo.SpecialType.StringSet) {
                Assert.assertion(isArray, "isArray");
                toPage = ToStringSetPage.create(dataType, toPage);
            } else if (isArray) {
                Assert.assertion(!isCodec, "!isCodec");
                if (Vector.class.isAssignableFrom(dataType)) {
                    toPage = ToVectorPage.create(dataType, componentType, toPage);
                } else if (dataType.isArray()) {
                    toPage = ToArrayPage.create(dataType, componentType, toPage);
                }
            }

            // noinspection unchecked
            return (ToPage<ATTR, RESULT>) toPage;

        } catch (final RuntimeException except) {
            throw new TableDataException(
                    "Unexpected exception accessing column " + parquetColumnName, except);
        }
    }

    private static class LogicalTypeVisitor<ATTR extends Any>
            implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ToPage<ATTR, ?>> {

        private final String name;
        private final ColumnChunkReader columnChunkReader;
        private final Class<?> pageType;

        LogicalTypeVisitor(@NotNull final String name, @NotNull final ColumnChunkReader columnChunkReader,
                final Class<?> pageType) {
            this.name = name;
            this.columnChunkReader = columnChunkReader;
            this.pageType = pageType;
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(
                final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            return Optional.of(ToStringPage.create(pageType, columnChunkReader.getDictionarySupplier()));
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(
                final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            if (timestampLogicalType.isAdjustedToUTC()) {
                // The column will be stored as nanoseconds elapsed since epoch as long values
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return Optional.of(ToInstantPage.createFromMillis(pageType));
                    case MICROS:
                        return Optional.of(ToInstantPage.createFromMicros(pageType));
                    case NANOS:
                        return Optional.of(ToInstantPage.createFromNanos(pageType));
                    default:
                        throw new IllegalArgumentException("Unsupported unit=" + timestampLogicalType.getUnit());
                }
            }
            // The column will be stored as as LocalDateTime
            // Ref:https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#local-semantics-timestamps-not-normalized-to-utc
            switch (timestampLogicalType.getUnit()) {
                case MILLIS:
                    return Optional.of(ToLocalDateTimePage.createFromMillis(pageType));
                case MICROS:
                    return Optional.of(ToLocalDateTimePage.createFromMicros(pageType));
                case NANOS:
                    return Optional.of(ToLocalDateTimePage.createFromNanos(pageType));
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + timestampLogicalType.getUnit());
            }
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            if (intLogicalType.isSigned()) {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                        if (pageType == byte.class) {
                            return Optional.of(ToBytePage.create(pageType));
                        } else if (pageType == short.class) {
                            return Optional.of(ToShortPage.create(pageType));
                        } else if (pageType == int.class) {
                            return Optional.of(ToIntPage.create(pageType));
                        } else if (pageType == long.class) {
                            return Optional.of(ToLongPage.createFromInt(pageType));
                        }
                        throw new IllegalArgumentException("Cannot convert parquet byte column to " + pageType);
                    case 16:
                        if (pageType == short.class) {
                            return Optional.of(ToShortPage.create(pageType));
                        } else if (pageType == int.class) {
                            return Optional.of(ToIntPage.create(pageType));
                        } else if (pageType == long.class) {
                            return Optional.of(ToLongPage.createFromInt(pageType));
                        }
                        throw new IllegalArgumentException("Cannot convert parquet short column to " + pageType);
                    case 32:
                        if (pageType == int.class) {
                            return Optional.of(ToIntPage.create(pageType));
                        } else if (pageType == long.class) {
                            return Optional.of(ToLongPage.createFromInt(pageType));
                        }
                        throw new IllegalArgumentException("Cannot convert parquet int column to " + pageType);
                    case 64:
                        return Optional.of(ToLongPage.create(pageType));
                }
            } else {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                        if (pageType == char.class) {
                            return Optional.of(ToCharPage.create(pageType));
                        } else if (pageType == short.class) {
                            return Optional.of(ToShortPage.createFromUnsignedByte(pageType));
                        } else if (pageType == int.class) {
                            return Optional.of(ToIntPage.createFromUnsignedByte(pageType));
                        } else if (pageType == long.class) {
                            return Optional.of(ToLongPage.createFromUnsignedByte(pageType));
                        }
                        throw new IllegalArgumentException(
                                "Cannot convert parquet unsigned byte column to " + pageType);
                    case 16:
                        if (pageType == char.class) {
                            return Optional.of(ToCharPage.create(pageType));
                        } else if (pageType == int.class) {
                            return Optional.of(ToIntPage.createFromUnsignedShort(pageType));
                        } else if (pageType == long.class) {
                            return Optional.of(ToLongPage.createFromUnsignedShort(pageType));
                        }
                        throw new IllegalArgumentException(
                                "Cannot convert parquet unsigned short column to " + pageType);
                    case 32:
                        return Optional.of(ToLongPage.createFromUnsignedInt(pageType));
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(ToLocalDatePage.create(pageType));
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            // isAdjustedToUTC parameter is ignored while reading LocalTime from Parquet files
            switch (timeLogicalType.getUnit()) {
                case MILLIS:
                    return Optional.of(ToLocalTimePage.createFromMillis(pageType));
                case MICROS:
                    return Optional.of(ToLocalTimePage.createFromMicros(pageType));
                case NANOS:
                    return Optional.of(ToLocalTimePage.createFromNanos(pageType));
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + timeLogicalType.getUnit());
            }
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(
                final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            final PrimitiveType type = columnChunkReader.getType();
            final PrimitiveType.PrimitiveTypeName typeName = type.getPrimitiveTypeName();
            switch (typeName) {
                case INT32:
                    return Optional.of(ToBigDecimalFromNumeric.createFromInt(pageType, decimalLogicalType.getScale()));
                case INT64:
                    return Optional.of(ToBigDecimalFromNumeric.createFromLong(pageType, decimalLogicalType.getScale()));
                case FIXED_LEN_BYTE_ARRAY: // fall through
                case BINARY:
                    final int encodedSizeInBytes = typeName == BINARY ? -1 : type.getTypeLength();
                    if (pageType == BigDecimal.class) {
                        final int precision = decimalLogicalType.getPrecision();
                        final int scale = decimalLogicalType.getScale();
                        try {
                            verifyPrecisionAndScale(precision, scale, typeName);
                        } catch (final IllegalArgumentException exception) {
                            throw new TableDataException(
                                    "Invalid scale and precision for column " + name + ": " + exception.getMessage());
                        }
                        return Optional.of(ToBigDecimalPage.create(
                                pageType,
                                new BigDecimalParquetBytesCodec(precision, scale, encodedSizeInBytes),
                                columnChunkReader.getDictionarySupplier()));
                    } else if (pageType == BigInteger.class) {
                        return Optional.of(ToBigIntegerPage.create(
                                pageType,
                                new BigIntegerParquetBytesCodec(encodedSizeInBytes),
                                columnChunkReader.getDictionarySupplier()));
                    }
                    // We won't blow up here, Maybe someone will provide us a codec instead.
                default:
                    return Optional.empty();
            }
        }
    }
}
