package io.deephaven.db.tables.libs;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

// from default_*_imports.txt
public class QueryLibraryImportsDefaults implements QueryLibraryImports {

    @Override
    public Set<Package> packages() {
        return new LinkedHashSet<>(Arrays.asList(
                Package.getPackage("java.lang"),
                Package.getPackage("java.util")));
    }

    @Override
    public Set<Class<?>> classes() {
        return new LinkedHashSet<>(Arrays.asList(
                java.lang.reflect.Array.class,
                io.deephaven.util.type.TypeUtils.class,
                io.deephaven.db.tables.Table.class,
                io.deephaven.db.tables.DataColumn.class,
                io.deephaven.db.tables.utils.ArrayUtils.class,
                io.deephaven.db.tables.utils.DBDateTime.class,
                io.deephaven.db.tables.utils.DBTimeUtils.class,
                io.deephaven.base.string.cache.CompressedString.class,
                java.util.Arrays.class,
                org.joda.time.LocalTime.class,
                io.deephaven.db.tables.utils.DBPeriod.class,
                io.deephaven.db.tables.select.Param.class,
                io.deephaven.db.v2.sources.ColumnSource.class,
                io.deephaven.db.v2.utils.Index.class,
                io.deephaven.db.v2.utils.IndexBuilder.class,
                io.deephaven.db.v2.utils.Index.SequentialBuilder.class,
                io.deephaven.db.util.LongSizedDataStructure.class,
                java.util.concurrent.ConcurrentHashMap.class,
                io.deephaven.db.v2.sources.chunk.Attributes.class,
                io.deephaven.db.v2.sources.chunk.Chunk.class,
                io.deephaven.db.v2.sources.chunk.ByteChunk.class,
                io.deephaven.db.v2.sources.chunk.CharChunk.class,
                io.deephaven.db.v2.sources.chunk.ShortChunk.class,
                io.deephaven.db.v2.sources.chunk.IntChunk.class,
                io.deephaven.db.v2.sources.chunk.LongChunk.class,
                io.deephaven.db.v2.sources.chunk.FloatChunk.class,
                io.deephaven.db.v2.sources.chunk.DoubleChunk.class,
                io.deephaven.db.v2.sources.chunk.ObjectChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableByteChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableCharChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableShortChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableIntChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableLongChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableFloatChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableDoubleChunk.class,
                io.deephaven.db.v2.sources.chunk.WritableObjectChunk.class,
                io.deephaven.db.v2.sources.chunk.Context.class,
                io.deephaven.db.v2.select.ConditionFilter.FilterKernel.class,
                io.deephaven.db.v2.utils.OrderedKeys.class));
    }

    @Override
    public Set<Class<?>> statics() {
        return new LinkedHashSet<>(Arrays.asList(
                io.deephaven.util.QueryConstants.class,
                io.deephaven.libs.primitives.BytePrimitives.class,
                io.deephaven.libs.primitives.ByteNumericPrimitives.class,
                io.deephaven.libs.primitives.CharacterPrimitives.class,
                io.deephaven.libs.primitives.DoublePrimitives.class,
                io.deephaven.libs.primitives.DoubleNumericPrimitives.class,
                io.deephaven.libs.primitives.DoubleFpPrimitives.class,
                io.deephaven.libs.primitives.FloatPrimitives.class,
                io.deephaven.libs.primitives.FloatFpPrimitives.class,
                io.deephaven.libs.primitives.FloatNumericPrimitives.class,
                io.deephaven.libs.primitives.IntegerPrimitives.class,
                io.deephaven.libs.primitives.IntegerNumericPrimitives.class,
                io.deephaven.libs.primitives.ShortPrimitives.class,
                io.deephaven.libs.primitives.ShortNumericPrimitives.class,
                io.deephaven.libs.primitives.LongPrimitives.class,
                io.deephaven.libs.primitives.LongNumericPrimitives.class,
                io.deephaven.libs.primitives.ObjectPrimitives.class,
                io.deephaven.libs.primitives.BooleanPrimitives.class,
                io.deephaven.libs.primitives.ComparePrimitives.class,
                io.deephaven.libs.primitives.BinSearch.class,
                io.deephaven.libs.primitives.Casting.class,
                io.deephaven.libs.primitives.PrimitiveParseUtil.class,
                io.deephaven.db.tables.lang.DBLanguageFunctionUtil.class,
                io.deephaven.db.tables.utils.DBTimeUtils.class,
                io.deephaven.db.tables.utils.DBTimeZone.class,
                io.deephaven.base.string.cache.CompressedString.class,
                io.deephaven.db.tables.utils.WhereClause.class,
                io.deephaven.gui.color.Color.class,
                io.deephaven.db.util.DBColorUtilImpl.class,
                io.deephaven.db.tables.verify.TableAssertions.class,
                io.deephaven.util.calendar.StaticCalendarMethods.class,
                io.deephaven.db.v2.sources.chunk.Attributes.class));
    }
}
