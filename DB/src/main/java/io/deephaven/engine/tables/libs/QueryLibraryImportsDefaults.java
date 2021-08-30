package io.deephaven.engine.tables.libs;

import io.deephaven.engine.structures.rowsequence.OrderedKeys;
import io.deephaven.engine.structures.rowset.Index;
import io.deephaven.engine.structures.rowset.IndexBuilder;
import io.deephaven.engine.structures.util.ArrayUtils;
import io.deephaven.engine.structures.util.LongSizedDataStructure;

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
                io.deephaven.engine.tables.Table.class,
                io.deephaven.engine.tables.DataColumn.class,
                ArrayUtils.class,
                io.deephaven.engine.tables.utils.DBDateTime.class,
                io.deephaven.engine.tables.utils.DBTimeUtils.class,
                io.deephaven.base.string.cache.CompressedString.class,
                java.util.Arrays.class,
                org.joda.time.LocalTime.class,
                io.deephaven.engine.tables.utils.DBPeriod.class,
                io.deephaven.engine.tables.select.Param.class,
                io.deephaven.engine.v2.sources.ColumnSource.class,
                Index.class,
                IndexBuilder.class,
                Index.SequentialBuilder.class,
                LongSizedDataStructure.class,
                java.util.concurrent.ConcurrentHashMap.class,
                io.deephaven.engine.structures.chunk.Attributes.class,
                io.deephaven.engine.structures.chunk.Chunk.class,
                io.deephaven.engine.structures.chunk.ByteChunk.class,
                io.deephaven.engine.structures.chunk.CharChunk.class,
                io.deephaven.engine.structures.chunk.ShortChunk.class,
                io.deephaven.engine.structures.chunk.IntChunk.class,
                io.deephaven.engine.structures.chunk.LongChunk.class,
                io.deephaven.engine.structures.chunk.FloatChunk.class,
                io.deephaven.engine.structures.chunk.DoubleChunk.class,
                io.deephaven.engine.structures.chunk.ObjectChunk.class,
                io.deephaven.engine.structures.chunk.WritableChunk.class,
                io.deephaven.engine.structures.chunk.WritableByteChunk.class,
                io.deephaven.engine.structures.chunk.WritableCharChunk.class,
                io.deephaven.engine.structures.chunk.WritableShortChunk.class,
                io.deephaven.engine.structures.chunk.WritableIntChunk.class,
                io.deephaven.engine.structures.chunk.WritableLongChunk.class,
                io.deephaven.engine.structures.chunk.WritableFloatChunk.class,
                io.deephaven.engine.structures.chunk.WritableDoubleChunk.class,
                io.deephaven.engine.structures.chunk.WritableObjectChunk.class,
                io.deephaven.engine.structures.chunk.Context.class,
                io.deephaven.engine.v2.select.ConditionFilter.FilterKernel.class,
                OrderedKeys.class));
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
                io.deephaven.engine.tables.lang.DBLanguageFunctionUtil.class,
                io.deephaven.engine.tables.utils.DBTimeUtils.class,
                io.deephaven.engine.tables.utils.DBTimeZone.class,
                io.deephaven.base.string.cache.CompressedString.class,
                io.deephaven.engine.tables.utils.WhereClause.class,
                io.deephaven.gui.color.Color.class,
                io.deephaven.engine.util.DBColorUtilImpl.class,
                io.deephaven.engine.tables.verify.TableAssertions.class,
                io.deephaven.util.calendar.StaticCalendarMethods.class,
                io.deephaven.engine.structures.chunk.Attributes.class));
    }
}
