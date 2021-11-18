package io.deephaven.engine.tables.libs;

import io.deephaven.engine.function.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.tables.lang.LanguageFunctionUtil;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.time.DateTimeUtils;
import io.deephaven.engine.time.Period;
import io.deephaven.engine.time.TimeZone;
import io.deephaven.engine.util.ColorUtilImpl;
import io.deephaven.util.type.ArrayTypeUtils;

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
                Table.class,
                DataColumn.class,
                ArrayTypeUtils.class,
                DateTime.class,
                DateTimeUtils.class,
                io.deephaven.base.string.cache.CompressedString.class,
                java.util.Arrays.class,
                org.joda.time.LocalTime.class,
                Period.class,
                io.deephaven.engine.tables.select.Param.class,
                ColumnSource.class,
                RowSet.class,
                WritableRowSet.class,
                TrackingRowSet.class,
                TrackingWritableRowSet.class,
                RowSetFactory.class,
                RowSetBuilderRandom.class,
                RowSetBuilderSequential.class,
                LongSizedDataStructure.class,
                java.util.concurrent.ConcurrentHashMap.class,
                io.deephaven.engine.chunk.Attributes.class,
                io.deephaven.engine.chunk.Chunk.class,
                io.deephaven.engine.chunk.ByteChunk.class,
                io.deephaven.engine.chunk.CharChunk.class,
                io.deephaven.engine.chunk.ShortChunk.class,
                io.deephaven.engine.chunk.IntChunk.class,
                io.deephaven.engine.chunk.LongChunk.class,
                io.deephaven.engine.chunk.FloatChunk.class,
                io.deephaven.engine.chunk.DoubleChunk.class,
                io.deephaven.engine.chunk.ObjectChunk.class,
                io.deephaven.engine.chunk.WritableChunk.class,
                io.deephaven.engine.chunk.WritableByteChunk.class,
                io.deephaven.engine.chunk.WritableCharChunk.class,
                io.deephaven.engine.chunk.WritableShortChunk.class,
                io.deephaven.engine.chunk.WritableIntChunk.class,
                io.deephaven.engine.chunk.WritableLongChunk.class,
                io.deephaven.engine.chunk.WritableFloatChunk.class,
                io.deephaven.engine.chunk.WritableDoubleChunk.class,
                io.deephaven.engine.chunk.WritableObjectChunk.class,
                Context.class,
                io.deephaven.engine.v2.select.ConditionFilter.FilterKernel.class,
                RowSequence.class));
    }

    @Override
    public Set<Class<?>> statics() {
        return new LinkedHashSet<>(Arrays.asList(
                io.deephaven.util.QueryConstants.class,
                BytePrimitives.class,
                ByteNumericPrimitives.class,
                CharacterPrimitives.class,
                DoublePrimitives.class,
                DoubleNumericPrimitives.class,
                DoubleFpPrimitives.class,
                FloatPrimitives.class,
                FloatFpPrimitives.class,
                FloatNumericPrimitives.class,
                IntegerPrimitives.class,
                IntegerNumericPrimitives.class,
                ShortPrimitives.class,
                ShortNumericPrimitives.class,
                LongPrimitives.class,
                LongNumericPrimitives.class,
                ObjectPrimitives.class,
                BooleanPrimitives.class,
                ComparePrimitives.class,
                BinSearch.class,
                Casting.class,
                PrimitiveParseUtil.class,
                LanguageFunctionUtil.class,
                DateTimeUtils.class,
                TimeZone.class,
                io.deephaven.base.string.cache.CompressedString.class,
                io.deephaven.gui.color.Color.class,
                ColorUtilImpl.class,
                io.deephaven.engine.tables.verify.TableAssertions.class,
                io.deephaven.engine.time.calendar.StaticCalendarMethods.class,
                io.deephaven.engine.chunk.Attributes.class));
    }
}
