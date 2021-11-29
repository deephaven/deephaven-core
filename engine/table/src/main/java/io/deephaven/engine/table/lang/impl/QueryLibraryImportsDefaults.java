package io.deephaven.engine.table.lang.impl;

import io.deephaven.function.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.lang.QueryLibraryImports;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.VectorConversions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.Context;
import io.deephaven.time.DateTime;
import io.deephaven.time.Period;
import io.deephaven.time.TimeZone;
import io.deephaven.engine.util.ColorUtilImpl;
import io.deephaven.util.type.ArrayTypeUtils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class QueryLibraryImportsDefaults implements QueryLibraryImports {

    @Override
    public Set<Package> packages() {
        final ClassLoader classLoader = getClass().getClassLoader();
        return new LinkedHashSet<>(Arrays.asList(
                classLoader.getDefinedPackage("java.lang"),
                classLoader.getDefinedPackage("java.util"),
                classLoader.getDefinedPackage("io.deephaven.chunk.attributes"),
                classLoader.getDefinedPackage("io.deephaven.engine.rowset.chunkattributes")
        ));
    }

    @Override
    public Set<Class<?>> classes() {
        return new LinkedHashSet<>(Arrays.asList(
                java.lang.reflect.Array.class,
                io.deephaven.util.type.TypeUtils.class,
                Table.class,
                DataColumn.class,
                ArrayTypeUtils.class,
                VectorConversions.class,
                DateTime.class,
                DateTimeUtils.class,
                io.deephaven.base.string.cache.CompressedString.class,
                java.util.Arrays.class,
                org.joda.time.LocalTime.class,
                Period.class,
                QueryScopeParam.class,
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
                io.deephaven.chunk.Chunk.class,
                io.deephaven.chunk.ByteChunk.class,
                io.deephaven.chunk.CharChunk.class,
                io.deephaven.chunk.ShortChunk.class,
                io.deephaven.chunk.IntChunk.class,
                io.deephaven.chunk.LongChunk.class,
                io.deephaven.chunk.FloatChunk.class,
                io.deephaven.chunk.DoubleChunk.class,
                io.deephaven.chunk.ObjectChunk.class,
                io.deephaven.chunk.WritableChunk.class,
                io.deephaven.chunk.WritableByteChunk.class,
                io.deephaven.chunk.WritableCharChunk.class,
                io.deephaven.chunk.WritableShortChunk.class,
                io.deephaven.chunk.WritableIntChunk.class,
                io.deephaven.chunk.WritableLongChunk.class,
                io.deephaven.chunk.WritableFloatChunk.class,
                io.deephaven.chunk.WritableDoubleChunk.class,
                io.deephaven.chunk.WritableObjectChunk.class,
                Context.class,
                io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel.class,
                RowSequence.class
        ));
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
                QueryLanguageFunctionUtils.class,
                DateTimeUtils.class,
                TimeZone.class,
                io.deephaven.base.string.cache.CompressedString.class,
                io.deephaven.gui.color.Color.class,
                ColorUtilImpl.class,
                io.deephaven.engine.table.impl.verify.TableAssertions.class,
                io.deephaven.time.calendar.StaticCalendarMethods.class
        ));
    }
}
