package io.deephaven.engine.table.lang.impl;

import com.google.auto.service.AutoService;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.verify.TableAssertions;
import io.deephaven.engine.table.lang.QueryLibraryImports;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.engine.util.ColorUtilImpl;
import io.deephaven.function.Basic;
import io.deephaven.function.BinSearch;
import io.deephaven.function.BinSearchAlgo;
import io.deephaven.function.Cast;
import io.deephaven.function.Logic;
import io.deephaven.function.Numeric;
import io.deephaven.function.Parse;
import io.deephaven.function.Random;
import io.deephaven.function.Sort;
import io.deephaven.gui.color.Color;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.Period;
import io.deephaven.time.TimeZone;
import io.deephaven.time.calendar.StaticCalendarMethods;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.VectorConversions;
import org.joda.time.LocalTime;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@AutoService(QueryLibraryImports.class)
public class QueryLibraryImportsDefaults implements QueryLibraryImports {

    @Override
    public Set<Package> packages() {
        return new LinkedHashSet<>(Arrays.asList(
                Object.class.getPackage(),
                Arrays.class.getPackage(),
                Any.class.getPackage(),
                RowKeys.class.getPackage()));
    }

    @Override
    public Set<Class<?>> classes() {
        return new LinkedHashSet<>(Arrays.asList(
                Array.class,
                TypeUtils.class,
                Table.class,
                DataColumn.class,
                ArrayTypeUtils.class,
                VectorConversions.class,
                DateTime.class,
                DateTimeUtils.class,
                CompressedString.class,
                java.util.Arrays.class,
                LocalTime.class,
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
                ConcurrentHashMap.class,
                Chunk.class,
                ByteChunk.class,
                CharChunk.class,
                ShortChunk.class,
                IntChunk.class,
                LongChunk.class,
                FloatChunk.class,
                DoubleChunk.class,
                ObjectChunk.class,
                WritableChunk.class,
                WritableByteChunk.class,
                WritableCharChunk.class,
                WritableShortChunk.class,
                WritableIntChunk.class,
                WritableLongChunk.class,
                WritableFloatChunk.class,
                WritableDoubleChunk.class,
                WritableObjectChunk.class,
                Context.class,
                ConditionFilter.FilterKernel.class,
                RowSequence.class));
    }

    @Override
    public Set<Class<?>> statics() {
        return new LinkedHashSet<>(Arrays.asList(
                QueryConstants.class,
                Basic.class,
                BinSearch.class,
                BinSearchAlgo.class,
                Cast.class,
                Logic.class,
                Numeric.class,
                Parse.class,
                Random.class,
                Sort.class,
                QueryLanguageFunctionUtils.class,
                DateTimeUtils.class,
                TimeZone.class,
                CompressedString.class,
                Color.class,
                ColorUtilImpl.class,
                TableAssertions.class,
                StaticCalendarMethods.class));
    }
}
