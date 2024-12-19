//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.lang.impl;

import com.google.auto.service.AutoService;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.context.QueryLibraryImports;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.verify.TableAssertions;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.util.ColorUtilImpl;
import io.deephaven.function.*;
import io.deephaven.gui.color.Color;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.time.calendar.StaticCalendarMethods;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.VectorConversions;

import java.lang.reflect.Array;
import java.time.*;
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
                ArrayTypeUtils.class,
                VectorConversions.class,
                DateTimeUtils.class,
                CompressedString.class,
                java.util.Arrays.class,
                LocalDate.class,
                LocalTime.class,
                Instant.class,
                ZonedDateTime.class,
                Duration.class,
                Period.class,
                QueryScopeParam.class,
                ColumnSource.class,
                ColumnVectors.class,
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
                BooleanChunk.class,
                ByteChunk.class,
                CharChunk.class,
                ShortChunk.class,
                IntChunk.class,
                LongChunk.class,
                FloatChunk.class,
                DoubleChunk.class,
                ObjectChunk.class,
                WritableChunk.class,
                WritableBooleanChunk.class,
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
                CompressedString.class,
                Color.class,
                ColorUtilImpl.class,
                TableAssertions.class,
                Calendars.class,
                StaticCalendarMethods.class));
    }
}
