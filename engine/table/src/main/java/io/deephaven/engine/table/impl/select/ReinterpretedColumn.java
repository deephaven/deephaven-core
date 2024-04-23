//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.engine.table.impl.sources.LocalDateWrapperSource;
import io.deephaven.engine.table.impl.sources.LocalTimeWrapperSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsLocalDateColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsLocalTimeColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsZonedDateTimeColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.TableTimeConversions;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Allows {@link ColumnSource} reinterpretation via view-type ({@link Table#view} and {@link Table#updateView})
 * {@link Table} operations.
 * <p>
 * TODO: If we come up with other valid, useful reinterpretations, it would be trivial to create a general purpose
 * syntax for use in view()/updateView() column expressions.
 * <p>
 * The syntax I have in mind is: "&lt;ColumnNameB&gt;=&lt;ColumnNameA&gt;.as(&lt;ClassName&gt;)"
 * "&lt;ColumnName&gt;.as(&lt;ClassName&gt;)"
 * <p>
 * Making this work would consist of any one of: 1. Adding a V1 version and updating SelectColumnFactory and
 * SelectColumnAdaptor 2. Adding the appropriate if-regex-matches to realColumn selection in V2 SwitchColumn 3. Creating
 * a V2-native SelectColumnFactory
 */
public class ReinterpretedColumn<S, D> implements SelectColumn {

    @NotNull
    private final String sourceName;
    @NotNull
    private final Class<S> sourceDataType;
    @NotNull
    private final String destName;
    @NotNull
    private final Class<D> destDataType;
    private final Object[] reinterpParams;
    private ColumnSource<S> sourceColumnSource;

    private ZoneId zone;

    /**
     * Create a {@link ReinterpretedColumn} that attempts to convert the source column into the destination type,
     * optionally with parameters.
     *
     * @param sourceName the name of the Source column within the table
     * @param sourceDataType the type of the source column
     * @param destName the name of the desired destination column
     * @param destDataType the type to try to convert to
     * @param reinterpParams a varargs set of parameters for the arguments if required.
     */
    public ReinterpretedColumn(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            Object... reinterpParams) {
        Assert.gtZero(destName.length(), "destName.length()");
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.sourceDataType = Require.neqNull(sourceDataType, "sourceDataType");
        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        this.reinterpParams = reinterpParams;
    }

    @Override
    public String toString() {
        return "reinterpretAs(" + sourceName + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        // noinspection unchecked
        final ColumnSource<S> localSourceColumnSource = (ColumnSource<S>) columnsOfInterest.get(sourceName);
        if (localSourceColumnSource == null) {
            throw new NoSuchColumnException(columnsOfInterest.keySet(), sourceName);
        }

        if (!localSourceColumnSource.getType().equals(sourceDataType)) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + localSourceColumnSource.getType() + ", expected " + sourceDataType);
        }

        if (!(localSourceColumnSource.allowsReinterpret(destDataType))) {
            if (TableTimeConversions.requiresZone(destDataType)) {
                if (reinterpParams == null || reinterpParams.length != 1 || !(reinterpParams[0] instanceof ZoneId)) {
                    throw new IllegalArgumentException("Incorrect arguments for ZonedDateTime conversion");
                }
                zone = (ZoneId) reinterpParams[0];
            } else {
                zone = null;
            }

            final boolean isDestTypeTimeIsh =
                    destDataType == ZonedDateTime.class
                            || destDataType == LocalDate.class
                            || destDataType == LocalTime.class
                            || destDataType == Instant.class
                            || destDataType == long.class
                            || destDataType == Long.class;

            if (localSourceColumnSource instanceof ConvertibleTimeSource
                    && ((ConvertibleTimeSource) localSourceColumnSource).supportsTimeConversion()) {
                if (!isDestTypeTimeIsh) {
                    throw new IllegalArgumentException(
                            "Source column " + sourceName + " (Class=" + localSourceColumnSource.getClass()
                                    + ") - cannot be reinterpreted as " + destDataType);
                }
            } else if (sourceDataType == Instant.class
                    || sourceDataType == ZonedDateTime.class
                    || sourceDataType == long.class
                    || sourceDataType == Long.class) {
                if (!isDestTypeTimeIsh) {
                    throw new IllegalArgumentException(
                            "Source column " + sourceName + " (Class=" + localSourceColumnSource.getClass()
                                    + ") - cannot be reinterpreted as " + destDataType);
                }
            } else {
                throw new IllegalArgumentException("Source column " + sourceName + " (Class="
                        + localSourceColumnSource.getClass() + ") - cannot be reinterpreted as " + destDataType);
            }
        }

        sourceColumnSource = localSourceColumnSource;
        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        // noinspection unchecked
        final ColumnDefinition<S> sourceColumnDefinition = (ColumnDefinition<S>) columnDefinitionMap.get(sourceName);
        if (sourceColumnDefinition == null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), sourceName);
        }
        if (!sourceColumnDefinition.getDataType().equals(sourceDataType)) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + sourceColumnDefinition.getDataType() + ", expected " + sourceDataType);
        }
        return getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return destDataType;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        // we don't support reinterpreting column types with components
        return null;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(sourceName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public ColumnSource<D> getDataView() {

        final Function<ColumnSource<?>, ColumnSource<D>> checkResult = result -> {
            if (!result.getType().equals(destDataType)) {
                throw new IllegalArgumentException("Reinterpreted column from " + sourceName + " has wrong data type "
                        + result.getType() + ", expected " + destDataType);
            }
            // noinspection unchecked
            return (ColumnSource<D>) result;
        };

        if (sourceColumnSource.allowsReinterpret(destDataType)) {
            return checkResult.apply(sourceColumnSource.reinterpret(destDataType));
        }

        // The only other conversions we will do are various time permutations.
        // If we can just reinterpret as time, great!
        if (sourceColumnSource instanceof ConvertibleTimeSource &&
                ((ConvertibleTimeSource) sourceColumnSource).supportsTimeConversion()) {
            if (destDataType == ZonedDateTime.class) {
                return checkResult.apply(((ConvertibleTimeSource) sourceColumnSource).toZonedDateTime(zone));
            } else if (destDataType == LocalDate.class) {
                return checkResult.apply(((ConvertibleTimeSource) sourceColumnSource).toLocalDate(zone));
            } else if (destDataType == LocalTime.class) {
                return checkResult.apply(((ConvertibleTimeSource) sourceColumnSource).toLocalTime(zone));
            } else if (destDataType == Instant.class) {
                return checkResult.apply(((ConvertibleTimeSource) sourceColumnSource).toInstant());
            } else if (destDataType == long.class || destDataType == Long.class) {
                return checkResult.apply(((ConvertibleTimeSource) sourceColumnSource).toEpochNano());
            }
        }

        if (sourceDataType == ZonedDateTime.class &&
                (destDataType == LocalDate.class || destDataType == LocalTime.class)) {
            // We can short circuit some ZDT conversions to try to be less wasteful
            if (destDataType == LocalDate.class) {
                // noinspection unchecked
                return checkResult.apply(new LocalDateWrapperSource(
                        (ColumnSource<ZonedDateTime>) sourceColumnSource, zone));
            } else {
                // noinspection unchecked
                return checkResult.apply(new LocalTimeWrapperSource(
                        (ColumnSource<ZonedDateTime>) sourceColumnSource, zone));
            }
        }

        // If we just want to go from X to long, this is fairly straightforward. Note that we skip LocalDate and
        // LocalTime these are not linked to nanos of epoch in any way. You could argue that LocalDate is, but then
        // we have to create even more garbage objects just to get the "time at midnight". Users should just do that
        // directly.
        final Function<ColumnSource<?>, ColumnSource<Long>> toLong;
        if (sourceDataType == Instant.class) {
            // noinspection unchecked
            toLong = s -> ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) s);
        } else if (sourceDataType == ZonedDateTime.class) {
            // noinspection unchecked
            toLong = s -> ReinterpretUtils.zonedDateTimeToLongSource((ColumnSource<ZonedDateTime>) s);
        } else if (sourceDataType == long.class || sourceDataType == Long.class) {
            // noinspection unchecked
            toLong = s -> (ColumnSource<Long>) sourceColumnSource;
        } else {
            throw new IllegalArgumentException("Source column " + sourceName + " (Class="
                    + sourceColumnSource.getClass() + ") - cannot be reinterpreted as " + destDataType);
        }

        // Otherwise we'll have to go from long back to a wrapped typed source.
        if (destDataType == Long.class || destDataType == long.class) {
            return checkResult.apply(toLong.apply(sourceColumnSource));
        } else if (destDataType == ZonedDateTime.class) {
            return checkResult.apply(new LongAsZonedDateTimeColumnSource(toLong.apply(sourceColumnSource), zone));
        } else if (destDataType == Instant.class) {
            return checkResult.apply(new LongAsInstantColumnSource(toLong.apply(sourceColumnSource)));
        } else if (destDataType == LocalDate.class) {
            return checkResult.apply(new LongAsLocalDateColumnSource(toLong.apply(sourceColumnSource), zone));
        } else if (destDataType == LocalTime.class) {
            return checkResult.apply(new LongAsLocalTimeColumnSource(toLong.apply(sourceColumnSource), zone));
        }

        throw new IllegalArgumentException("Source column " + sourceName + " (Class=" + sourceColumnSource.getClass()
                + ") - cannot be reinterpreted as " + destDataType);
    }

    @NotNull
    @Override
    public ColumnSource<D> getLazyView() {
        return getDataView();
    }

    @Override
    public String getName() {
        return destName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        throw new UnsupportedOperationException("ReinterpretedColumn should only be used with updateView() clauses.");
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ReinterpretedColumn<?, ?> that = (ReinterpretedColumn<?, ?>) o;

        return sourceName.equals(that.sourceName) && sourceDataType.equals(that.sourceDataType)
                && destName.equals(that.destName) && destDataType.equals(that.destDataType);
    }

    @Override
    public int hashCode() {
        int result = sourceName.hashCode();
        result = 31 * result + sourceDataType.hashCode();
        result = 31 * result + destName.hashCode();
        result = 31 * result + destDataType.hashCode();
        result = 31 * result + Arrays.hashCode(reinterpParams);
        return result;
    }

    @Override
    public boolean isStateless() {
        return sourceColumnSource.isStateless();
    }

    @Override
    public ReinterpretedColumn<S, D> copy() {
        return new ReinterpretedColumn<>(sourceName, sourceDataType, destName, destDataType, reinterpParams);
    }
}
