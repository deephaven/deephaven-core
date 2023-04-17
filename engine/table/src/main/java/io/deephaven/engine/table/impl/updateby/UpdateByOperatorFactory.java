package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.updateby.ema.*;
import io.deephaven.engine.table.impl.updateby.fill.*;
import io.deephaven.engine.table.impl.updateby.minmax.*;
import io.deephaven.engine.table.impl.updateby.prod.*;
import io.deephaven.engine.table.impl.updateby.rollingavg.*;
import io.deephaven.engine.table.impl.updateby.rollinggroup.RollingGroupOperator;
import io.deephaven.engine.table.impl.updateby.rollingminmax.*;
import io.deephaven.engine.table.impl.updateby.rollingproduct.*;
import io.deephaven.engine.table.impl.updateby.rollingsum.*;
import io.deephaven.engine.table.impl.updateby.rollingwavg.*;
import io.deephaven.engine.table.impl.updateby.sum.*;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * A factory to visit all of the {@link UpdateByOperation}s to produce a set of {@link UpdateByOperator}s that
 * {@link UpdateBy} can use to produce a result.
 */
public class UpdateByOperatorFactory {
    private final Table source;
    private final MatchPair[] groupByColumns;
    @Nullable
    private final WritableRowRedirection rowRedirection;
    @NotNull
    private final UpdateByControl control;

    public UpdateByOperatorFactory(@NotNull final Table source,
            @NotNull final MatchPair[] groupByColumns,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        this.source = source;
        this.groupByColumns = groupByColumns;
        this.rowRedirection = rowRedirection;
        this.control = control;
    }

    /**
     * Create a collection of operator output column names from a list of {@link UpdateByOperation operator specs}.
     *
     * @param specs the collection of {@link UpdateByOperation specs} to create
     * @return a collection of {@link String column names}
     */
    final Collection<String> getOutputColumns(@NotNull final Collection<? extends UpdateByOperation> specs) {
        final OutputColumnVisitor v = new OutputColumnVisitor();
        specs.forEach(s -> s.walk(v));
        return v.outputColumns;
    }

    /**
     * Create a collection of operators from a list of {@link UpdateByOperation operator specs}. This operation assumes
     * that the {@link UpdateByOperation specs} are window-compatible, i.e. will share cumulative vs. rolling properties
     * and window parameters.
     *
     * @param specs the collection of {@link UpdateByOperation specs} to create
     * @return a organized collection of {@link UpdateByOperator operations} where each operator can share resources
     *         within the collection
     */
    final Collection<UpdateByOperator> getOperators(@NotNull final Collection<? extends UpdateByOperation> specs) {
        final OperationVisitor v = new OperationVisitor();
        specs.forEach(s -> s.walk(v));

        // Do we have a combined rolling group operator to create?
        if (v.rollingGroupSpec != null) {
            v.ops.add(v.makeRollingGroupOperator(v.rollingGroupPairs, source, v.rollingGroupSpec));
        }
        return v.ops;
    }

    /**
     * Organize the {@link UpdateByOperation operator specs} into windows so that the operators that can share
     * processing resources are stored within the same collection. Generally, this collects cumulative operators
     * together and collects rolling operators with the same parameters together.
     *
     * @param specs the collection of {@link UpdateByOperation specs} to organize
     * @return a organized collection of {@link List<ColumnUpdateOperation> operation lists} where the operator specs
     *         can share resources within the collection
     */
    final Collection<List<ColumnUpdateOperation>> getWindowOperatorSpecs(
            @NotNull final Collection<? extends UpdateByOperation> specs) {
        final WindowVisitor v = new WindowVisitor();
        specs.forEach(s -> s.walk(v));
        return v.windowMap.values();
    }

    static MatchPair[] parseMatchPairs(final List<Pair> columns) {
        return columns.stream()
                .map(MatchPair::of)
                .toArray(MatchPair[]::new);
    }

    /**
     * If the input columns to add is an empty array, create a new one that maps each column to itself in the result
     *
     * @param table the source table
     * @param columnsToAdd the list of {@link MatchPair}s for the result columns
     * @return the input columns to add if it was non-empty, or a new one that maps each source column 1:1 to the
     *         output.
     */
    @NotNull
    static MatchPair[] createColumnsToAddIfMissing(final @NotNull Table table,
            final @NotNull MatchPair[] columnsToAdd,
            final @NotNull UpdateBySpec spec,
            final MatchPair[] groupByColumns) {
        if (columnsToAdd.length == 0) {
            return createOneToOneMatchPairs(table, groupByColumns, spec);
        }
        return columnsToAdd;
    }

    /**
     * Create a new {@link MatchPair} array that maps each input column to itself on the output side.
     *
     * @param table the source table.
     * @param groupByColumns the columns to group the table by
     * @return A new {@link MatchPair}[] that maps each source column 1:1 to the output.
     */
    @NotNull
    static MatchPair[] createOneToOneMatchPairs(final @NotNull Table table,
            final MatchPair[] groupByColumns,
            @NotNull final UpdateBySpec spec) {
        final Set<String> usedGroupColumns = groupByColumns.length == 0 ? Collections.emptySet()
                : Arrays.stream(groupByColumns)
                        .map(MatchPair::rightColumn).collect(Collectors.toSet());
        return table.getDefinition().getColumnStream()
                .filter(c -> !usedGroupColumns.contains(c.getName()) && spec.applicableTo(c.getDataType()))
                .map(c -> new MatchPair(c.getName(), c.getName()))
                .toArray(MatchPair[]::new);
    }

    public String describe(Collection<? extends UpdateByOperation> clauses) {
        final Describer d = new Describer();
        clauses.forEach(c -> c.walk(d));
        return d.descriptionBuilder.toString();
    }

    private static class Describer implements UpdateByOperation.Visitor<Void> {
        final StringBuilder descriptionBuilder = new StringBuilder();

        @Override
        public Void visit(ColumnUpdateOperation clause) {
            final MatchPair[] pairs = parseMatchPairs(clause.columns());
            final String columnStr;
            if (pairs.length == 0) {
                columnStr = "[All]";
            } else {
                columnStr = MatchPair.matchString(pairs);
            }

            descriptionBuilder.append(clause.spec().toString()).append("(").append(columnStr).append("), ");
            return null;
        }
    }

    private class OutputColumnVisitor implements UpdateByOperation.Visitor<Void> {
        final List<String> outputColumns = new ArrayList<>();

        @Override
        public Void visit(@NotNull final ColumnUpdateOperation clause) {
            final UpdateBySpec spec = clause.spec();
            final MatchPair[] pairs =
                    createColumnsToAddIfMissing(source, parseMatchPairs(clause.columns()), spec, groupByColumns);
            for (MatchPair pair : pairs) {
                outputColumns.add(pair.leftColumn);
            }
            return null;
        }
    }

    private static class WindowVisitor implements UpdateByOperation.Visitor<Void> {
        boolean created = false;

        // We will divide the operators into similar windows for efficient processing.
        final KeyedObjectHashMap<ColumnUpdateOperation, List<ColumnUpdateOperation>> windowMap =
                new KeyedObjectHashMap<>(new KeyedObjectKey<>() {
                    @Override
                    public ColumnUpdateOperation getKey(final List<ColumnUpdateOperation> clauseList) {
                        return clauseList.get(0);
                    }

                    @Override
                    public int hashKey(final ColumnUpdateOperation clause) {
                        final UpdateBySpec spec = clause.spec();

                        final boolean windowed = spec instanceof RollingOpSpec;
                        int hash = Boolean.hashCode(windowed);

                        // Treat all cumulative ops with the same input columns as identical, even if they rely on
                        // timestamps
                        if (!windowed) {
                            return hash;
                        }

                        final RollingOpSpec rollingSpec = (RollingOpSpec) spec;

                        // Windowed ops are unique per type (ticks/time-based) and window dimensions
                        hash = 31 * hash + Objects.hashCode(rollingSpec.revWindowScale().timestampCol());
                        hash = 31 * hash + Long.hashCode(rollingSpec.revWindowScale().timescaleUnits());
                        hash = 31 * hash + Long.hashCode(rollingSpec.fwdWindowScale().timescaleUnits());
                        return hash;
                    }

                    @Override
                    public boolean equalKey(final ColumnUpdateOperation clauseA,
                            final List<ColumnUpdateOperation> clauseList) {
                        final ColumnUpdateOperation clauseB = clauseList.get(0);

                        final UpdateBySpec specA = clauseA.spec();
                        final UpdateBySpec specB = clauseB.spec();

                        // Equivalent if both are cumulative, not equivalent if only one is cumulative
                        boolean aRolling = specA instanceof RollingOpSpec;
                        boolean bRolling = specB instanceof RollingOpSpec;

                        if (!aRolling && !bRolling) {
                            return true;
                        } else if (aRolling != bRolling) {
                            return false;
                        }

                        final RollingOpSpec rsA = (RollingOpSpec) specA;
                        final RollingOpSpec rsB = (RollingOpSpec) specB;

                        // Rolling ops are equivalent when they share a time or row based accumulation and the same
                        // fwd/prev units
                        if (!Objects.equals(rsA.revWindowScale().timestampCol(), rsB.revWindowScale().timestampCol())) {
                            return false;
                        }
                        return rsA.revWindowScale().timescaleUnits() == rsB.revWindowScale().timescaleUnits() &&
                                rsA.fwdWindowScale().timescaleUnits() == rsB.fwdWindowScale().timescaleUnits();
                    }
                });

        @Override
        public Void visit(@NotNull final ColumnUpdateOperation clause) {
            created = false;
            final List<ColumnUpdateOperation> opList = windowMap.putIfAbsent(clause,
                    (newOpListOp) -> {
                        final List<ColumnUpdateOperation> newOpList = new ArrayList<>();
                        newOpList.add(clause);
                        created = true;
                        return newOpList;
                    });
            if (!created) {
                opList.add(clause);
            }
            return null;
        }
    }

    private class OperationVisitor implements UpdateBySpec.Visitor<Void>, UpdateByOperation.Visitor<Void> {
        private final List<UpdateByOperator> ops = new ArrayList<>();
        private MatchPair[] pairs;

        // Storage for delayed RollingGroup creation.
        RollingGroupSpec rollingGroupSpec;
        MatchPair[] rollingGroupPairs;

        /**
         * Check if the supplied type is one of the supported time types.
         *
         * @param type the type
         * @return true if the type is one of the useable time types
         */
        public boolean isTimeType(final @NotNull Class<?> type) {
            // TODO: extend time handling similar to enterprise (Instant, ZonedDateTime, LocalDate, LocalTime)
            return type == DateTime.class;
        }

        @Override
        public Void visit(@NotNull final ColumnUpdateOperation clause) {
            final UpdateBySpec spec = clause.spec();
            pairs = createColumnsToAddIfMissing(source, parseMatchPairs(clause.columns()), spec, groupByColumns);
            spec.walk(this);
            pairs = null;
            return null;
        }

        @Override
        public Void visit(@NotNull final EmaSpec es) {
            final boolean isTimeBased = es.timeScale().isTimeBased();
            final String timestampCol = es.timeScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeEmaOperator(fc,
                            source,
                            es))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final FillBySpec fbs) {
            Arrays.stream(pairs)
                    .map(fc -> makeForwardFillOperator(fc, source))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final CumSumSpec css) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumSumOperator(fc, source))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(CumMinMaxSpec cmms) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumMinMaxOperator(fc, source, cmms.isMax()))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(CumProdSpec cps) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumProdOperator(fc, source))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingSumSpec rss) {
            final boolean isTimeBased = rss.revWindowScale().isTimeBased();
            final String timestampCol = rss.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingSumOperator(fc,
                            source,
                            rss))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingGroupSpec rgs) {
            // Delay the creation of the operator until we combine all the pairs together.
            if (rollingGroupSpec == null) {
                rollingGroupSpec = rgs;
                rollingGroupPairs = pairs;
                return null;
            }

            // The specs are identical and we can accumulate the pairs. We are not testing for uniqueness because
            // subsequent checks will catch collisions.
            final MatchPair[] newPairs = Arrays.copyOf(rollingGroupPairs, rollingGroupPairs.length + pairs.length);
            System.arraycopy(pairs, 0, newPairs, rollingGroupPairs.length, pairs.length);
            rollingGroupPairs = newPairs;
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingProductSpec rps) {
            final boolean isTimeBased = rps.revWindowScale().isTimeBased();
            final String timestampCol = rps.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingProductOperator(fc,
                            source,
                            rps))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingAvgSpec rs) {
            final boolean isTimeBased = rs.revWindowScale().isTimeBased();
            final String timestampCol = rs.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingAvgOperator(fc,
                            source,
                            rs))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingMinMaxSpec rmm) {
            final boolean isTimeBased = rmm.revWindowScale().isTimeBased();
            final String timestampCol = rmm.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingMinMaxOperator(fc,
                            source,
                            rmm))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingWAvgSpec rws) {
            final boolean isTimeBased = rws.revWindowScale().isTimeBased();
            final String timestampCol = rws.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingWAvgOperator(fc,
                            source,
                            rws))
                    .forEach(ops::add);
            return null;
        }

        private UpdateByOperator makeEmaOperator(@NotNull final MatchPair pair,
                @NotNull final Table source,
                @NotNull final EmaSpec ema) {
            // noinspection rawtypes
            final ColumnSource columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final String[] affectingColumns;
            if (ema.timeScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {ema.timeScale().timestampCol(), pair.rightColumn};
            }

            // use the correct units from the EmaSpec (depending on if Time or Tick based)
            final long timeScaleUnits = ema.timeScale().timescaleUnits();
            final OperationControl control = ema.controlOrDefault();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == long.class || csType == Long.class) {
                return new LongEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            } else if (csType == BigInteger.class) {
                return new BigIntegerEMAOperator(pair, affectingColumns, rowRedirection, control,
                        ema.timeScale().timestampCol(), timeScaleUnits, columnSource);
            }

            throw new IllegalArgumentException("Can not perform EMA on type " + csType);
        }

        private UpdateByOperator makeCumProdOperator(MatchPair fc, Table source) {
            final Class<?> csType = source.getColumnSource(fc.rightColumn).getType();
            if (csType == byte.class || csType == Byte.class) {
                return new ByteCumProdOperator(fc, rowRedirection, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumProdOperator(fc, rowRedirection);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumProdOperator(fc, rowRedirection);
            } else if (csType == long.class || csType == Long.class) {
                return new LongCumProdOperator(fc, rowRedirection);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumProdOperator(fc, rowRedirection);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumProdOperator(fc, rowRedirection);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalCumProdOperator(fc, rowRedirection, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerCumProdOperator(fc, rowRedirection);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Min/Max on type " + csType);
        }

        private UpdateByOperator makeCumMinMaxOperator(MatchPair fc, Table source, boolean isMax) {
            final ColumnSource<?> columnSource = source.getColumnSource(fc.rightColumn);
            final Class<?> csType = columnSource.getType();
            if (csType == byte.class || csType == Byte.class) {
                return new ByteCumMinMaxOperator(fc, isMax, rowRedirection, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumMinMaxOperator(fc, isMax, rowRedirection);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumMinMaxOperator(fc, isMax, rowRedirection);
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongCumMinMaxOperator(fc, isMax, rowRedirection, csType);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumMinMaxOperator(fc, isMax, rowRedirection);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumMinMaxOperator(fc, isMax, rowRedirection);
            } else if (Comparable.class.isAssignableFrom(csType)) {
                // noinspection rawtypes
                return new ComparableCumMinMaxOperator(fc, isMax, rowRedirection, csType);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Min/Max on type " + csType);
        }

        private UpdateByOperator makeCumSumOperator(MatchPair fc, Table source) {
            final Class<?> csType = source.getColumnSource(fc.rightColumn).getType();
            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteCumSumOperator(fc, rowRedirection, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteCumSumOperator(fc, rowRedirection, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumSumOperator(fc, rowRedirection);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumSumOperator(fc, rowRedirection);
            } else if (csType == long.class || csType == Long.class) {
                return new LongCumSumOperator(fc, rowRedirection);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumSumOperator(fc, rowRedirection);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumSumOperator(fc, rowRedirection);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalCumSumOperator(fc, rowRedirection, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerCumSumOperator(fc, rowRedirection);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Sum on type " + csType);
        }

        private UpdateByOperator makeForwardFillOperator(MatchPair fc, Table source) {
            final ColumnSource<?> columnSource = source.getColumnSource(fc.rightColumn);
            final Class<?> csType = columnSource.getType();
            if (csType == char.class || csType == Character.class) {
                return new CharFillByOperator(fc, rowRedirection);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteFillByOperator(fc, rowRedirection);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortFillByOperator(fc, rowRedirection);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntFillByOperator(fc, rowRedirection);
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongFillByOperator(fc, rowRedirection, csType);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatFillByOperator(fc, rowRedirection);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleFillByOperator(fc, rowRedirection);
            } else if (csType == boolean.class || csType == Boolean.class) {
                return new BooleanFillByOperator(fc, rowRedirection);
            } else {
                return new ObjectFillByOperator<>(fc, rowRedirection, csType);
            }
        }

        private UpdateByOperator makeRollingSumOperator(@NotNull final MatchPair pair,
                @NotNull final Table source,
                @NotNull final RollingSumSpec rs) {
            // noinspection rawtypes
            final ColumnSource columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().timescaleUnits();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingSumOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            }

            throw new IllegalArgumentException("Can not perform RollingSum on type " + csType);
        }

        private UpdateByOperator makeRollingGroupOperator(@NotNull final MatchPair[] pairs,
                @NotNull final Table source,
                @NotNull final RollingGroupSpec rg) {

            // noinspection rawtypes
            final ColumnSource[] columnSources = new ColumnSource[pairs.length];
            final String[] affectingColumns;
            if (rg.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[pairs.length];
            } else {
                // We are affected by the timestamp column. Add it to the end of the list
                affectingColumns = new String[pairs.length + 1];
                affectingColumns[pairs.length] = rg.revWindowScale().timestampCol();
            }

            // Assemble the arrays of input and affecting sources
            for (int ii = 0; ii < pairs.length; ii++) {
                MatchPair pair = pairs[ii];

                columnSources[ii] = source.getColumnSource(pair.rightColumn);
                affectingColumns[ii] = pair.rightColumn;
            }

            final long prevWindowScaleUnits = rg.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rg.fwdWindowScale().timescaleUnits();

            return new RollingGroupOperator(pairs, affectingColumns, rowRedirection,
                    rg.revWindowScale().timestampCol(),
                    prevWindowScaleUnits, fwdWindowScaleUnits, columnSources);
        }

        private UpdateByOperator makeRollingAvgOperator(@NotNull final MatchPair pair,
                @NotNull final Table source,
                @NotNull final RollingAvgSpec rs) {
            // noinspection rawtypes
            final ColumnSource columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().timescaleUnits();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            }

            throw new IllegalArgumentException("Can not perform RollingSum on type " + csType);
        }

        private UpdateByOperator makeRollingMinMaxOperator(@NotNull MatchPair pair,
                @NotNull Table source,
                @NotNull RollingMinMaxSpec rmm) {
            final ColumnSource<?> columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final String[] affectingColumns;
            if (rmm.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rmm.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rmm.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rmm.fwdWindowScale().timescaleUnits();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingMinMaxOperator(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (Comparable.class.isAssignableFrom(csType)) {
                // noinspection rawtypes
                return new ComparableRollingMinMaxOperator<>(pair, affectingColumns, rowRedirection,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax(), csType);
            }

            throw new IllegalArgumentException("Can not perform Rolling Min/Max on type " + csType);
        }

        private UpdateByOperator makeRollingProductOperator(@NotNull final MatchPair pair,
                @NotNull final Table source,
                @NotNull final RollingProductSpec rs) {
            // noinspection rawtypes
            final ColumnSource columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().timescaleUnits();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingProductOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            }

            throw new IllegalArgumentException("Can not perform RollingProduct on type " + csType);
        }

        private UpdateByOperator makeRollingWAvgOperator(@NotNull final MatchPair pair,
                @NotNull final Table source,
                @NotNull final RollingWAvgSpec rs) {
            // noinspection rawtypes
            final ColumnSource columnSource = source.getColumnSource(pair.rightColumn);
            final Class<?> csType = columnSource.getType();

            final ColumnSource weightColumnSource = source.getColumnSource(rs.weightCol());
            final Class<?> weightCsType = weightColumnSource.getType();

            if (!rs.weightColumnApplicableTo(weightCsType)) {
                throw new IllegalArgumentException("Can not perform RollingWAvg on weight column type " + csType);
            }

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn, rs.weightCol()};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn, rs.weightCol()};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().timescaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().timescaleUnits();

            if (csType == BigDecimal.class || csType == BigInteger.class ||
                    weightCsType == BigDecimal.class || weightCsType == BigInteger.class) {
                // We need to produce a BigDecimal result output. All input columns will be cast to BigDecimal so
                // there is no distinction between input types.
                return new BigDecimalRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource,
                        columnSource, control.mathContextOrDefault());
            }

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingWAvgOperator(pair, affectingColumns, rowRedirection,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), weightColumnSource);
            }

            throw new IllegalArgumentException("Can not perform RollingWAvg on type " + csType);
        }
    }
}
