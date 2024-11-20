//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.Pair;
import io.deephaven.api.Selectable;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.updateby.delta.*;
import io.deephaven.engine.table.impl.updateby.em.*;
import io.deephaven.engine.table.impl.updateby.emstd.*;
import io.deephaven.engine.table.impl.updateby.fill.*;
import io.deephaven.engine.table.impl.updateby.minmax.*;
import io.deephaven.engine.table.impl.updateby.prod.*;
import io.deephaven.engine.table.impl.updateby.rollingavg.*;
import io.deephaven.engine.table.impl.updateby.rollingcount.*;
import io.deephaven.engine.table.impl.updateby.rollingformula.*;
import io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.RollingFormulaMultiColumnOperator;
import io.deephaven.engine.table.impl.updateby.rollinggroup.RollingGroupOperator;
import io.deephaven.engine.table.impl.updateby.rollingminmax.*;
import io.deephaven.engine.table.impl.updateby.rollingproduct.*;
import io.deephaven.engine.table.impl.updateby.rollingstd.*;
import io.deephaven.engine.table.impl.updateby.rollingsum.*;
import io.deephaven.engine.table.impl.updateby.rollingwavg.*;
import io.deephaven.engine.table.impl.updateby.sum.*;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.vector.VectorFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * A factory to visit all of the {@link UpdateByOperation}s to produce a set of {@link UpdateByOperator}s that
 * {@link UpdateBy} can use to produce a result.
 */
public class UpdateByOperatorFactory {
    private final TableDefinition tableDef;
    private final MatchPair[] groupByColumns;
    @NotNull
    private final UpdateByControl control;
    private Map<String, ColumnDefinition<?>> vectorColumnDefinitions;

    public UpdateByOperatorFactory(
            @NotNull final TableDefinition tableDef,
            @NotNull final MatchPair[] groupByColumns,
            @NotNull final UpdateByControl control) {
        this.tableDef = tableDef;
        this.groupByColumns = groupByColumns;
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
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();

        final OperationVisitor v = new OperationVisitor(compilationProcessor);
        specs.forEach(s -> s.walk(v));

        // Do we have a combined rolling group operator to create?
        if (v.rollingGroupSpec != null) {
            v.ops.add(v.makeRollingGroupOperator(v.rollingGroupPairs, tableDef, v.rollingGroupSpec));
        }

        compilationProcessor.compile();

        // Each EmStd operator needs to be paired with an Ema operator. If one already exists for the input column,
        // use it. Otherwise create one but hide the output columns.
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
     * @param tableDef the source table definition
     * @param columnsToAdd the list of {@link MatchPair}s for the result columns
     * @return the input columns to add if it was non-empty, or a new one that maps each source column 1:1 to the
     *         output.
     */
    @NotNull
    static MatchPair[] createColumnsToAddIfMissing(
            @NotNull final TableDefinition tableDef,
            @NotNull final MatchPair[] columnsToAdd,
            @NotNull final UpdateBySpec spec,
            final MatchPair[] groupByColumns) {
        if (columnsToAdd.length == 0) {
            return createOneToOneMatchPairs(tableDef, groupByColumns, spec);
        }
        return columnsToAdd;
    }

    /**
     * Create a new {@link MatchPair} array that maps each input column to itself on the output side.
     *
     * @param tableDef the source table definition
     * @param groupByColumns the columns to group the tableDef by
     * @return A new {@link MatchPair}[] that maps each source column 1:1 to the output.
     */
    @NotNull
    static MatchPair[] createOneToOneMatchPairs(
            @NotNull final TableDefinition tableDef,
            final MatchPair[] groupByColumns,
            @NotNull final UpdateBySpec spec) {
        final Set<String> usedGroupColumns = groupByColumns.length == 0 ? Collections.emptySet()
                : Arrays.stream(groupByColumns)
                        .map(MatchPair::rightColumn).collect(Collectors.toSet());
        return tableDef.getColumnStream()
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
                    createColumnsToAddIfMissing(tableDef, parseMatchPairs(clause.columns()), spec, groupByColumns);
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

                        // Leverage ImmutableWindowScale.hashCode() function.
                        hash = 31 * hash + Objects.hashCode(rollingSpec.revWindowScale());
                        hash = 31 * hash + Objects.hashCode(rollingSpec.fwdWindowScale());
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
                        // Leverage ImmutableWindowScale.equals() functions.
                        return Objects.equals(rsA.revWindowScale(), rsB.revWindowScale()) &&
                                Objects.equals(rsA.fwdWindowScale(), rsB.fwdWindowScale());
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
        private final QueryCompilerRequestProcessor compilationProcessor;
        private final List<UpdateByOperator> ops = new ArrayList<>();
        private MatchPair[] pairs;

        // Storage for delayed RollingGroup creation.
        RollingGroupSpec rollingGroupSpec;
        MatchPair[] rollingGroupPairs;

        OperationVisitor(
                @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
            this.compilationProcessor = compilationProcessor;
        }

        /**
         * Check if the supplied type is one of the supported time types.
         *
         * @param type the type
         * @return true if the type is one of the useable time types
         */
        public boolean isTimeType(@NotNull final Class<?> type) {
            // TODO: extend time handling similar to enterprise (ZonedDateTime, LocalDate, LocalTime)
            return type == Instant.class;
        }

        @Override
        public Void visit(@NotNull final ColumnUpdateOperation clause) {
            final UpdateBySpec spec = clause.spec();
            pairs = createColumnsToAddIfMissing(tableDef, parseMatchPairs(clause.columns()), spec, groupByColumns);
            spec.walk(this);
            pairs = null;
            return null;
        }

        @Override
        public Void visit(@NotNull final EmaSpec es) {
            final boolean isTimeBased = es.windowScale().isTimeBased();
            final String timestampCol = es.windowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeEmaOperator(fc,
                            tableDef,
                            es))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final EmsSpec spec) {
            final boolean isTimeBased = spec.windowScale().isTimeBased();
            final String timestampCol = spec.windowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeEmsOperator(fc,
                            tableDef,
                            spec))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final EmMinMaxSpec spec) {
            final boolean isTimeBased = spec.windowScale().isTimeBased();
            final String timestampCol = spec.windowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeEmMinMaxOperator(fc,
                            tableDef,
                            spec))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final EmStdSpec spec) {
            final boolean isTimeBased = spec.windowScale().isTimeBased();
            final String timestampCol = spec.windowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeEmStdOperator(fc,
                            tableDef,
                            spec))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final FillBySpec fbs) {
            Arrays.stream(pairs)
                    .map(fc -> makeForwardFillOperator(fc, tableDef))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final CumSumSpec css) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumSumOperator(fc, tableDef))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(CumMinMaxSpec cmms) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumMinMaxOperator(fc, tableDef, cmms.isMax()))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(CumProdSpec cps) {
            Arrays.stream(pairs)
                    .map(fc -> makeCumProdOperator(fc, tableDef))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final DeltaSpec spec) {
            Arrays.stream(pairs)
                    .map(fc -> makeDeltaOperator(fc, tableDef, spec))
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
                            tableDef,
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
                            tableDef,
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
                            tableDef,
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
                            tableDef,
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
                            tableDef,
                            rws))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingStdSpec spec) {
            final boolean isTimeBased = spec.revWindowScale().isTimeBased();
            final String timestampCol = spec.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingStdOperator(fc,
                            tableDef,
                            spec))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingCountSpec spec) {
            final boolean isTimeBased = spec.revWindowScale().isTimeBased();
            final String timestampCol = spec.revWindowScale().timestampCol();

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingCountOperator(fc,
                            tableDef,
                            spec))
                    .forEach(ops::add);
            return null;
        }

        @Override
        public Void visit(@NotNull final RollingFormulaSpec spec) {
            final boolean isTimeBased = spec.revWindowScale().isTimeBased();
            final String timestampCol = spec.revWindowScale().timestampCol();

            // These operators can re-use formula columns when the types match.
            final Map<Class<?>, FormulaColumn> formulaColumnMap = new HashMap<>();

            // noinspection deprecation
            if (spec.paramToken().isEmpty()) {
                ops.add(makeRollingFormulaMultiColumnOperator(tableDef, spec));
                return null;
            }

            Arrays.stream(pairs)
                    .filter(p -> !isTimeBased || !p.rightColumn().equals(timestampCol))
                    .map(fc -> makeRollingFormulaOperator(fc, tableDef, formulaColumnMap, spec))
                    .forEach(ops::add);
            return null;
        }

        private UpdateByOperator makeEmaOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final EmaSpec spec) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (spec.windowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {spec.windowScale().timestampCol(), pair.rightColumn};
            }

            // use the correct units from the EmaSpec (depending on if Time or Tick based)
            final double timeScaleUnits = spec.windowScale().getFractionalTimeScaleUnits();
            final OperationControl control = spec.controlOrDefault();
            final MathContext mathCtx = control.bigValueContextOrDefault();

            final BasePrimitiveEMOperator.EmFunction doubleFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                final double decayedVal = prev * alpha;
                return decayedVal + (oneMinusAlpha * cur);
            };
            final BaseBigNumberEMOperator.EmFunction bdFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                final BigDecimal decayedVal = prev.multiply(alpha, mathCtx);
                return decayedVal.add(cur.multiply(oneMinusAlpha, mathCtx), mathCtx);
            };

            if (csType == byte.class || csType == Byte.class) {
                return new ByteEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == long.class || csType == Long.class) {
                return new LongEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            } else if (csType == BigInteger.class) {
                return new BigIntegerEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            }

            throw new IllegalArgumentException("Can not perform EMA on type " + csType);
        }

        private UpdateByOperator makeEmsOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final EmsSpec spec) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (spec.windowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {spec.windowScale().timestampCol(), pair.rightColumn};
            }

            // use the correct units from the EmsSpec (depending on if Time or Tick based)
            final double timeScaleUnits = spec.windowScale().getFractionalTimeScaleUnits();
            final OperationControl control = spec.controlOrDefault();
            final MathContext mathCtx = control.bigValueContextOrDefault();

            final BasePrimitiveEMOperator.EmFunction doubleFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                final double decayedVal = prev * alpha;
                return decayedVal + cur;
            };
            final BaseBigNumberEMOperator.EmFunction bdFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                final BigDecimal decayedVal = prev.multiply(alpha, mathCtx);
                return decayedVal.add(cur, mathCtx);
            };

            if (csType == byte.class || csType == Byte.class) {
                return new ByteEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == long.class || csType == Long.class) {
                return new LongEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            } else if (csType == BigInteger.class) {
                return new BigIntegerEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            }

            throw new IllegalArgumentException("Can not perform EMS on type " + csType);
        }

        private UpdateByOperator makeEmMinMaxOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final EmMinMaxSpec spec) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (spec.windowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {spec.windowScale().timestampCol(), pair.rightColumn};
            }

            // use the correct units from the EmMinMaxSpec (depending on if Time or Tick based)
            final double timeScaleUnits = spec.windowScale().getFractionalTimeScaleUnits();
            final OperationControl control = spec.controlOrDefault();
            final MathContext mathCtx = control.bigValueContextOrDefault();

            final BasePrimitiveEMOperator.EmFunction doubleFunction;
            final BaseBigNumberEMOperator.EmFunction bdFunction;

            if (spec.isMax()) {
                doubleFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                    final double decayedVal = prev * alpha;
                    return Math.max(decayedVal, cur);
                };
                bdFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                    final BigDecimal decayedVal = prev.multiply(alpha, mathCtx);
                    return decayedVal.compareTo(cur) > 0
                            ? decayedVal
                            : cur;
                };
            } else {
                doubleFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                    final double decayedVal = prev * alpha;
                    return Math.min(decayedVal, cur);
                };
                bdFunction = (prev, cur, alpha, oneMinusAlpha) -> {
                    final BigDecimal decayedVal = prev.multiply(alpha, mathCtx);
                    return decayedVal.compareTo(cur) < 0
                            ? decayedVal
                            : cur;
                };
            }

            if (csType == byte.class || csType == Byte.class) {
                return new ByteEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == long.class || csType == Long.class) {
                return new LongEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, doubleFunction);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            } else if (csType == BigInteger.class) {
                return new BigIntegerEMOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, bdFunction);
            }

            throw new IllegalArgumentException("Can not perform EmMinMax on type " + csType);
        }

        private UpdateByOperator makeEmStdOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final EmStdSpec spec) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (spec.windowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {spec.windowScale().timestampCol(), pair.rightColumn};
            }

            // use the correct units from the EmaSpec (depending on if Time or Tick based)
            final double timeScaleUnits = spec.windowScale().getFractionalTimeScaleUnits();
            final OperationControl control = spec.controlOrDefault();
            final MathContext mathCtx = control.bigValueContextOrDefault();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, mathCtx);
            } else if (csType == BigInteger.class) {
                return new BigIntegerEmStdOperator(pair, affectingColumns, control,
                        spec.windowScale().timestampCol(), timeScaleUnits, mathCtx);
            }

            throw new IllegalArgumentException("Can not perform EmStd on type " + csType);
        }

        private UpdateByOperator makeCumProdOperator(MatchPair pair, TableDefinition tableDef) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteCumProdOperator(pair, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumProdOperator(pair);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumProdOperator(pair);
            } else if (csType == long.class || csType == Long.class) {
                return new LongCumProdOperator(pair);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumProdOperator(pair);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumProdOperator(pair);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalCumProdOperator(pair, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerCumProdOperator(pair);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Min/Max on type " + csType);
        }

        private UpdateByOperator makeCumMinMaxOperator(MatchPair pair, TableDefinition tableDef, boolean isMax) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteCumMinMaxOperator(pair, isMax, NULL_BYTE);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumMinMaxOperator(pair, isMax);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumMinMaxOperator(pair, isMax);
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongCumMinMaxOperator(pair, isMax, csType);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumMinMaxOperator(pair, isMax);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumMinMaxOperator(pair, isMax);
            } else if (Comparable.class.isAssignableFrom(csType)) {
                return new ComparableCumMinMaxOperator(pair, isMax, csType);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Min/Max on type " + csType);
        }

        private UpdateByOperator makeCumSumOperator(MatchPair pair, TableDefinition tableDef) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteCumSumOperator(pair, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteCumSumOperator(pair, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharCumSumOperator(pair);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortCumSumOperator(pair);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntCumSumOperator(pair);
            } else if (csType == long.class || csType == Long.class) {
                return new LongCumSumOperator(pair);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatCumSumOperator(pair);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleCumSumOperator(pair);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalCumSumOperator(pair, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerCumSumOperator(pair);
            }

            throw new IllegalArgumentException("Can not perform Cumulative Sum on type " + csType);
        }

        private UpdateByOperator makeForwardFillOperator(MatchPair pair, TableDefinition tableDef) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            if (csType == char.class || csType == Character.class) {
                return new CharFillByOperator(pair);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteFillByOperator(pair);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortFillByOperator(pair);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntFillByOperator(pair);
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongFillByOperator(pair, csType);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatFillByOperator(pair);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleFillByOperator(pair);
            } else if (csType == boolean.class || csType == Boolean.class) {
                return new BooleanFillByOperator(pair);
            } else {
                return new ObjectFillByOperator<>(pair, csType);
            }
        }

        private UpdateByOperator makeDeltaOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final DeltaSpec ds) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            if (csType == Character.class || csType == char.class) {
                return new CharDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == short.class || csType == Short.class) {
                return new ShortDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == int.class || csType == Integer.class) {
                return new IntDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == float.class || csType == Float.class) {
                return new FloatDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == BigDecimal.class) {
                return new BigDecimalDeltaOperator(pair, ds.deltaControlOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerDeltaOperator(pair, ds.deltaControlOrDefault());
            }

            throw new IllegalArgumentException("Can not perform Delta on type " + csType);
        }

        private UpdateByOperator makeRollingSumOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingSumSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingSumOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            }

            throw new IllegalArgumentException("Can not perform RollingSum on type " + csType);
        }

        private UpdateByOperator makeRollingGroupOperator(@NotNull final MatchPair[] pairs,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingGroupSpec rg) {

            Stream<String> inputColumnStream = Arrays.stream(pairs).map(MatchPair::rightColumn);
            if (rg.revWindowScale().timestampCol() != null) {
                // Include the timestamp column in the affecting list
                inputColumnStream = Stream.concat(Stream.of(rg.revWindowScale().timestampCol()), inputColumnStream);
            }
            final String[] affectingColumns = inputColumnStream.toArray(String[]::new);

            final long prevWindowScaleUnits = rg.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rg.fwdWindowScale().getTimeScaleUnits();

            return new RollingGroupOperator(pairs, affectingColumns,
                    rg.revWindowScale().timestampCol(),
                    prevWindowScaleUnits, fwdWindowScaleUnits, tableDef);
        }

        private UpdateByOperator makeRollingAvgOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingAvgSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            }

            throw new IllegalArgumentException("Can not perform RollingSum on type " + csType);
        }

        private UpdateByOperator makeRollingMinMaxOperator(@NotNull MatchPair pair,
                @NotNull TableDefinition tableDef,
                @NotNull RollingMinMaxSpec rmm) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rmm.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rmm.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rmm.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rmm.fwdWindowScale().getTimeScaleUnits();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == long.class || csType == Long.class || isTimeType(csType)) {
                return new LongRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingMinMaxOperator(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax());
            } else if (Comparable.class.isAssignableFrom(csType)) {
                return new ComparableRollingMinMaxOperator<>(pair, affectingColumns,
                        rmm.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rmm.isMax(), csType);
            }

            throw new IllegalArgumentException("Can not perform Rolling Min/Max on type " + csType);
        }

        private UpdateByOperator makeRollingProductOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingProductSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingProductOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            }

            throw new IllegalArgumentException("Can not perform RollingProduct on type " + csType);
        }

        private UpdateByOperator makeRollingCountOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingCountSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == boolean.class || csType == Boolean.class) {
                return new ByteRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else {
                return new ObjectRollingCountOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            }
        }

        private UpdateByOperator makeRollingStdOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingStdSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == Boolean.class || csType == boolean.class) {
                return new ByteRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BOOLEAN_AS_BYTE);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, NULL_BYTE);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits);
            } else if (csType == BigDecimal.class) {
                return new BigDecimalRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            } else if (csType == BigInteger.class) {
                return new BigIntegerRollingStdOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, control.mathContextOrDefault());
            }

            throw new IllegalArgumentException("Can not perform RollingStd on type " + csType);
        }

        private UpdateByOperator makeRollingWAvgOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingWAvgSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final ColumnDefinition<?> weightColumnDef = tableDef.getColumn(rs.weightCol());
            final Class<?> weightCsType = weightColumnDef.getDataType();

            if (!rs.weightColumnApplicableTo(weightCsType)) {
                throw new IllegalArgumentException("Can not perform RollingWAvg on weight column type " + weightCsType);
            }

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn, rs.weightCol()};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn, rs.weightCol()};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            if (csType == BigDecimal.class || csType == BigInteger.class ||
                    weightCsType == BigDecimal.class || weightCsType == BigInteger.class) {
                // We need to produce a BigDecimal result output. All input columns will be cast to BigDecimal so
                // there is no distinction between input types.
                return new BigDecimalRollingWAvgOperator<>(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol(), control.mathContextOrDefault());
            }

            if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingWAvgOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, rs.weightCol());
            }

            throw new IllegalArgumentException("Can not perform RollingWAvg on type " + csType);
        }

        private UpdateByOperator makeRollingFormulaOperator(@NotNull final MatchPair pair,
                @NotNull final TableDefinition tableDef,
                @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
                @NotNull final RollingFormulaSpec rs) {
            final ColumnDefinition<?> columnDef = tableDef.getColumn(pair.rightColumn);
            final Class<?> csType = columnDef.getDataType();

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = new String[] {pair.rightColumn};
            } else {
                affectingColumns = new String[] {rs.revWindowScale().timestampCol(), pair.rightColumn};
            }

            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            final String formula = rs.formula();
            // noinspection deprecation
            final String paramToken = rs.paramToken().orElseThrow();

            if (csType == boolean.class || csType == Boolean.class) {
                return new BooleanRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == byte.class || csType == Byte.class) {
                return new ByteRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == char.class || csType == Character.class) {
                return new CharRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == short.class || csType == Short.class) {
                return new ShortRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == int.class || csType == Integer.class) {
                return new IntRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == long.class || csType == Long.class) {
                return new LongRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == float.class || csType == Float.class) {
                return new FloatRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            } else if (csType == double.class || csType == Double.class) {
                return new DoubleRollingFormulaOperator(pair, affectingColumns,
                        rs.revWindowScale().timestampCol(),
                        prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                        formulaColumnMap, tableDef, compilationProcessor);
            }
            return new ObjectRollingFormulaOperator<>(pair, affectingColumns,
                    rs.revWindowScale().timestampCol(),
                    prevWindowScaleUnits, fwdWindowScaleUnits, formula, paramToken,
                    formulaColumnMap, tableDef, compilationProcessor);
        }

        private UpdateByOperator makeRollingFormulaMultiColumnOperator(
                @NotNull final TableDefinition tableDef,
                @NotNull final RollingFormulaSpec rs) {
            final long prevWindowScaleUnits = rs.revWindowScale().getTimeScaleUnits();
            final long fwdWindowScaleUnits = rs.fwdWindowScale().getTimeScaleUnits();

            final Map<String, ColumnDefinition<?>> columnDefinitionMap = tableDef.getColumnNameMap();

            // Create the colum
            final SelectColumn selectColumn = SelectColumn.of(Selectable.parse(rs.formula()));

            // Get or create a column definition map composed of vectors of the original column types (or scalars when
            // part of the group_by columns).
            final Set<String> groupByColumnSet =
                    Arrays.stream(groupByColumns).map(MatchPair::rightColumn).collect(Collectors.toSet());
            if (vectorColumnDefinitions == null) {
                vectorColumnDefinitions = tableDef.getColumnStream().collect(Collectors.toMap(
                        ColumnDefinition::getName,
                        (final ColumnDefinition<?> cd) -> groupByColumnSet.contains(cd.getName())
                                ? cd
                                : ColumnDefinition.fromGenericType(
                                        cd.getName(),
                                        VectorFactory.forElementType(cd.getDataType()).vectorType(),
                                        cd.getDataType())));
            }

            // Get the input column names from the formula and provide them to the rolling formula operator
            final String[] allInputColumns =
                    selectColumn.initDef(vectorColumnDefinitions, compilationProcessor).toArray(String[]::new);
            if (!selectColumn.getColumnArrays().isEmpty()) {
                throw new IllegalArgumentException("RollingFormulaMultiColumnOperator does not support column arrays ("
                        + selectColumn.getColumnArrays() + ")");
            }
            if (selectColumn.hasVirtualRowVariables()) {
                throw new IllegalArgumentException("RollingFormula does not support virtual row variables");
            }

            final Map<Boolean, List<String>> partitioned = Arrays.stream(allInputColumns)
                    .collect(Collectors.partitioningBy(groupByColumnSet::contains));
            final String[] inputKeyColumns = partitioned.get(true).toArray(String[]::new);
            final String[] inputNonKeyColumns = partitioned.get(false).toArray(String[]::new);

            final Class<?>[] inputKeyColumnTypes = new Class[inputKeyColumns.length];
            final Class<?>[] inputKeyComponentTypes = new Class[inputKeyColumns.length];
            for (int i = 0; i < inputKeyColumns.length; i++) {
                final ColumnDefinition<?> columnDef = columnDefinitionMap.get(inputKeyColumns[i]);
                inputKeyColumnTypes[i] = columnDef.getDataType();
                inputKeyComponentTypes[i] = columnDef.getComponentType();
            }

            final Class<?>[] inputNonKeyColumnTypes = new Class[inputNonKeyColumns.length];
            final Class<?>[] inputNonKeyVectorTypes = new Class[inputNonKeyColumns.length];
            for (int i = 0; i < inputNonKeyColumns.length; i++) {
                final ColumnDefinition<?> columnDef = columnDefinitionMap.get(inputNonKeyColumns[i]);
                inputNonKeyColumnTypes[i] = columnDef.getDataType();
                inputNonKeyVectorTypes[i] = vectorColumnDefinitions.get(inputNonKeyColumns[i]).getDataType();
            }

            final String[] affectingColumns;
            if (rs.revWindowScale().timestampCol() == null) {
                affectingColumns = inputNonKeyColumns;
            } else {
                affectingColumns = ArrayUtils.add(inputNonKeyColumns, rs.revWindowScale().timestampCol());
            }

            // Create a new column pair with the same name for the left and right columns
            final MatchPair pair = new MatchPair(selectColumn.getName(), selectColumn.getName());

            return new RollingFormulaMultiColumnOperator(
                    pair,
                    affectingColumns,
                    rs.revWindowScale().timestampCol(),
                    prevWindowScaleUnits,
                    fwdWindowScaleUnits,
                    selectColumn,
                    inputKeyColumns,
                    inputKeyColumnTypes,
                    inputKeyComponentTypes,
                    inputNonKeyColumns,
                    inputNonKeyColumnTypes,
                    inputNonKeyVectorTypes);
        }
    }
}
