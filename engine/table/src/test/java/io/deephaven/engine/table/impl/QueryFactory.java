/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.time.DateTime;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to generate query strings. You can use the along with the getTablePreamble() function to create queries.
 * <p>
 * <p>
 * To use on a remote server jar up this class and stick it in /etc/sysconfig/deephaven.d/java_lib/ on all nodes.
 * <p>
 * Example:
 * <p>
 * cd engine/table/build/classes/java/test/ jar cfv QueryFactory.jar io/deephaven/engine/table/QueryFactory*
 * <p>
 * This will get you a QueryFactory jar you can use on the remote servers.
 * <p>
 * Once you have that you can run it in a script like this:
 * <p>
 * <p>
 * <p>
 * import io.deephaven.engine.table.impl.QueryFactory
 * <p>
 * qf = new QueryFactory() queryPart1 = qf.getTablePreamble(123L) queryPart2 = qf.generateQuery(456L)
 * <p>
 * __groovySession.evaluate(queryPart1) __groovySession.evaluate(queryPart2)
 */
public class QueryFactory {

    // Names of the tables
    private final String tableNameOne;
    private final String tableNameTwo;

    private final int numberOfOperations;
    private final boolean finalOperationChangesTypes;
    private final boolean doSelectHalfWay;
    private final boolean doJoinMostOfTheWayIn;
    private final ArrayList<String> numericColumns;
    private final ArrayList<String> nonNumericColumns;
    private final String[] columnNames;
    private final Class[] columnTypes;

    // wrapping up a bunch of operations into object for easy modification.
    // Needs to have keys: supportedOps, safeBy, safeAgg, changingBy, changingAgg
    private final Map<String, String[]> switchControlValues;

    // default values
    private static final String DEFAULT_TABLE_ONE = "randomValues";
    private static final String DEFAULT_TABLE_TWO = "tickingValues";
    private static final String[] DEFAULT_COLUMN_NAMES = {"Timestamp", "MyString", "MyInt", "MyLong", "MyFloat",
            "MyDouble", "MyBoolean", "MyChar", "MyShort", "MyByte", "MyBigDecimal", "MyBigInteger"};
    private static final Class[] DEFAULT_COLUMN_TYPES =
            {DateTime.class, String.class, Integer.class, Long.class, Float.class, Double.class, Boolean.class,
                    Character.class, short.class, byte.class, java.math.BigDecimal.class, java.math.BigInteger.class};
    // Copy and modify this block of code if you want to disable an operation.
    private static final String[] IMPLEMENTED_OPS = {"where", "merge", "flatten", "slice", "head", "tail", "headPct",
            "tailPct", "reverse", "sort", "byOpp", "aggCombo", "partitionBy"};
    private static final String[] CHANGING_AGG =
            {"AggSum", "AggVar", "AggStd", "AggGroup", "AggCount", "AggWAvg", "AggDistinct", "AggCountDistinct"};
    private static final String[] CHANGING_BY =
            {"avgBy", "sumBy", "stdBy", "varBy", "countBy", "medianBy", "percentileBy"};
    private static final String[] ROLLUP_AGG =
            {"AggSum", "AggVar", "AggStd", "AggCount", "AggMin", "AggMax", "AggFirst", "AggLast"};
    private static final String[] SAFE_AGG = {"AggMin", "AggMax", "AggFirst", "AggLast"};
    private static final String[] SAFE_BY = {"maxBy", "minBy", "firstBy", "lastBy", "sortedFirstBy", "sortedLastBy"};
    // TODO (https://github.com/deephaven/deephaven-core/issues/64): Re-enable tree
    // TODO (https://github.com/deephaven/deephaven-core/issues/65): Re-enable rollup
    private static final String[] FINAL_OPS =
            {"selectDistinct", "byOperation", "aggCombo", /* "tree", "rollup", */ "applyToAllBy"};
    private static final HashMap<String, String[]> DEFAULT_SWITCH_CONTROL = new HashMap<String, String[]>() {
        {
            put("supportedOps", IMPLEMENTED_OPS);
            put("changingAgg", CHANGING_AGG);
            put("changingBy", CHANGING_BY);
            put("safeAgg", SAFE_AGG);
            put("safeBy", SAFE_BY);
            put("rollupAgg", ROLLUP_AGG);
            put("finalOps", FINAL_OPS);

        }
    };
    private static final Set<Class> NUMERIC_TYPES = new HashSet<Class>() {
        {
            add(Integer.class);
            add(int.class);
            add(Long.class);
            add(long.class);
            add(Float.class);
            add(float.class);
            add(Double.class);
            add(double.class);
            add(short.class);
            add(Short.class);
            add(byte.class);
            add(Byte.class);
            add(java.math.BigDecimal.class);
            add(java.math.BigInteger.class);
        }
    };

    @SuppressWarnings("WeakerAccess")
    public QueryFactory(int numberOfOperations, boolean finalOperationChangesTypes, boolean doSelectHalfWay,
            boolean doJoinMostOfTheWayIn, String firstTableName, String secondTableName, String[] columnNames,
            Class[] columnTypes, Map<String, String[]> switchControlValues) {
        this.numberOfOperations = numberOfOperations;
        this.finalOperationChangesTypes = finalOperationChangesTypes;
        this.doSelectHalfWay = doSelectHalfWay;
        this.doJoinMostOfTheWayIn = doJoinMostOfTheWayIn;
        tableNameOne = firstTableName;
        tableNameTwo = secondTableName;
        this.switchControlValues = switchControlValues;
        numericColumns = new ArrayList<>();
        nonNumericColumns = new ArrayList<>();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;

        for (int columnNum = 0; columnNum < columnNames.length; ++columnNum) {
            Class type = columnTypes[columnNum];
            if (NUMERIC_TYPES.contains(type)) {
                numericColumns.add(columnNames[columnNum]);
            } else {
                nonNumericColumns.add(columnNames[columnNum]);
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    public QueryFactory(int numberOfOperations, boolean finalOperationChangesTypes, boolean doSelectHalfWay) {
        this(numberOfOperations, finalOperationChangesTypes, doSelectHalfWay, true, DEFAULT_TABLE_ONE,
                DEFAULT_TABLE_TWO, DEFAULT_COLUMN_NAMES, DEFAULT_COLUMN_TYPES, DEFAULT_SWITCH_CONTROL);
    }

    @SuppressWarnings("WeakerAccess")
    public QueryFactory(int numberOfOperations) {
        this(numberOfOperations, true, true);
    }

    public QueryFactory() {
        this(10);
    }

    /**
     * Turn a String [x,y,z] array into "x","y","z"
     *
     * @param values String collection you want to use.
     * @return Single string with the above format.
     */
    private String stringArrayToMultipleStringArgumentList(Collection<String> values) {
        return values.stream().map(x -> "\"" + x + "\"").collect(Collectors.joining(","));
    }

    /**
     * turn a string [x,y,z] into "x,y,z"
     *
     * @param values String collection you want to use.
     * @return Single string with the above format.
     */
    private String stringArrayToSingleArgumentList(Collection<String> values) {
        return "\"" + values.stream().collect(Collectors.joining(",")) + "\"";
    }

    /**
     * Adds the string "table[seedName]_opNum = table[seedName]_opNum-1" to the StringBuilder
     */
    private void addNormalTableSegment(StringBuilder opChain, String nameSeed, int opNum) {
        opChain.append("table").append(nameSeed).append("_").append(opNum).append(" = table").append(nameSeed)
                .append("_").append(opNum - 1);
    }

    /**
     * Create a new query from a given long seed value. This query will always be the same for the given seed value.
     *
     * @param seed Seed you want to use.
     * @return String query
     */
    public String generateQuery(long seed) {
        final Random queryRandom = new Random(seed);

        final String nameSeed = Long.toString(Math.abs(seed));// Name can't have a "-" in it.
        final StringBuilder opChain = new StringBuilder(2000);
        opChain.append("table").append(nameSeed).append("_0 = ");
        if (queryRandom.nextInt(2) == 0) {
            opChain.append(tableNameOne).append(".select();\n");
        } else {
            opChain.append(tableNameTwo).append(".select();\n");

        }
        opChain.append("table").append(nameSeed).append("_0.setAttribute(\"TESTSEED\",").append(seed).append(");\n");

        for (int opNum = 1; opNum <= numberOfOperations; ++opNum) {

            // Select half way
            if (doSelectHalfWay && opNum == numberOfOperations / 2) {
                addSelectOperation(opNum, opChain, nameSeed);

                continue;
            }

            // Type changing final operation
            if (finalOperationChangesTypes && opNum == numberOfOperations) {
                // Pick by or combo style
                String operation = this.switchControlValues.get("finalOps")[queryRandom
                        .nextInt(switchControlValues.get("finalOps").length)];
                switch (operation) {
                    case "selectDistinct":
                        addNormalTableSegment(opChain, nameSeed, opNum);
                        String col = columnNames[queryRandom.nextInt(columnNames.length)];
                        opChain.append(".selectDistinct(\"").append(col).append("\");\n");
                        break;
                    case "byOperation":
                        addByOperation(opNum, opChain, queryRandom, switchControlValues.get("changingBy"), nameSeed);
                        break;
                    case "aggCombo":
                        addComboOperation(opNum, opChain, queryRandom, switchControlValues.get("changingAgg"),
                                nameSeed);
                        break;
                    case "tree":
                        addTreeTableOperation(opNum, opChain, queryRandom, nameSeed);
                        break;
                    case "rollup":
                        addRollupOperation(opNum, opChain, queryRandom, nameSeed);
                        break;
                    case "applyToAllBy":
                        addApplyToAllByOperation(opNum, opChain, queryRandom, nameSeed);
                        break;
                    default:
                        throw new RuntimeException("Have a bug in the final operation code.");
                }
                continue;
            }

            if (doJoinMostOfTheWayIn && opNum == numberOfOperations * 3 / 4) {
                // pick a join style
                if (queryRandom.nextInt(2) == 0) {
                    addJoinOperation(opNum, opChain, queryRandom, nameSeed);
                } else {
                    addNaturalJoinOperation(opNum, opChain, queryRandom, nameSeed);
                }
                continue;
            }

            // Add a normal operation
            addNormalOperation(opNum, opChain, queryRandom, nameSeed);

            // TODO add attribute string
        }

        // TODO add validators
        return opChain.toString();
    }


    private void addSelectOperation(int opNum, StringBuilder opChain, String nameSeed) {

        final String nextTableName = "table" + nameSeed + "_" + opNum;
        final String lastTableName = "table" + nameSeed + "_" + (opNum - 1);
        opChain.append(nextTableName).append("prime = ");
        opChain.append("io.deephaven.engine.table.impl.SelectOverheadLimiter.clampSelectOverhead(")
                .append(lastTableName)
                .append(", 10.0d);\n");
        opChain.append(nextTableName).append(" = ").append(nextTableName).append("prime").append(".select();\n");
    }

    private void addMergeOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        final int style = random.nextInt(10);// Make the old style rare
        if (style != 0) {
            // make sure we have at least some values
            opChain.append("partitioned = merge(table").append(nameSeed).append("_").append(opNum - 1).append(", ");
            opChain.append(tableNameOne).append(".head(1L)").append(", ");
            opChain.append(tableNameTwo).append(".head(1L)").append(")");

            opChain.append(".partitionBy(\"").append(columnNames[random.nextInt(columnNames.length)]).append("\");\n");
            opChain.append("table").append(nameSeed).append("_").append(opNum).append(" = ");
            opChain.append("partitioned.merge();\n");
        } else {
            opChain.append("table").append(nameSeed).append("_").append(opNum).append(" = merge(");
            if (random.nextInt(2) == 0) {
                opChain.append("table").append(nameSeed).append("_").append(opNum - 1).append(",").append(tableNameTwo);
            } else {
                opChain.append("table").append(nameSeed).append("_").append(opNum - 1).append(",").append(tableNameOne);

            }
            opChain.append(");\n");
        }
    }

    private void addByOperation(int opNum, StringBuilder opChain, Random random, String[] possibleByOperations,
            String nameSeed) {
        final String operation = possibleByOperations[random.nextInt(possibleByOperations.length)];
        final int numberOfColumns = random.nextInt(3) + 1;
        final List<String> anyCols = new ArrayList<>();
        final List<String> numericCols = new ArrayList<>();
        final List<String> colSet = new ArrayList<>(Arrays.asList(columnNames));
        final List<String> numericColSet = new ArrayList<>(numericColumns);
        addNormalTableSegment(opChain, nameSeed, opNum);

        Collections.shuffle(colSet, random);
        Collections.shuffle(numericColSet, random);

        for (int colNum = 0; colNum < numberOfColumns; ++colNum) {
            anyCols.add(colSet.get(colNum));
            numericCols.add(numericColSet.get(colNum));
        }

        switch (operation) {

            case "avgBy":
                opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(nonNumericColumns))
                        .append(").avgBy(").append(stringArrayToMultipleStringArgumentList(numericCols)).append(");\n");
                break;

            case "sumBy":
                opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(nonNumericColumns))
                        .append(").sumBy(").append(stringArrayToMultipleStringArgumentList(numericCols)).append(");\n");
                break;

            case "stdBy":
                opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(nonNumericColumns))
                        .append(").stdBy(").append(stringArrayToMultipleStringArgumentList(numericCols)).append(");\n");
                break;

            case "varBy":
                opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(nonNumericColumns))
                        .append(").varBy(").append(stringArrayToMultipleStringArgumentList(numericCols)).append(");\n");
                break;

            case "medianBy":
                opChain.append(".medianBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "countBy":
                opChain.append(".countBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "maxBy":
                opChain.append(".maxBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "minBy":
                opChain.append(".minBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "firstBy":
                opChain.append(".firstBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "lastBy":
                opChain.append(".lastBy(").append(stringArrayToMultipleStringArgumentList(anyCols)).append(");\n");
                break;

            case "sortedFirstBy":
                opChain.append(".aggBy(List.of(AggSortedFirst(List.of(")
                        .append(stringArrayToMultipleStringArgumentList(anyCols))
                        .append("), ")
                        .append(stringArrayToMultipleStringArgumentList(colSet))
                        .append(")));\n");
                break;

            case "sortedLastBy":
                opChain.append(".aggBy(List.of(AggSortedLast(List.of(")
                        .append(stringArrayToMultipleStringArgumentList(anyCols))
                        .append("), ")
                        .append(stringArrayToMultipleStringArgumentList(colSet))
                        .append(")));\n");
                break;

            case "percentileBy":
                opChain.append(".aggBy(List.of(AggPct(0.25, ")
                        .append(stringArrayToMultipleStringArgumentList(colSet))
                        .append(")));\n");
                break;

            default:
                throw new RuntimeException("By operation(" + operation + ") not found in switch statement");
        }
    }

    private void addApplyToAllByOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        addNormalTableSegment(opChain, nameSeed, opNum);
        // getDirect() is required for previous value to work. see IDS-6257
        opChain.append(".applyToAllBy(\"each.getDirect().subVector(0L,1L)\",");
        final int numOfColumns = random.nextInt(3) + 1;

        final ArrayList<String> colSet = new ArrayList<>(Arrays.asList(columnNames));

        Collections.shuffle(colSet, random);

        for (int colNum = 0; colNum < numOfColumns; ++colNum) {
            opChain.append("\"").append(colSet.get(colNum)).append("\",");
        }
        opChain.deleteCharAt(opChain.length() - 1);
        opChain.append(");\n");

    }

    private void addRollupOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        addNormalTableSegment(opChain, nameSeed, opNum);
        final ArrayList<String> aggSet = new ArrayList<>(Arrays.asList(switchControlValues.get("rollupAgg")));
        final ArrayList<String> activeAggs = new ArrayList<>();
        final ArrayList<String> colSet = new ArrayList<>(numericColumns);
        final int numOfAggs = random.nextInt(colSet.size() - 1) + 1;

        Collections.shuffle(colSet, random);
        Collections.shuffle(aggSet, random);
        for (int aggNum = 0; aggNum < numOfAggs; ++aggNum) {
            activeAggs.add(aggSet.get(aggNum));
        }

        opChain.append(".rollup(List.of(");
        int columnNumber = 0;
        for (String agg : activeAggs) {
            opChain.append(agg).append("(\"").append(colSet.get(columnNumber)).append("\"),");
            ++columnNumber;
        }
        opChain.deleteCharAt(opChain.length() - 1);
        colSet.addAll(nonNumericColumns);
        opChain.append("),");
        for (; columnNumber < colSet.size(); ++columnNumber) {
            opChain.append("\"").append(colSet.get(columnNumber)).append(("\","));
        }
        opChain.deleteCharAt(opChain.length() - 1);
        opChain.append(");\n");

    }

    private void addTreeTableOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        StringBuilder previousTableName = new StringBuilder("table").append(nameSeed).append("_").append(opNum - 1);
        String columnName = columnNames[random.nextInt(columnNames.length)];

        opChain.append("part2 = ").append(previousTableName).append(".selectDistinct(\"").append(columnName)
                .append("\").update(\"Parent= (String) null\",\"ID= `T`+").append(columnName).append("\")");
        opChain.append(".update(");
        for (int colNumber = 0; colNumber < columnNames.length; ++colNumber) {
            opChain.append("\"").append(columnNames[colNumber]).append(" = (").append(columnTypes[colNumber].getName())
                    .append(") null \",");
        }
        opChain.deleteCharAt(opChain.length() - 1);
        opChain.append(");\n");

        opChain.append("atomicLong_").append(nameSeed).append(" = new java.util.concurrent.atomic.AtomicLong();\n");
        opChain.append("part1 = ").append(previousTableName).append(".update(\"ID=`a`+atomicLong_").append(nameSeed)
                .append(".getAndIncrement()\",");


        opChain.append(" \"Parent = `T`+").append(columnName).append("\");\n");

        opChain.append("tree").append(nameSeed).append(" = merge(part1,part2).tree(\"ID\",\"Parent\");\n");
    }

    private void addPartitionByOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        final StringBuilder partitionedName =
                new StringBuilder("partitioned").append(nameSeed).append("_").append(opNum);
        final StringBuilder previousTableName =
                new StringBuilder("table").append(nameSeed).append("_").append(opNum - 1);
        opChain.append(partitionedName).append(" = ").append(previousTableName).append(".partitionBy(\"")
                .append(columnNames[random.nextInt(columnNames.length)]).append("\");\n");
        opChain.append("table").append(nameSeed).append("_").append(opNum).append(" = ");
        opChain.append(partitionedName).append(".table().size() == 0 ? ").append(previousTableName).append(" : ");
        opChain.append(partitionedName).append(".table().getColumnSource(")
                .append(partitionedName).append(".constituentColumnName()").append(").get(")
                .append(partitionedName).append(".table().getRowSet().firstRowKey());\n");
    }

    private void addJoinOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        addNormalTableSegment(opChain, nameSeed, opNum);
        String joinCol = columnNames[random.nextInt(columnNames.length)];
        ArrayList<String> colsToJoin = new ArrayList<>();
        for (String col : columnNames) {
            if (!joinCol.equals(col) && random.nextInt(2) == 0) {
                colsToJoin.add(col);
            }
        }

        // Make sure we have at least one column
        if (colsToJoin.size() == 0) {
            if (columnNames[0].equals(joinCol))
                colsToJoin.add(columnNames[columnNames.length - 1]);
            else
                colsToJoin.add(columnNames[0]);
        }

        opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(colsToJoin))
                .append(").flatten().join(");
        if (random.nextInt(2) == 0) {
            opChain.append(tableNameOne);

        } else {
            opChain.append(tableNameTwo);
        }

        opChain.append(",\"").append(joinCol).append("\",").append(stringArrayToSingleArgumentList(colsToJoin))
                .append(");\n");
    }

    private void addNaturalJoinOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        final ArrayList<String> columnsToDrop = new ArrayList<>();
        final ArrayList<String> columnsToMatch = new ArrayList<>();

        final int dropNum = random.nextInt(3) + 1;
        final int matchNum = random.nextInt(3) + 1;

        final ArrayList<String> colSet = new ArrayList<>(Arrays.asList(columnNames));
        Collections.shuffle(colSet, random);
        int columnNumber = 0;
        for (; columnNumber < dropNum; ++columnNumber) {
            columnsToDrop.add(colSet.get(columnNumber));
        }
        for (; columnNumber < dropNum + matchNum; ++columnNumber) {
            columnsToMatch.add(colSet.get(columnNumber));
        }

        addNormalTableSegment(opChain, nameSeed, opNum);
        opChain.append(".dropColumns(").append(stringArrayToMultipleStringArgumentList(columnsToDrop)).append(")");
        opChain.append(".naturalJoin(table").append(nameSeed).append("_0.lastBy(")
                .append(stringArrayToMultipleStringArgumentList(columnsToMatch)).append("),");
        opChain.append(stringArrayToSingleArgumentList(columnsToMatch)).append(", ")
                .append(stringArrayToSingleArgumentList(columnsToDrop)).append(");\n");
    }

    private void addComboOperation(int opNum, StringBuilder opChain, Random random, String[] possiblyComboOperation,
            String nameSeed) {
        // combo style op
        // AggSum, AggVar, AggAvg, AggStd, AggGroup, AggCount
        ArrayList<String> aggSet = new ArrayList<>(Arrays.asList(possiblyComboOperation));
        boolean safeOp = true;

        Collections.shuffle(aggSet, random);
        int numOfAggs = random.nextInt(Math.min(numericColumns.size(), aggSet.size()) - 1) + 1;
        addNormalTableSegment(opChain, nameSeed, opNum);

        ArrayList<String> colSet = new ArrayList<>(numericColumns);

        Collections.shuffle(colSet, random);

        // Set up buckets
        ArrayList<ArrayList<String>> argLists = new ArrayList<>();
        ArrayList<String> activeAggs = new ArrayList<>();
        int columnIndex = 0;
        for (int aggNum = 0; aggNum < numOfAggs; ++aggNum) {
            activeAggs.add(aggSet.get(aggNum));
            argLists.add(new ArrayList<>());
        }

        activeAggs.add("NOP");
        ArrayList<String> NOOPColumns = new ArrayList<>(nonNumericColumns);
        // NOOPColumns.add("Timestamp");
        argLists.add(NOOPColumns);

        // Add columns to buckets.
        // Make sure each bucket has at least one column
        for (int aggNum = 0; aggNum < activeAggs.size(); ++aggNum) {
            argLists.get(aggNum).add(colSet.get(columnIndex));
            ++columnIndex;
        }

        for (; columnIndex < colSet.size(); ++columnIndex) {
            argLists.get(random.nextInt(argLists.size())).add(colSet.get(columnIndex));
        }

        opChain.append(".aggBy(List.of(");
        for (int aggNum = 0; aggNum < activeAggs.size(); ++aggNum) {
            switch (activeAggs.get(aggNum)) {
                case "AggSum":
                    opChain.append("AggSum(");
                    safeOp = false;
                    break;

                case "AggVar":
                    opChain.append("AggVar(");
                    safeOp = false;
                    break;

                case "AggStd":
                    opChain.append("AggStd(");
                    safeOp = false;
                    break;

                case "AggGroup":
                    opChain.append("AggGroup(");
                    safeOp = false;
                    break;

                case "AggCount":
                    opChain.append("AggCount(\"").append(argLists.get(aggNum).get(0)).append("\"),");
                    safeOp = false;
                    continue;

                case "AggDistinct":
                    opChain.append("AggDistinct(");
                    safeOp = false;
                    break;

                case "AggCountDistinct":
                    opChain.append("AggCountDistinct(");
                    safeOp = false;
                    break;

                case "AggMin":
                    opChain.append("AggMin(");
                    break;

                case "AggMax":
                    opChain.append("AggMax(");
                    break;

                case "AggFirst":
                    opChain.append("AggFirst(");
                    break;

                case "AggLast":
                    opChain.append("AggLast(");
                    break;

                case "AggPct":
                    opChain.append("AggPct(0.9d, ");
                    break;

                case "AggWAvg":
                    // Can't use BigInteger or BigDecimal on here. Create a new set for this. Make sure the columns are
                    // renamed
                    String[] wightedAverageCols = {"MyInt", "MyLong", "MyFloat", "MyDouble", "MyShort", "MyByte"};
                    ArrayList<String> otherColSet = new ArrayList<>(Arrays.asList(wightedAverageCols));
                    int otherColNum = 1;
                    int numOfColumns = random.nextInt(otherColSet.size() - 2) + 2; // Must be > 1
                    Collections.shuffle(otherColSet, random);
                    opChain.append("AggWAvg(\"").append(otherColSet.get(0)).append("\",");
                    for (; otherColNum < numOfColumns; ++otherColNum) {
                        String col = otherColSet.get(otherColNum);
                        opChain.append("\"other_").append(col).append(" = ").append(col).append("\",");
                    }
                    opChain.deleteCharAt(opChain.length() - 1);
                    opChain.append("),");
                    safeOp = false;
                    continue;

                case "NOP":
                    // These go into the join to get the columns back. Skip them
                    continue;

                default:
                    throw new RuntimeException("Have a bug in the aggCombo: " + activeAggs.get(aggNum) + " missing");

            }
            opChain.append(stringArrayToMultipleStringArgumentList(argLists.get(aggNum))).append("),");
        }

        opChain.deleteCharAt(opChain.length() - 1);
        opChain.append("))");
        if (safeOp)
            opChain.append(".join( table").append(nameSeed).append("_").append(opNum - 1).append(",\"")
                    .append(argLists.get(0).get(0)).append("\", ")
                    .append(stringArrayToSingleArgumentList(argLists.get(argLists.size() - 1))).append(");\n");
    }

    private String createWhereFilter(Random random) {
        final int colNum = random.nextInt(columnNames.length);
        final String colName = columnNames[colNum];

        // TODO add more filter variations.
        StringBuilder filter = new StringBuilder();
        switch (columnTypes[colNum].getSimpleName()) {

            case "DateTime":
                filter.append(colName).append(" > ").append(random.nextInt(1000) * 1_000_000_000L);
                break;

            case "String":
                filter.append(colName).append(" != `a").append(random.nextInt(100)).append("`");
                break;

            case "int":
            case "Integer":
            case "long":
            case "Long":
            case "short":
            case "Short":
                filter.append(colName).append(" > ").append(random.nextInt(500) - 250);
                break;

            case "float":
            case "Float":
            case "double":
            case "Double":
                filter.append(colName).append(" > ").append(250 * random.nextFloat());
                break;

            case "boolean":
            case "Boolean":
                filter.append(colName).append(" == ").append(random.nextBoolean());
                break;

            case "char":
            case "Character":
                filter.append("in(").append(colName).append(",'").append((char) (random.nextInt(27) + 97)).append("')");
                break;

            case "byte":
            case "Byte":
                filter.append(colName).append(" > ").append(random.nextInt(128) - 64);
                break;

            case "BigDecimal":
            case "BigInteger":
                filter.append(colName).append(" != null");
                break;

            default:
                throw new RuntimeException("Column type not found:" + columnTypes[colNum].getSimpleName());
        }

        return filter.toString();
    }

    private void addNormalOperation(int opNum, StringBuilder opChain, Random random, String nameSeed) {
        String operation =
                switchControlValues.get("supportedOps")[random.nextInt(switchControlValues.get("supportedOps").length)];

        switch (operation) {

            case "where":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".where(\"").append(createWhereFilter(random)).append("\");\n");
                break;

            case "merge":
                addMergeOperation(opNum, opChain, random, nameSeed);

                break;

            case "flatten":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".flatten();\n");
                break;

            case "naturalJoin":
                addNaturalJoinOperation(opNum, opChain, random, nameSeed);
                break;

            case "slice":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".slice(10L,-10L);\n");
                break;

            case "head":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".head(1000);\n");
                break;

            case "tail":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".tail(1000);\n");
                break;

            case "headPct":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".headPct(0.9);\n");
                break;

            case "tailPct":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".tailPct(0.90);\n");
                break;

            case "reverse":
                addNormalTableSegment(opChain, nameSeed, opNum);
                opChain.append(".reverse();\n");
                break;

            case "sort":
                addNormalTableSegment(opChain, nameSeed, opNum);
                String colToSort = columnNames[random.nextInt(columnNames.length)];
                if (random.nextInt(2) == 0) {
                    opChain.append(".sort(\"").append(colToSort).append("\");\n");
                } else {
                    opChain.append(".sortDescending(\"").append(colToSort).append("\");\n");
                }
                break;

            case "byOpp":
                addByOperation(opNum, opChain, random, this.switchControlValues.get("safeBy"), nameSeed);
                break;

            case "aggCombo":
                addComboOperation(opNum, opChain, random, switchControlValues.get("safeAgg"), nameSeed);
                break;

            case "join":
                addJoinOperation(opNum, opChain, random, nameSeed);
                break;

            case "partitionBy":
                addPartitionByOperation(opNum, opChain, random, nameSeed);
                break;

            default:
                throw new RuntimeException("No switch value for:" + operation);

        }
        // TODO set attributes
    }

    /**
     * Get a set of table to use with the fuzzer.
     * <p>
     * The seed determines the values of the randomValues table.
     * <p>
     * The ticking table's values are always the same.
     *
     * @param tableSeed The seed you want to use to create the random values table.
     * @return A string that contains all common things that are needed to use generateQuery()
     */
    public String getTablePreamble(Long tableSeed) {
        return "\n\nimport static io.deephaven.api.agg.Aggregation.*;\n" +
                "tableSeed = " + tableSeed + " as long;\n" +
                "size = 100 as int;\n" +
                "scale = 1000 as int;\n" +
                "useRandomNullPoints = true as boolean;\n" +
                "tableRandom = new Random(tableSeed) as Random;\n\n" +
                "columnRandoms = new Random[11] as Random[];\n" +
                "for(int colNum =0; colNum<11;++colNum) {\n" +
                "\tseed = tableRandom.nextLong();\n" +
                "\tSystem.out.println(\"column: \"+colNum+\"[Seed] \" + seed);\n" +
                "\tcolumnRandoms[colNum] = new Random(seed);\n" +
                "}\n\n" +
                "tt = timeTable(\"00:00:00.1\");" +
                "tickingValues = tt.update(\n" +
                "\"MyString=new String(`a`+i)\",\n" +
                "\"MyInt=new Integer(i)\",\n" +
                "\"MyLong=new Long(i)\",\n" +
                "\"MyDouble=new Double(i+i/10)\",\n" +
                "\"MyFloat=new Float(i+i/10)\",\n" +
                "\"MyBoolean=new Boolean(i%2==0)\",\n" +
                "\"MyChar= new Character((char) ((i%26)+97))\",\n" +
                "\"MyShort=new Short(Integer.toString(i%32767))\",\n" +
                "\"MyByte= new java.lang.Byte(Integer.toString(i%127))\",\n" +
                "\"MyBigDecimal= new java.math.BigDecimal(i+i/10)\",\n" +
                "\"MyBigInteger= new java.math.BigInteger(Integer.toString(i))\"\n" +
                ");\n" +
                "\n" +
                "nullPoints = new int[16] as int[];\n" +
                "if (useRandomNullPoints) {\n" +
                "\tfor (int k = 0; k < nullPoints.length; ++k) {\n" +
                "\t\tnullPoints[k] = tableRandom.nextInt(60) + 4;\n" +
                "\t}\n" +
                "} else {\n" +
                "\tnullPoints = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19] as int[];\n" +
                "}\n" +
                "\n" +
                "randomValues = emptyTable(size)\n" +
                ".update(\"Timestamp= i%nullPoints[0] == 0 ? null : new DateTime(i*1_000_000_000L)\")\n" +
                ".update(\"MyString=(i%nullPoints[1] == 0 ? null : `a`+ (columnRandoms[0].nextInt(scale*2) - scale) )\",\n"
                +
                "\"MyInt=(i%nullPoints[2] == 0 ? null : columnRandoms[1].nextInt(scale*2) - scale )\",\n" +
                "\"MyLong=(i%nullPoints[3] ==0 ? null : (long)(columnRandoms[2].nextInt(scale*2) - scale))\",\n" +
                "\"MyFloat=(float)(i%nullPoints[4] == 0 ? null : i%nullPoints[5] == 0 ? 1.0F/0.0F: i%nullPoints[6] == 0 ? -1.0F/0.0F : (columnRandoms[3].nextFloat()-0.5)*scale)\",\n"
                +
                "\"MyDouble=(double)(i%nullPoints[7] == 0 ? null : i%nullPoints[8] == 0 ? 1.0D/0.0D: i%nullPoints[9] == 0 ? -1.0D/0.0D : (columnRandoms[4].nextDouble()-0.5)*scale)\",\n"
                +
                "\"MyBoolean = (i%nullPoints[10] == 0 ? null : columnRandoms[5].nextBoolean())\",\n" +
                "\"MyChar = (i%nullPoints[11] == 0 ? null : new Character( (char) (columnRandoms[6].nextInt(27)+97) ) )\",\n"
                +
                "\"MyShort=(short)(i%nullPoints[12] == 0 ? null : columnRandoms[7].nextInt(scale*2) - scale )\",\n" +
                "\"MyByte=(Byte)(i%nullPoints[13] == 0 ? null : new Byte( Integer.toString( (int)( columnRandoms[8].nextInt(Byte.MAX_VALUE*2)-Byte.MAX_VALUE ) ) ) )\",\n"
                +
                "\"MyBigDecimal=(i%nullPoints[14] == 0 ? null : new java.math.BigDecimal( (columnRandoms[9].nextDouble()-0.5)*scale ))\",\n"
                +
                "\"MyBigInteger=(i%nullPoints[15] == 0 ? null : new java.math.BigInteger(Integer.toString(columnRandoms[10].nextInt(scale*2) - scale) ))\"\n"
                +
                ");\n\n";
    }
}
