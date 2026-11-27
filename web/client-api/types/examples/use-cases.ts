// Examples embedded in the guides. The build type-checks them and runs each against a server.
import { dh } from "@deephaven/jsapi-types";
declare const session: dh.IdeSession;

// #region createAndTransform
async function createAndTransform() {
    const t = await session.emptyTable(5).update(["X = i", "Y = X * 2"]);
    const data = await t.createSnapshot({ rows: { first: 0, last: 4 }, columns: t.columns });
    const y = t.findColumn("Y");
    const firstY: unknown = data.getData(0, y);
    t.close();
}
// #endregion createAndTransform

// #region tickingSorted
async function tickingSorted() {
    const t = await session.timeTable(1e9).update(["X = i"]).sort([{ column: "X", direction: "DESC" }]);
    const t2 = await session.timeTable(1e9).update(["X = i"]).sort(["X"]);
    t.close();
    t2.close();
}
// #endregion tickingSorted

// #region filterNamed
async function filterNamed() {
    const source = session.getObject({ type: "Table", name: "source" }) as dh.PendingTable<dh.Table>;
    // Filters name their columns, so the fetch need not be awaited first
    const filtered = await source.where([dh.FilterValue.ofColumn("X").greaterThan(dh.FilterValue.ofNumber(2))]);
    filtered.close();
}
// #endregion filterNamed

// #region naturalJoin
async function naturalJoin() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // For each left row, Value from the right row with the same Key, or null if there is none
    const joined = await left.naturalJoin(right, ["Key"], ["Value"]);
    joined.close();
}
// #endregion naturalJoin

// #region exactJoin
async function exactJoin() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // As naturalJoin, but every left row must have exactly one matching right row
    const joined = await left.exactJoin(right, ["Key"], ["Value"]);
    joined.close();
}
// #endregion exactJoin

// #region crossJoin
async function crossJoin() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // One row for every pair of left and right rows with the same Key; naturalJoin allows at most one right row
    const joined = await left.join(right, ["Key"], ["Value"]);
    joined.close();
}
// #endregion crossJoin

// #region asOfJoins
async function asOfJoins() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // left and right are never awaited, so both joins use them in this task: one batch, one await
    const [asOf, reverseAsOf] = await dh.resolveAll([
        // For each left row, the last right row with the same Key whose Ts is at or before the left row's
        left.aj(right, ["Key", "Ts"]),
        // For each left row, the first right row with the same Key whose Ts is at or after the left row's
        left.raj(right, ["Key", "Ts"]),
    ]);
    asOf.close();
    reverseAsOf.close();
}
// #endregion asOfJoins

// #region rangeJoin
async function rangeJoin() {
    // rangeJoin only supports static inputs, so neither table is a time table
    const t = session.emptyTable(30).update(["Key = i % 3", "Ts = i"]);
    const windows = session.emptyTable(3).update(["Key = i % 3", "LStart = i * 10", "LEnd = LStart + 5"]);
    // For each window, the Ts values of t's rows with the same Key from LStart through LEnd
    const ranged = await windows.rangeJoin(t, ["Key"], {
        leftStartColumn: "LStart",
        rangeStartRule: "LESS_THAN_OR_EQUAL",
        rightRangeColumn: "Ts",
        rangeEndRule: "GREATER_THAN_OR_EQUAL",
        leftEndColumn: "LEnd",
    }, [dh.agg.Aggregation.AggGroup(["Ts"])]);
    ranged.close();
}
// #endregion rangeJoin

// #region uniqueWithSentinel
async function uniqueWithSentinel() {
    // Key 0's rows all have X = 7; the other keys have two distinct values each
    const t = session.emptyTable(6).update(["Key = i % 3", "X = Key == 0 ? 7 : i", "L = (long) X"]);
    const unique = await t.aggBy({
        aggregations: [
            dh.agg.Aggregation.AggUnique(["X"], false, { type: "int", value: -1 }),
            { type: "Unique", columns: ["L"], nonUniqueSentinel: { type: "long", value: dh.LongWrapper.ofString("-1") } },
        ],
        groupByColumns: ["Key"],
    });
    const data = await unique.createSnapshot({ rows: { first: 0, last: 2 }, columns: unique.columns });
    const x = unique.findColumn("X");
    // Prints [ 7, -1, -1 ]: Key 0's unique value, then the sentinel for keys 1 and 2
    console.log(data.rows.map(row => row.get(x)));
    unique.close();
}
// #endregion uniqueWithSentinel

// #region aggregate
async function aggregate() {
    const t = session.timeTable(1e9).update(["Key = i % 3", "X = i"]);
    const [viaFactories, viaLiterals, counted] = await dh.resolveAll([
        t.aggBy({
            aggregations: [dh.agg.Aggregation.AggSum(["SumX = X"]), dh.agg.Aggregation.AggCount("N")],
            groupByColumns: ["Key"],
        }),
        t.aggBy({
            aggregations: [{ type: "Sum", columns: ["SumX = X"] }, { type: "Count", col: "N" }],
            groupByColumns: ["Key"],
        }),
        // Shorthand for aggBy with AggCount("N")
        t.countBy("N", ["Key"]),
    ]);
    viaFactories.close();
    viaLiterals.close();
    counted.close();
}
// #endregion aggregate

// #region updateBy
async function updateBy() {
    const t = await session.timeTable(1e9).update(["Key = i % 3", "X = i"]).updateBy({
        operations: [{ spec: { type: "UpdateByCumulativeSum" }, matchPairs: ["CumX = X"] }],
        groupByColumns: ["Key"],
    });
    t.close();
}
// #endregion updateBy

// #region rollingUpdateBy
async function rollingUpdateBy() {
    const t = await session.timeTable(1e9).update(["Key = i % 3", "X = i"]).updateBy({
        operations: [{
            spec: { type: "UpdateByRollingSum", reverseWindowScale: { type: "ticks", ticks: 5 } },
            matchPairs: ["SumX = X"],
        }],
        groupByColumns: ["Key"],
    });
    t.close();
}
// #endregion rollingUpdateBy

// #region weightedAverages
async function weightedAverages() {
    const t = session.emptyTable(12).update(["Key = i % 3", "X = i", "W = i % 4 + 1"]);
    // Chosen columns, renamed, beside other aggregations
    const averages = await t.aggBy({
        aggregations: [
            dh.agg.Aggregation.AggWAvg("W", ["WAvgX = X"]),
            dh.agg.Aggregation.AggAvg(["AvgX = X"]),
            dh.agg.Aggregation.AggCount("N"),
        ],
        groupByColumns: ["Key"],
    });
    averages.close();
}
// #endregion weightedAverages

// #region rollingWeightedAverage
async function rollingWeightedAverage() {
    const t = session.emptyTable(12).update(["Key = i % 3", "X = i", "W = i % 4 + 1"]);
    // Over each row and the two before it in its group
    const rolling = await t.updateBy({
        operations: [{
            spec: { type: "UpdateByRollingWAvg", weightColumn: "W", reverseWindowScale: { type: "ticks", ticks: 3 } },
            matchPairs: ["RollWAvgX = X"],
        }],
        groupByColumns: ["Key"],
    });
    rolling.close();
}
// #endregion rollingWeightedAverage

// #region filterByTable
async function filterByTable() {
    const t = session.emptyTable(30).update(["Key = i % 3", "Ts = i"]);
    const keys = session.emptyTable(2).update(["Key = i"]);
    // t's rows whose Key appears in keys, and t's rows whose Key does not
    const [inKeys, notInKeys] = await dh.resolveAll([
        t.whereIn(keys, ["Key"]),
        t.whereNotIn(keys, ["Key"]),
    ]);
    inKeys.close();
    notInKeys.close();
}
// #endregion filterByTable

// #region cancelAndAbort
// Abandoning work that is no longer wanted: cancel() a pending table, or abort the signal given to resolve()
async function cancelAndAbort(signal: AbortSignal) {
    const unwanted = session.timeTable(1e9).update(["X = i"]).sumBy([]);
    // Not sent if it has not been yet; awaiting it now rejects with "Operation canceled"
    unwanted.cancel();
    // If the signal aborts before the table arrives, the await rejects and nothing is left to close
    const t = await session.timeTable(1e9).update(["X = i"]).resolve({ signal });
    t.close();
}
// #endregion cancelAndAbort

// #region handleFailure
// A pending table has no catch(): use try/await, or wrap it with Promise.resolve in the task that made it
async function handleFailure() {
    try {
        const t = await session.emptyTable(1).update(["X = NoSuchColumn"]);
        t.close();
    } catch (e) {
        console.warn("update failed", e);
    }
    const orNull = await Promise.resolve(session.emptyTable(1).update(["X = NoSuchColumn"]))
        .catch(() => null);
    orNull?.close();
}
// #endregion handleFailure

// #region severalThatMayFail
// Several tables at once when any can fail: Promise.all would reject and leave the ones that arrived open.
// dh.resolveAll closes them instead, so a failure leaves nothing to clean up; one signal aborts them all.
async function severalThatMayFail(signal: AbortSignal) {
    const t = session.emptyTable(5).update(["X = i"]);
    let tables: dh.ResolvedTable[];
    try {
        tables = await dh.resolveAll([t.update(["Y = X * 2"]), t.update(["Z = NoSuchColumn"])], { signal });
    } catch (e) {
        console.warn("an update failed, or the signal aborted; no table is left open", e);
        return;
    }
    // ... use the tables ...
    tables.forEach(table => table.close());
}
// #endregion severalThatMayFail

// #region keepWhatSucceeded
// Some results wanted even when others fail: Promise.allSettled keeps each outcome, and each table that arrived is
// the caller's to close
async function keepWhatSucceeded() {
    const t = session.emptyTable(5).update(["X = i"]);
    const results = await Promise.allSettled([t.update(["Y = X * 2"]), t.update(["Z = NoSuchColumn"])]);
    const tables = results.flatMap(r => r.status === "fulfilled" ? [r.value] : []);
    // ... use the tables that arrived ...
    tables.forEach(table => table.close());
}
// #endregion keepWhatSucceeded

// #region readEither
// Reading any table, mutable or not: Table, TotalsTable and ResolvedTable share these members
function describe(t: dh.Table | dh.TotalsTable | dh.ResolvedTable): string {
    return `${t.size} rows: ${t.columns.map(c => c.name).join(", ")}`;
}
async function readEither() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    const resolved = await table.operations();
    console.log(describe(table), describe(resolved));
    resolved.close();
    table.close();
}
// #endregion readEither

// #region snapshot
async function snapshot() {
    const t = session.timeTable(1e9).update(["X = i"]);
    // A static copy of t as it is now, which does not tick
    const copy = await t.snapshot();
    copy.close();
}
// #endregion snapshot

// #region snapshotWhen
async function snapshotWhen() {
    const t = session.timeTable(1e9).update(["Key = i % 3"]);
    // A copy of t taken each time the trigger ticks. The trigger's columns are added to the result, so its Timestamp is
    // renamed to avoid t's
    const sampled = await t.snapshotWhen(session.timeTable(5e9), {
        initial: true,
        stampColumns: ["TriggerTs = Timestamp"],
    });
    sampled.close();
}
// #endregion snapshotWhen

// #region mergeTwo
async function mergeTwo() {
    const a = await session.emptyTable(2).update(["X = i"]);
    const b = await session.emptyTable(2).update(["X = i + 2"]);
    // mergeTables takes TableReference[], so resolved and pending tables can be mixed
    const merged = await session.mergeTables([a, b, session.emptyTable(1).update(["X = 99"])]);
    merged.close();
    a.close();
    b.close();
}
// #endregion mergeTwo

// #region readTable
// A function that reads a table it is lent and chains on it. It takes a ResolvedTable, which the caller resolved and
// still closes; it closes only what its own chain produced.
async function readTable(t: dh.ResolvedTable) {
    const n: number = t.size;
    const top = await t.head(10);
    top.close();
}
// #endregion readTable

// #region mutableTable
// Mixing a mutable dh.Table with table operations
async function mutableTable() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    table.applyFilter([]);
    const derived = await table.operations().update(["Z = X + 1"]);
    // derived already has every column of table, so name the one to add
    const joined = await derived.naturalJoin(table, ["Key"], ["OriginalX = X"]);
    const view = await table.operations();
    await readTable(view);
    view.close();
    table.close();
    derived.close();
    joined.close();
}
// #endregion mutableTable

// #region viewport
// Data access on a resolved chain result, viewport style
async function viewport() {
    const t = await session.timeTable(1e9).update(["X = i"]);
    const sub = t.createViewportSubscription({ rows: { first: 0, last: 9 }, columns: ["X"] });
    sub.close();
    t.close();
}
// #endregion viewport

// #region selectColumns
async function selectColumns() {
    const t = session.emptyTable(5).update(["Key = i % 2", "X = i"]);
    const tables = await dh.resolveAll([
        // view and select keep only the listed columns; updateView and update keep the others too.
        // view and updateView compute a formula each time a value is read; select and update compute it up front and
        // store the results.
        t.view(["Key", "Double = X * 2"]),
        t.select(["Key", "Double = X * 2"]),
        t.updateView(["Double = X * 2"]),
        t.update(["Double = X * 2"]),
    ]);
    for (const table of tables) {
        console.log(table.columns.map(c => c.name));
        table.close();
    }
    // Prints [ 'Key', 'Double' ] twice, then [ 'Key', 'X', 'Double' ] twice
}
// #endregion selectColumns

// #region headAndTail
async function headAndTail() {
    const t = session.emptyTable(10).update(["X = i"]);
    const [firstThree, lastThree] = await dh.resolveAll([t.head(3), t.tail(3)]);
    firstThree.close();
    lastThree.close();
}
// #endregion headAndTail

// #region sliceAndReverse
async function sliceAndReverse() {
    const t = session.emptyTable(10).update(["X = i"]);
    const [middle, reversed] = await dh.resolveAll([
        // Positions count from the start, or from the end when negative; the end position is excluded
        t.slice(1, -1),
        t.reverse(),
    ]);
    middle.close();
    reversed.close();
}
// #endregion sliceAndReverse

// #region selectDistinct
async function selectDistinct() {
    const t = session.emptyTable(10).update(["Key = i % 3", "X = i"]);
    // One row for each distinct Key
    const keys = await t.selectDistinct(["Key"]);
    keys.close();
}
// #endregion selectDistinct

// #region groupAndUngroup
async function groupAndUngroup() {
    const t = session.emptyTable(6).update(["Key = i % 3", "X = i"]);
    const [grouped, regrouped, flat] = await dh.resolveAll([
        // One row per key, with X holding that key's values as an array
        t.groupBy(["Key"]),
        // ungroup turns each array back into rows
        t.groupBy(["Key"]).ungroup(),
        // flatten renumbers a filtered table's rows so their keys are contiguous again
        t.where([dh.FilterValue.ofColumn("X").greaterThan(dh.FilterValue.ofNumber(2))]).flatten(),
    ]);
    grouped.close();
    regrouped.close();
    flat.close();
}
// #endregion groupAndUngroup

// #region shorthandAggregations
async function shorthandAggregations() {
    const t = session.emptyTable(12).update(["Key = i % 3", "X = i", "W = i % 4 + 1"]);
    // Each is shorthand for aggAllBy with one aggregation, applied to every column but the keys
    const tables = await dh.resolveAll([
        t.sumBy(["Key"]),
        t.firstBy(["Key"]),
        t.lastBy(["Key"]),
        t.minBy(["Key"]),
        t.maxBy(["Key"]),
        t.avgBy(["Key"]),
        t.medianBy(["Key"]),
        t.stdBy(["Key"]),
        t.varBy(["Key"]),
        t.absSumBy(["Key"]),
        // Every column but the keys and W, weighted by W
        t.wavgBy("W", ["Key"]),
        t.wsumBy("W", ["Key"]),
    ]);
    tables.forEach(table => table.close());
}
// #endregion shorthandAggregations

// #region aggregateAll
async function aggregateAll() {
    const t = session.emptyTable(6).update(["Key = i % 3", "X = i", "Y = i * 2"]);
    // One aggregation over every column but the keys, so it names no columns
    const maxes = await t.aggAllBy(dh.agg.Aggregation.AggMax(), ["Key"]);
    maxes.close();
}
// #endregion aggregateAll


// #region downsampleForChart
// Keeps only the rows needed to draw each Y column as a line across a chart 800 pixels wide
async function downsampleForChart() {
    const t = session.timeTable(1e9).update(["Y = i"]);
    const small = await t.downsample({ xColumn: "Timestamp", yColumns: ["Y"], width: 800 });
    small.close();
}
// #endregion downsampleForChart

// #region readValues
async function readValues() {
    const t = await session.emptyTable(1).update([
        "L = 9007199254740993L",
        "When = parseInstant(`2026-01-02T03:04:05.123456789Z`)",
        "Day = parseLocalDate(`2026-01-02`)",
        "Time = parseLocalTime(`03:04:05`)",
        "Price = java.math.BigDecimal.valueOf(1234567, 2)",
        "Big = java.math.BigInteger.TWO.pow(70)",
    ]);
    const data = await t.createSnapshot({ rows: { first: 0, last: 0 }, columns: t.columns });
    const cell = (name: string) => data.getData(0, t.findColumn(name));

    // 2^53 + 1: exact as a string, while asNumber() gives the nearest double
    const long = cell("L") as dh.LongWrapper;
    const exact: string = long.toString();
    const approx: number = long.asNumber();
    // Prints 9007199254740993 9007199254740992
    console.log(exact, approx);

    // A JS Date keeps milliseconds; the wrapper keeps the nanoseconds
    const when = cell("When") as dh.DateWrapper;
    const date: Date = when.asDate();
    const formatted = dh.i18n.DateTimeFormat.format("yyyy-MM-dd HH:mm:ss.SSSSSSSSS", when,
        dh.i18n.TimeZone.getTimeZone("America/New_York"));
    // Prints 2026-01-02T03:04:05.123Z 2026-01-01 22:04:05.123456789
    console.log(date.toISOString(), formatted);

    const day = cell("Day") as dh.LocalDateWrapper;
    const month: number = day.getMonthValue();
    const time = cell("Time") as dh.LocalTimeWrapper;
    const hour: number = time.getHour();
    // Prints 2026-01-02 1 03:04:05.000000000 3
    console.log(day.toString(), month, time.toString(), hour);

    const price = cell("Price") as dh.BigDecimalWrapper;
    const priceText = dh.i18n.NumberFormat.format("#,##0.00", price);
    const big = (cell("Big") as dh.BigIntegerWrapper).toString();
    // Prints 12,345.67 1180591620717411303424
    console.log(priceText, big);
    t.close();
}
// #endregion readValues

// #region previewArrays
async function previewArrays() {
    const t = await session.emptyTable(2).update(["Values = new int[] {1, 2, 3, 4, 5, 6}"]);
    // At most three elements of each array, plus one more when it is longer, to show there are more
    const data = await t.createSnapshot({
        rows: { first: 0, last: 1 },
        columns: t.columns,
        previewOptions: { array: 3 },
    });
    const values = t.findColumn("Values");
    for (const row of data.rows) {
        // Prints [ 1, 2, 3, 4 ]: the three asked for, and a fourth because each array holds six
        console.log(Array.from(row.get(values)));
    }
    t.close();
}
// #endregion previewArrays

// #region filterConditions
async function filterConditions() {
    const t = session.emptyTable(20).update([
        "Row = i", "Sym = i % 2 == 0 ? `AAPL` : `MSFT`", "Price = i * 1.5", "Size = 10 - i",
    ]);
    const sym = dh.FilterValue.ofColumn("Sym");
    const price = dh.FilterValue.ofColumn("Price");
    const size = dh.FilterValue.ofColumn("Size");
    // Every condition in the list must hold
    const filtered = await t.where([
        // Keeps the even rows, where Sym is AAPL
        sym.in([dh.FilterValue.ofString("AAPL"), dh.FilterValue.ofString("GOOG")]),
        // A column compared with a value, or with another column: rows 0 to 3, and 7 onward
        price.greaterThan(dh.FilterValue.ofNumber(10)).or(price.lessThan(size)),
        // Drops row 8
        size.eq(dh.FilterValue.ofNumber(2)).not(),
    ]);
    const data = await filtered.createSnapshot({ rows: { first: 0, last: filtered.size - 1 }, columns: filtered.columns });
    const row = filtered.findColumn("Row");
    // Prints [ 0, 2, 10, 12, 14, 16, 18 ]
    console.log(data.rows.map(r => r.get(row)));
    filtered.close();
}
// #endregion filterConditions

// #region filterMutableTable
async function filterMutableTable() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    // Column.filter() starts a condition from one of the table's columns, so conditions can follow the columns it has
    const integerColumns = table.columns.filter(column => column.type === "int" || column.type === "long");
    const applied = table.nextEvent(dh.Table.EVENT_FILTERCHANGED, 5000);
    // Replaces the table's filters; the table changes asynchronously, then fires EVENT_FILTERCHANGED
    table.applyFilter(integerColumns.map(column => column.filter().greaterThan(dh.FilterValue.ofNumber(2))));
    await applied;
    table.close();
}
// #endregion filterMutableTable

// #region changeMutableTable
async function changeMutableTable() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    const x = table.findColumn("X");
    // No need to wait between changes: make them all, then wait for the one that matters
    const columnsChanged = table.nextEvent(dh.Table.EVENT_CUSTOMCOLUMNSCHANGED, 5000);
    table.applySort([x.sort().desc()]);
    table.applyFilter([x.filter().lessThan(dh.FilterValue.ofNumber(8))]);
    table.applyCustomColumns(["Doubled = X * 2"]);
    await columnsChanged;

    // The data reflects all three changes; prints [ 14, 12, 10, 8, 6, 4, 2, 0 ]
    const data = await table.createSnapshot({ rows: { first: 0, last: table.size - 1 }, columns: table.columns });
    const doubled = table.findColumn("Doubled");
    console.log(data.rows.map(row => row.get(doubled)));
    table.close();
}
// #endregion changeMutableTable

// #region listenToMutableTable
async function listenToMutableTable() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    // addEventListener returns a function that removes the listener
    const removeListeners = [
        table.addEventListener(dh.Table.EVENT_SIZECHANGED, () => console.log("size", table.size)),
        table.addEventListener(dh.Table.EVENT_DISCONNECT, () => console.log("disconnected")),
        table.addEventListener(dh.Table.EVENT_RECONNECT, () => console.log("reconnected")),
        table.addEventListener(dh.Table.EVENT_RECONNECTFAILED, e => console.log("cannot reconnect", e.detail)),
    ];
    // A change the server rejects fires EVENT_REQUEST_FAILED, and the table returns to its last working state
    const failed = table.nextEvent(dh.Table.EVENT_REQUEST_FAILED, 5000);
    table.applyCustomColumns(["Bad = NoSuchColumn"]);
    console.warn("request failed", (await failed).detail);
    // Prints 0: the rejected column is gone again
    console.log(table.customColumns.length);
    removeListeners.forEach(remove => remove());
    table.close();
}
// #endregion listenToMutableTable
