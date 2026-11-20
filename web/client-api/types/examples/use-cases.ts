// Examples of the table operations, embedded in the guides. The build type-checks them and runs each against a server.
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

// #region joins
async function joins() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // left and right are never awaited, so every use of them belongs in this task: one batch, one await.
    // resolveAll, unlike Promise.all, closes the tables that arrived if another fails
    const [joined, asOf, exact] = await dh.resolveAll([
        left.naturalJoin(right, ["Key"], ["Value"]),
        left.aj(right, ["Key", "Ts"]),
        left.exactJoin(right, ["Key"], ["Value"]),
    ]);
    joined.close();
    asOf.close();
    exact.close();
}
// #endregion joins

// #region uniqueWithSentinel
async function uniqueWithSentinel() {
    const t = session.emptyTable(6).update(["Key = i % 3", "X = i", "L = (long) i"]);
    const unique = await t.aggBy({
        aggregations: [
            dh.agg.Aggregation.AggUnique(["X"], false, { type: "int", value: -1 }),
            { type: "Unique", columns: ["L"], nonUniqueSentinel: { type: "long", value: dh.LongWrapper.ofString("-1") } },
        ],
        groupByColumns: ["Key"],
    });
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

// #region whereInAndRangeJoin
async function whereInAndRangeJoin() {
    // rangeJoin only supports static inputs, so t is not a time table
    const t = session.emptyTable(30).update(["Key = i % 3", "Ts = i"]);
    const keys = session.emptyTable(2).update(["Key = i"]);
    const windows = session.emptyTable(3).update(["Key = i % 3", "LStart = i * 10", "LEnd = LStart + 5"]);
    const [inKeys, ranged] = await dh.resolveAll([
        t.whereIn(keys, ["Key"]),
        windows.rangeJoin(t, ["Key"], {
            leftStartColumn: "LStart",
            rangeStartRule: "LESS_THAN_OR_EQUAL",
            rightRangeColumn: "Ts",
            rangeEndRule: "GREATER_THAN_OR_EQUAL",
            leftEndColumn: "LEnd",
        }, [dh.agg.Aggregation.AggGroup(["Ts"])]),
    ]);
    // ... use inKeys and ranged ...
    inKeys.close();
    ranged.close();
}
// #endregion whereInAndRangeJoin

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
// Reading any table, legacy or new: Table, TotalsTable and ResolvedTable share these members
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

// #region misc
async function misc() {
    const t = session.timeTable(1e9).update(["Key = i % 3"]);
    const [head, distinct, snap, when] = await dh.resolveAll([
        t.head(5),
        t.selectDistinct(["Key"]),
        t.snapshot(),
        // The trigger's columns are added to the result, so rename its Timestamp to avoid t's
        t.snapshotWhen(session.timeTable(5e9), { initial: true, stampColumns: ["TriggerTs = Timestamp"] }),
    ]);
    head.close();
    distinct.close();
    snap.close();
    when.close();
}
// #endregion misc

// #region mergeTwo
async function mergeTwo() {
    const a = await session.emptyTable(2).update(["X = i"]);
    const b = await session.emptyTable(2).update(["X = i + 2"]);
    // mergeTables takes TableReference[], so chain results, tables and pendings all work
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

// #region legacy
// Mixing the legacy mutable Table with the new API
async function legacy() {
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
// #endregion legacy

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
    const [viewed, selected, extended] = await dh.resolveAll([
        // view and updateView compute their formulas each time a value is read; select and update compute them once
        t.view(["Key", "Double = X * 2"]),
        t.select(["Key", "X"]),
        t.updateView(["Triple = X * 3"]),
    ]);
    viewed.close();
    selected.close();
    extended.close();
}
// #endregion selectColumns

// #region rowRanges
async function rowRanges() {
    const t = session.emptyTable(10).update(["X = i"]);
    const [lastThree, middle, reversed] = await dh.resolveAll([
        t.tail(3),
        // Positions count from the start, or from the end when negative; the end position is excluded
        t.slice(1, -1),
        t.reverse(),
    ]);
    lastThree.close();
    middle.close();
    reversed.close();
}
// #endregion rowRanges

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

// #region moreJoins
async function moreJoins() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    const [reverseAsOf, cross, notInRight] = await dh.resolveAll([
        // For each left row, the first right row with the same Key whose Ts is at or after the left row's
        left.raj(right, ["Key", "Ts"]),
        // Every matching pair of rows; naturalJoin instead allows at most one right row per key
        left.join(right, ["Key"], ["Value"]),
        left.whereNotIn(right, ["Key"]),
    ]);
    reverseAsOf.close();
    cross.close();
    notInRight.close();
}
// #endregion moreJoins

// #region downsampleForChart
// Keeps only the rows needed to draw each Y column as a line across a chart 800 pixels wide
async function downsampleForChart() {
    const t = session.timeTable(1e9).update(["Y = i"]);
    const small = await t.downsample({ xColumn: "Timestamp", yColumns: ["Y"], width: 800 });
    small.close();
}
// #endregion downsampleForChart
