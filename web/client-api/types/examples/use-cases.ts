// Use cases modeled on py-server documentation examples. Type-checked against the generated definitions; not run.
import { dh } from "@deephaven/jsapi-types";
declare const session: dh.IdeSession;

// py: empty_table(5).update(["X = i", "Y = X * 2"])
async function createAndTransform() {
    const t = await session.emptyTable(5).update(["X = i", "Y = X * 2"]);
    const data = await t.createSnapshot({ rows: { first: 0, last: 4 }, columns: t.columns });
    const y = t.findColumn("Y");
    const firstY: unknown = data.getData(0, y);
    t.close();
}

// py: time_table("PT1s").update(["X = i"]).sort_descending(["X"])
async function tickingSorted() {
    const t = await session.timeTable(1e9).update(["X = i"]).sort([{ column: "X", direction: "DESC" }]);
    const t2 = await session.timeTable(1e9).update(["X = i"]).sort(["X"]);
    t.close();
    t2.close();
}

// py: source = ...; source.where(["X > 2"])
async function filterNamed() {
    const source = session.getObject({ type: "Table", name: "source" }) as dh.PendingTable<dh.Table>;
    // Filters name their columns, so the fetch need not be awaited first
    const filtered = await source.where([dh.FilterValue.ofColumn("X").greaterThan(dh.FilterValue.ofNumber(2))]);
    filtered.close();
}

// py: left.natural_join(right, on=["Key"], joins=["Value"]); left.aj(right, on=["Key", "Ts"])
async function joins() {
    const left = session.getObject({ type: "Table", name: "left" }) as dh.PendingTable<dh.Table>;
    const right = session.getObject({ type: "Table", name: "right" }) as dh.PendingTable<dh.Table>;
    // left and right are never awaited, so every use of them belongs in this task: one batch, one await
    const [joined, asOf, exact] = await Promise.all([
        left.naturalJoin(right, ["Key"], ["Value"]),
        left.aj(right, ["Key", "Ts"]),
        left.exactJoin(right, ["Key"], ["Value"]),
    ]);
    joined.close();
    asOf.close();
    exact.close();
}

// py: t.agg_by([agg.sum_(["SumX = X"]), agg.count_("N")], by=["Key"])
async function aggregate() {
    const t = session.timeTable(1e9).update(["Key = i % 3", "X = i"]);
    const [viaFactories, viaLiterals, sumBy] = await Promise.all([
        t.aggBy({
            aggregations: [dh.agg.Aggregation.AggSum(["SumX = X"]), dh.agg.Aggregation.AggCount("N")],
            groupByColumns: ["Key"],
        }),
        t.aggBy({
            aggregations: [{ type: "Sum", columns: ["SumX = X"] }, { type: "Count", col: "N" }],
            groupByColumns: ["Key"],
        }),
        t.sumBy(["Key"]),
    ]);
    viaFactories.close();
    viaLiterals.close();
    sumBy.close();
}

// py: t.update_by(ops=[cum_sum(["CumX = X"])], by=["Key"])
async function updateBy() {
    const t = await session.timeTable(1e9).update(["Key = i % 3", "X = i"]).updateBy({
        operations: [{ spec: { type: "UpdateByCumulativeSum" }, matchPairs: ["CumX = X"] }],
        groupByColumns: ["Key"],
    });
    t.close();
}

// py: t.update_by(ops=[rolling_sum_tick(["SumX = X"], rev_ticks=5)], by=["Key"])
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

// py: t.where_in(keys, ["Key"]); windows.range_join(t, on=["Key", "LStart <= Ts <= LEnd"], aggs=[agg.group(["Ts"])])
async function whereInAndRangeJoin() {
    const t = session.timeTable(1e9).update(["Key = i % 3", "Ts = i"]);
    const keys = session.emptyTable(2).update(["Key = i"]);
    const windows = session.emptyTable(3).update(["Key = i % 3", "LStart = i * 10", "LEnd = LStart + 5"]);
    const [inKeys, ranged] = await Promise.all([
        t.whereIn(keys, ["Key"]),
        windows.rangeJoin(t, ["Key"], {
            leftStartColumn: "LStart",
            rangeStartRule: "LESS_THAN_OR_EQUAL",
            rightRangeColumn: "Ts",
            rangeEndRule: "GREATER_THAN_OR_EQUAL",
            leftEndColumn: "LEnd",
        }, [dh.agg.Aggregation.AggGroup(["Ts"])]),
    ]);
    inKeys.close();
    ranged.close();
}

// Abandoning work that is no longer wanted: cancel() a pending table, or abort the signal given to resolve()
async function cancelAndAbort(signal: AbortSignal) {
    const unwanted = session.timeTable(1e9).update(["X = i"]).sumBy([]);
    // Not sent if it has not been yet; awaiting it now rejects with "Operation canceled"
    unwanted.cancel();
    // If the signal aborts before the table arrives, the await rejects and nothing is left to close
    const t = await session.timeTable(1e9).update(["X = i"]).resolve({ signal });
    t.close();
}

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

// Several tables at once when any can fail: Promise.all would reject and lose the ones that arrived, still open
async function severalThatMayFail() {
    const t = session.emptyTable(5).update(["X = i"]);
    const results = await Promise.allSettled([t.update(["Y = X * 2"]), t.update(["Z = NoSuchColumn"])]);
    const tables = results.flatMap(r => r.status === "fulfilled" ? [r.value] : []);
    try {
        if (tables.length < results.length) {
            throw new Error("some tables failed");
        }
        // ... use the tables ...
    } finally {
        tables.forEach(table => table.close());
    }
}

// Reading any table, legacy or new: Table, TotalsTable and ResolvedTable are all an ITable
function describe(t: dh.ITable): string {
    return `${t.size} rows: ${t.columns.map(c => c.name).join(", ")}`;
}
async function readEither() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    const resolved = await table.operations();
    console.log(describe(table), describe(resolved));
    resolved.close();
    table.close();
}

// py: t.head(5); t.select_distinct(["Key"]); t.snapshot(); t.snapshot_when(trigger, initial=True)
async function misc() {
    const t = session.timeTable(1e9).update(["Key = i % 3"]);
    const [head, distinct, snap, when] = await Promise.all([
        t.head(5),
        t.selectDistinct(["Key"]),
        t.snapshot(),
        t.snapshotWhen(session.timeTable(5e9), { initial: true }),
    ]);
    head.close();
    distinct.close();
    snap.close();
    when.close();
}

// py: merge([t1, t2])
async function mergeTwo() {
    const a = await session.emptyTable(2).update(["X = i"]);
    const b = await session.emptyTable(2).update(["X = i + 2"]);
    // mergeTables takes TableReference[], so chain results, tables and pendings all work
    const merged = await session.mergeTables([a, b, session.emptyTable(1).update(["X = 99"])]);
    merged.close();
    a.close();
    b.close();
}

// A function that accepts any operations, reads the result, and chains on it later. The table ops resolves to is the
// caller's, so this copies it to have one of its own to close.
async function readAny(ops: dh.TableOperations) {
    const t = (await ops.resolve()).copy();
    const n: number = t.size;
    const top = await t.head(10);
    top.close();
    t.close();
}

// Mixing the legacy mutable Table with the new API
async function legacy() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    table.applyFilter([]);
    const derived = await table.operations().update(["Z = X + 1"]);
    // derived already has every column of table, so name the one to add
    const joined = await derived.naturalJoin(table, ["Key"], ["OriginalX = X"]);
    await readAny(table.operations());
    table.close();
    derived.close();
    joined.close();
}

// Data access on a resolved chain result, viewport style
async function viewport() {
    const t = await session.timeTable(1e9).update(["X = i"]);
    const sub = t.createViewportSubscription({ rows: { first: 0, last: 9 }, columns: ["X"] });
    sub.close();
    t.close();
}
