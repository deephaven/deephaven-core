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
        left.exactJoin(right, ["Key"]),
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

// A function that accepts any operations and reads the result
async function readAny(ops: dh.TableOperations) {
    const t = await ops.resolve();
    const n: number = t.size;
    t.close();
}

// Mixing the legacy mutable Table with the new API
async function legacy() {
    const table = (await session.getObject({ type: "Table", name: "source" })) as dh.Table;
    table.applyFilter([]);
    const derived = await table.operations().update(["Z = X + 1"]);
    const joined = await derived.naturalJoin(table, ["Key"]);
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
