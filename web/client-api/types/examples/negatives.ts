// Inputs the types are expected to reject, and a few they still accept. Type-checked only.
import { dh } from "@deephaven/jsapi-types";
declare const session: dh.IdeSession;

async function negatives() {
    // A fresh pending table for each line: one used again after an await would be used too late
    const t = () => session.timeTable(1e9);
    // Expected to be rejected: direction is "ASC" | "DESC"; reversing is reverse(), not a direction
    // @ts-expect-error
    const badSort = await t().sort([{ column: "X", direction: "DOWN" }]);
    // @ts-expect-error
    const reversed = await t().sort([{ column: "X", direction: "REVERSE" }]);
    // Expected to be rejected: not a sort at all
    // @ts-expect-error
    const nonsense = await t().sort([{ banana: 1 }]);
    // Expected to be rejected: unknown aggregation discriminant
    // @ts-expect-error
    const badAgg = await t().aggBy({ aggregations: [{ type: "Summ" }] });
    // Expected to be rejected: unknown update-by spec
    // @ts-expect-error
    const badUb = await t().updateBy({ operations: [{ spec: { type: "Nope" }, matchPairs: [] }] });
    // Expected to be rejected: where with a string
    // @ts-expect-error
    const badWhere = await t().where(["X > 2"]);
    badSort.close();
    reversed.close();
    nonsense.close();
}

declare const def: dh.ide.VariableDescriptor;

async function wrongCasts() {
    // Expected to be rejected: a bare PendingTable is PendingTable<ResolvedTable>, but a fetched table is a
    // PendingTable<Table>; narrow with PendingTable<Table> or a property test instead
    // @ts-expect-error
    const fetched = session.getObject(def) as dh.PendingTable;
    // Expected to be rejected: the other direction, an operation's result as a PendingTable<Table>
    // @ts-expect-error
    const resolved = session.timeTable(1e9).update(["X = i"]) as dh.PendingTable<dh.Table>;
}

// Accepted by the types, wrong at runtime. Each line type-checks; the comment says what goes wrong.
async function acceptedButWrong() {
    // Awaiting two pendings one after the other: b was not used in the task that created it, so its await rejects.
    const a = session.timeTable(1e9);
    const b = session.timeTable(1e9);
    await a;
    await b;

    // Awaited and never closed: the table and its server export are held until the session ends.
    const leaked = await session.getTable("t");

    // Resolved and awaited: a pending table produces one table, so the second to take it rejects; here, the await.
    // await p, like Promise.resolve(p), calls then() a microtask later, so p.resolve() wins even when written after it.
    const p = session.getTable("t");
    const resolved = p.resolve();
    const table = await p;
}
