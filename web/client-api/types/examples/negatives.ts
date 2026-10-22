// Inputs the types are expected to reject, and a few they still accept. Type-checked only.
import { dh } from "@deephaven/jsapi-types";
declare const session: dh.IdeSession;

async function negatives() {
    const t = session.timeTable(1e9);
    // direction is a plain string for now (follow-up: literal union), so a bad direction still type-checks:
    const badSort = await t.sort([{ column: "X", direction: "DOWN" }]);
    // Expected to be rejected: not a sort at all
    // @ts-expect-error
    const nonsense = await t.sort([{ banana: 1 }]);
    // Expected to be rejected: unknown aggregation discriminant
    // @ts-expect-error
    const badAgg = await t.aggBy({ aggregations: [{ type: "Summ" }] });
    // Expected to be rejected: unknown update-by spec
    // @ts-expect-error
    const badUb = await t.updateBy({ operations: [{ spec: { type: "Nope" }, matchPairs: [] }] });
    // Expected to be rejected: where with a string
    // @ts-expect-error
    const badWhere = await t.where(["X > 2"]);
    badSort.close();
    nonsense.close();
}
