// What getObject returns: PendingTable<Table> | Promise<any>, and how to await, narrow and chain on it.
import { dh } from "@deephaven/jsapi-types";
declare const session: dh.IdeSession;
declare const def: dh.ide.VariableDescriptor;

async function probe() {
    // 1. await directly on the union; the result is `any` because of the Promise<any> member
    const a = await session.getObject(def);
    const asTable: dh.Table = a;
    const isAny: number = a;
    asTable.close();

    // 2. .then on the union
    session.getObject(def).then(t => {
        const tIsAny: number = t;
        t.close();
    });

    // 3. chaining without a cast
    // @ts-expect-error -- the union has no `where`
    session.getObject(def).where([]);

    // 4. chaining with a cast; a fetched table is PendingTable<Table>, so name the type argument (a bare
    // PendingTable means PendingTable<ResolvedTable>, what operations produce)
    const pending = session.getObject(def) as dh.PendingTable<dh.Table>;
    const r = await pending.where([]).sort([]);
    const rType: dh.ResolvedTable = r;
    r.close();

    // 5. Promise.all with the union
    const [x, y] = await Promise.all([session.getObject(def), session.getObject(def)]);
    const xType: dh.Table = x;
    x.close();
    y.close();

    // 6. narrowing by a property test
    const o = session.getObject(def);
    if ("where" in o) {
        const n = await o.where([]);
        const nType: dh.ResolvedTable = n;
        n.close();
    }
}
