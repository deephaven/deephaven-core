// Runs each function in the compiled examples against a Deephaven server, and fails if any rejects.
// Usage: DH_SERVER=http://host:10000 node run-examples.mjs <dh-core.js> <compiled use-cases.js> [report file]
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [dhCoreFile, examplesFile, reportFile] = process.argv.slice(2);
const serverUrl = process.env.DH_SERVER ?? "http://localhost:10000";
const TIMEOUT_MS = 30000;

// Tables the examples fetch by name
const FIXTURES = `
from deephaven import empty_table
source = empty_table(10).update(["X = i", "Key = i"])
left = empty_table(10).update(["Key = i % 3", "Ts = i", "L = i * 10"])
right = empty_table(3).update(["Key = i", "Ts = i", "Value = i * 100"])
`;
// Called by another example with a table it lends, not on its own
const SKIPPED = new Set(["readTable"]);

const { default: dh } = await import(pathToFileURL(path.resolve(dhCoreFile)).href);

// The examples import dh from the types package, which has no runtime; hand them the real one instead
const code = fs.readFileSync(examplesFile, "utf8")
    .replace(/^import \{ dh \} from "@deephaven\/jsapi-types";$/m, "");
const names = [...code.matchAll(/^async function (\w+)\(/gm)].map(m => m[1]);
const load = new Function("dh", "session", `${code}\nreturn { ${names.join(", ")} };`);

function withTimeout(promise, ms) {
    let timer;
    return Promise.race([
        promise.finally(() => clearTimeout(timer)),
        new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`timed out after ${ms} ms`)), ms);
        }),
    ]);
}

const client = new dh.CoreClient(serverUrl);
await client.login({ type: dh.CoreClient.LOGIN_TYPE_ANONYMOUS });
const session = await (await client.getAsIdeConnection()).startSession("python");
const fixtures = await session.runCode(FIXTURES);
if (fixtures.error) {
    throw new Error(`fixtures failed: ${fixtures.error}`);
}

const examples = load(dh, session);
const lines = [];
let failed = 0;
for (const name of names.filter(n => !SKIPPED.has(n))) {
    // Some examples warn on purpose, to show a handled failure
    const warnings = [];
    const warn = console.warn;
    console.warn = (...args) => warnings.push(args.map(String).join(" "));
    try {
        await withTimeout(examples[name](new AbortController().signal), TIMEOUT_MS);
        lines.push(`PASS ${name}`);
    } catch (e) {
        failed++;
        lines.push(`FAIL ${name}: ${e?.message ?? e}`);
    } finally {
        console.warn = warn;
    }
    lines.push(...warnings.map(w => `     warn: ${w}`));
}
lines.push(`${names.length - SKIPPED.size - failed} passed, ${failed} failed, ${SKIPPED.size} skipped`);

const report = lines.join("\n");
console.log(report);
if (reportFile) {
    fs.writeFileSync(reportFile, report + "\n");
}
client.disconnect();
process.exit(failed ? 1 : 0);
