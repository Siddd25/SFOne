/**
 * One-off recovery: removes this demo's Relay identities so the bridge can register
 * them again. A killed bridge can leave an identity registered; registration is
 * create-only, so startup fails with `name_conflict` until it is deleted.
 *
 *   node scripts/release-stale-identities.mjs
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const NAMES = ["intake", "ops-bot", "human", ...Array.from({ length: 8 }, (_, i) => `r${i + 1}`)];

function readEnv() {
  const env = { ...process.env };
  for (const line of readFileSync(resolve(process.cwd(), ".env"), "utf8").split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (match && env[match[1]] === undefined) env[match[1]] = match[2].replace(/^["']|["']$/g, "");
  }
  return env;
}

const env = readEnv();
const base = (env.RELAY_BASE_URL || "https://cast.agentrelay.com").replace(/\/$/, "");
const key = env.RELAY_WORKSPACE_KEY;
if (!key) {
  console.error("RELAY_WORKSPACE_KEY is not set in .env");
  process.exit(1);
}
const headers = { Authorization: `Bearer ${key}` };

async function call(method, path) {
  const response = await fetch(`${base}${path}`, { method, headers });
  const body = await response.text();
  return { status: response.status, body: body.slice(0, 160) };
}

const before = await call("GET", "/v1/agents");
console.log(`agents before: ${before.status} ${before.body}`);

for (const name of NAMES) {
  const result = await call("DELETE", `/v1/agents/${encodeURIComponent(name)}`);
  console.log(`delete ${name}: ${result.status} ${result.body}`);
}

const after = await call("GET", "/v1/agents");
console.log(`agents after: ${after.status} ${after.body}`);
