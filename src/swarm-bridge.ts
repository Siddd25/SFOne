/**
 * Connects the Python warehouse simulator to Agent Relay.
 *
 * Start the simulator first:  python python_files/server.py
 * Offline rehearsal:          npm run bridge -- --dry
 * Make a workspace once:      npm run bridge -- --create-workspace
 * Agent Relay:                RELAY_WORKSPACE_KEY=... npm run bridge
 */
import { Dedup, parseOpsCommand, routeOf, SendQueue, type Feed, type Hub } from "./bridge-lib.js";
import { DryHub, RelayHub, type AgentHub } from "./relay-hub.js";
import { RobotAgentFleet } from "./robot-agents.js";
import { createDecisionBrain } from "./decision-brain.js";
import { loadLocalEnv } from "./env.js";

loadLocalEnv();

const args = new Set(process.argv.slice(2));
const dry = args.has("--dry");
const agentsEnabled = args.has("--agents");
const llmDisabled = args.has("--no-llm");
const createWorkspace = args.has("--create-workspace");
const url = [...args].find((arg) => arg.startsWith("--ws="))?.slice("--ws=".length)
  ?? process.env.SWARM_WS_URL
  ?? "ws://localhost:8765";

const hub: Hub = dry ? new DryHub() : new RelayHub(createWorkspace);
const dedup = new Dedup();
const queue = new SendQueue((message) => hub.send(message));
let socket: WebSocket | undefined;
let robotFleet: RobotAgentFleet | undefined;

function say(text: string): void {
  // A relay hiccup must not surface as an unhandled rejection and kill the bridge.
  hub.say(text).catch((error) => console.warn(`relay say failed: ${String(error).slice(0, 120)}`));
}

function sendToSimulator(command: Record<string, unknown>): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    say(`Cannot run \`${command.cmd}\`: simulator is disconnected.`);
    return;
  }
  socket.send(JSON.stringify(command));
}

function mirrorFeed(state: { tick?: unknown; swarm?: { feed?: unknown } }): void {
  const tick = state.tick;
  if (typeof tick === "number") dedup.noteTick(tick);
  const feed = state.swarm?.feed;
  if (!Array.isArray(feed)) return;

  for (const item of feed) {
    if (!item || typeof item !== "object") continue;
    const event = item as Feed;
    if (!dedup.fresh(event)) continue;
    // In agent mode, #bids contains independent robot-agent bids, not the
    // simulator's internal bid trace.
    if (agentsEnabled && event.t === "BID") continue;
    const routed = routeOf(event);
    if (routed) queue.push(routed);
  }
}

async function main(): Promise<void> {
  await hub.start();
  if (agentsEnabled) {
    const brain = createDecisionBrain(llmDisabled);
    console.log(`Robot decision brain: ${brain.mode}${brain.mode === "llm" ? ` (${process.env.OPENAI_MODEL ?? "gpt-6-luna"})` : ""}.`);
    robotFleet = new RobotAgentFleet(
      hub as AgentHub,
      (robotId, orderId, part, cost) => {
        sendToSimulator({ cmd: "claim", robot: robotId, order: orderId, part, cost });
      },
      brain,
      (simMsg) => {
        sendToSimulator(simMsg);
      },
    );
    console.log("Robot communication agents enabled (r1-r8).");
  }
  hub.onOps((text) => {
    const command = parseOpsCommand(text);
    if (command) sendToSimulator(command);
    else say("Try: rush, kill r3, kill dispatcher, chaos, pause, resume, reset, or speed 2.");
  });

  connect();
}

let reconnectDelayMs = 2_000;

/** The simulator can restart or drop the socket mid-demo; the bridge must come back on its own. */
function connect(): void {
  socket = new WebSocket(url);
  socket.addEventListener("open", () => {
    reconnectDelayMs = 2_000;
    console.log(`SWARM bridge connected to ${url}${dry ? " (dry mode)" : ""}`);
    say("SWARM bridge online. Commands: rush, kill r3, kill dispatcher, chaos, pause, resume, reset, speed 2.");
  });
  socket.addEventListener("message", ({ data }) => {
    try {
      const message = JSON.parse(String(data)) as { type?: string; [key: string]: unknown };
      if (message.type === "state") {
        const state = message as { tick?: unknown; swarm?: { feed?: unknown; robots?: unknown } };
        robotFleet?.updateFromSnapshot(state.swarm);
        mirrorFeed(state);
      }
      if (message.type === "ack") console.log("simulator ack:", message);
    } catch (error) {
      console.warn("Ignoring invalid simulator message:", String(error));
    }
  });
  socket.addEventListener("error", () => console.error(`Could not connect to simulator at ${url}. Is server.py running?`));
  socket.addEventListener("close", () => {
    console.log(`Simulator connection closed; reconnecting in ${reconnectDelayMs} ms.`);
    const delay = reconnectDelayMs;
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, 15_000);
    setTimeout(connect, delay);
  });
}

void main().catch((error) => {
  console.error("Bridge failed:", error);
  process.exitCode = 1;
});
