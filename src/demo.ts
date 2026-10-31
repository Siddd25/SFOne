import { AgentRelay } from "@agent-relay/sdk";

const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

// This creates a new Relay workspace.
// In a real app, create it once and store its key in a server-side secret store.
const relay = await AgentRelay.createWorkspace({
  name: `hackathon-demo-${Date.now()}`,
});

console.log("\nWorkspace created.");
console.log("Save this only in a server-side .env file:");
console.log(`RELAY_WORKSPACE_KEY=${relay.workspaceKey}\n`);

// Every worker gets its own Relay identity.
const coordinator = await relay.workspace.register({
  name: "coordinator",
  type: "agent",
});

const researcher = await relay.workspace.register({
  name: "researcher",
  type: "agent",
});

const builder = await relay.workspace.register({
  name: "builder",
  type: "agent",
});

// Channels make routing explicit.
await coordinator.channels.create({
  name: "research-tasks",
  topic: "Tasks for the research agent",
});

await coordinator.channels.create({
  name: "build-tasks",
  topic: "Tasks for the builder agent",
});

await coordinator.channels.create({
  name: "results",
  topic: "Completed work",
});

await researcher.channels.join("research-tasks");
await researcher.channels.join("results");

await builder.channels.join("build-tasks");
await builder.channels.join("results");

await coordinator.channels.join("results");

// This is where each real worker would call its LLM, database,
// web-search function, code runner, etc.
async function runResearch(task: string) {
  return {
    summary: `Research completed for: ${task}`,
    recommendation: "Use short-lived tasks, structured results, and idempotent retries.",
  };
}

async function runBuild(task: string) {
  return {
    summary: `Build completed for: ${task}`,
    artifact: "Prototype implementation prepared.",
  };
}

// All Relay events arrive here. In production, each worker process
// would install only the handler relevant to its own role.
relay.addListener("message.created", async ({ message, envelope }) => {
  const channel = envelope.channel?.name;
  const sender = envelope.from?.name;

  if (channel === "research-tasks" && sender === coordinator.name) {
    const result = await runResearch(message.text);

    await researcher.sendMessage({
      to: "#results",
      text: JSON.stringify({
        type: "result",
        taskId: "research-001",
        from: "researcher",
        status: "done",
        result,
      }),
      idempotencyKey: "research-001-result",
    });
  }

  if (channel === "build-tasks" && sender === coordinator.name) {
    const result = await runBuild(message.text);

    await builder.sendMessage({
      to: "#results",
      text: JSON.stringify({
        type: "result",
        taskId: "build-001",
        from: "builder",
        status: "done",
        result,
      }),
      idempotencyKey: "build-001-result",
    });
  }

  if (channel === "results") {
    console.log("\nRESULT RECEIVED:");
    console.log(message.text);
  }
});

// The coordinator assigns work.
await coordinator.sendMessage({
  to: "#research-tasks",
  text: "Find a reliable strategy for retrying failed agent tasks.",
  idempotencyKey: "research-001-assignment",
});

await coordinator.sendMessage({
  to: "#build-tasks",
  text: "Create the first prototype of the task dashboard.",
  idempotencyKey: "build-001-assignment",
});

// Keep the WebSocket/event listener alive long enough to observe the demo.
await sleep(3000);

console.log("\nDemo finished.");
