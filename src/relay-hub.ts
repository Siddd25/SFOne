// The only file that talks to Agent Relay. If the SDK misbehaves, run the bridge with --dry
// (DryHub) and the rest of the system keeps working.
import { AgentRelay } from "@agent-relay/sdk";
import type { Hub, Routed } from "./bridge-lib.js";

export const ROBOT_NAMES = Array.from({ length: 8 }, (_, i) => `r${i + 1}`);
const CHANNELS: Record<string, string> = {
  orders: "New orders announced by intake",
  bids: "Robots bid on orders",
  fleet: "Awards, deliveries, faults, re-auctions",
  ops: "Humans type commands here: rush, kill r3, kill dispatcher, chaos",
};

type Client = Exclude<Awaited<ReturnType<AgentRelay["workspace"]["register"]>>, readonly unknown[]>;
export type OrderListener = (text: string) => void;
export type BidListener = (text: string) => void;
export interface AgentHub extends Hub {
  onOrder(handler: OrderListener): void;
  onBid(handler: BidListener): void;
}

export class RelayHub implements AgentHub {
  private relay!: AgentRelay;
  private clients = new Map<string, Client>();
  private opsHandler: (text: string) => void = () => {};
  private orderListeners: OrderListener[] = [];
  private bidListeners: BidListener[] = [];
  private handled = new Set<string>(); // fan-in may deliver one event more than once

  constructor(private readonly allowWorkspaceCreation = false) {}

  private async retryWorkspaceWrite<T>(operation: () => Promise<T>): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        const relayError = error as { code?: string; rawCode?: string; retryAfterMs?: number; retryable?: boolean };
        const retryable = relayError.retryable === true
          || relayError.code === "rate_limited"
          || relayError.rawCode === "workspace_busy";
        if (!retryable || attempt === 4) throw error;
        const delay = Math.max(relayError.retryAfterMs ?? 0, 500 * 2 ** attempt);
        console.warn(`Relay temporarily unavailable; retrying in ${delay} ms (${attempt + 1}/5).`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    throw lastError;
  }

  private async register(name: string, type: "agent" | "human"): Promise<Client> {
    const registered = await this.retryWorkspaceWrite(() => this.relay.workspace.register({ name, type }));
    // The SDK also supports batch registration, hence its union return type.
    // SWARM registers one identity at a time.
    const client = (Array.isArray(registered) ? registered[0] : registered) as Client;
    if (!client) throw new Error(`Relay did not create identity ${name}`);
    return client;
  }

  /**
   * Deletes an identity outright. `workspace.release()` only frees identities the
   * caller owns, which is exactly the case that fails after a restart, so this calls
   * the relay's delete endpoint directly. 404 means it is already gone.
   */
  private async deleteIdentity(name: string): Promise<void> {
    const key = process.env.RELAY_WORKSPACE_KEY?.trim();
    if (!key) return;
    const base = (process.env.RELAY_BASE_URL?.trim() || "https://cast.agentrelay.com").replace(/\/$/, "");
    const response = await fetch(`${base}/v1/agents/${encodeURIComponent(name)}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${key}` },
    });
    if (!response.ok && response.status !== 404) {
      throw new Error(`could not delete ${name}: ${response.status}`);
    }
  }

  /**
   * Registration is create-only, so a bridge that was killed (or a second instance)
   * leaves an identity behind and startup fails with name_conflict. Delete it and
   * retry: the relay can still be settling, so one attempt is not enough.
   */
  private async replaceIdentity(name: string, type: "agent" | "human"): Promise<Client> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 4; attempt++) {
      console.log(`Replacing stale Relay identity: ${name}${attempt ? ` (retry ${attempt + 1})` : ""}`);
      await this.retryWorkspaceWrite(() => this.relay.workspace.release({ name, deleteAgent: true })).catch(() => {});
      await this.deleteIdentity(name).catch((error) => console.warn(`release path failed: ${String(error).slice(0, 120)}`));
      try {
        return await this.register(name, type);
      } catch (error) {
        lastError = error;
        if (!this.isExistingAgentError(error)) throw error;
        await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** attempt));
      }
    }
    throw lastError;
  }

  private isExistingAgentError(error: unknown): boolean {
    const relayError = error as { code?: string; rawCode?: string; statusCode?: number };
    return relayError.code === "name_conflict"
      || relayError.rawCode === "agent_already_exists"
      || relayError.statusCode === 409
      || String(error).includes("name_conflict")
      || String(error).includes("already exists");
  }

  async start(): Promise<void> {
    const key = process.env.RELAY_WORKSPACE_KEY;
    const baseUrl = process.env.RELAY_BASE_URL?.trim() || undefined;
    if (key) {
      console.log(`Using RELAY_WORKSPACE_KEY from the environment${baseUrl ? ` at ${baseUrl}` : ""}.`);
      this.relay = new AgentRelay({ workspaceKey: key, ...(baseUrl ? { baseUrl } : {}) });
    } else {
      if (!this.allowWorkspaceCreation) {
        throw new Error("RELAY_WORKSPACE_KEY is not set. Set it before starting the bridge, or pass --create-workspace once to make a new workspace.");
      }
      this.relay = await this.retryWorkspaceWrite(() =>
        AgentRelay.createWorkspace({ name: `swarm-${Date.now()}`, ...(baseUrl ? { baseUrl } : {}) }));
      console.log("\nWorkspace created. Put this in .env to reuse it next run:");
      console.log(`RELAY_WORKSPACE_KEY=${this.relay.workspaceKey}\n`);
    }

    // Registration is create-only: a restart would hit name_conflict, so release the old identity first.
    const names = ["intake", "ops-bot", "human", ...ROBOT_NAMES];
    // A new workspace has a small write capacity. Register sequentially so
    // bootstrapping eleven identities does not trigger workspace_busy (429).
    for (const name of names) {
      const type = name === "human" ? "human" : "agent";
      try {
        this.clients.set(name, await this.register(name, type));
      } catch (e) {
        if (!this.isExistingAgentError(e)) throw e;
        this.clients.set(name, await this.replaceIdentity(name, type));
      }
    }

    const intake = this.clients.get("intake")!;
    for (const [name, topic] of Object.entries(CHANNELS)) {
      try { await this.retryWorkspaceWrite(() => intake.channels.create({ name, topic })); } catch { /* already exists */ }
    }
    for (const [name, client] of this.clients.entries()) {
      for (const channel of Object.keys(CHANNELS)) {
        if (channel === "ops" && name !== "ops-bot" && name !== "human") continue;
        await this.retryWorkspaceWrite(() => client.channels.join(channel)).catch(() => {});
      }
    }

    // Humans (or anyone but ops-bot) writing in #ops become sim commands.
    this.relay.addListener("message.created", ({ message, envelope }) => {
      if (this.handled.has(message.id)) return;
      this.handled.add(message.id);
      const channel = envelope.channel?.name;
      const sender = envelope.from?.name;
      if (channel === "ops" && sender !== "ops-bot") this.opsHandler(message.text);
      if (channel === "orders" && sender === "intake") {
        for (const listener of this.orderListeners) listener(message.text);
      }
      if (channel === "bids" && ROBOT_NAMES.includes(sender ?? "")) {
        for (const listener of this.bidListeners) listener(message.text);
      }
    });
  }

  async send(m: Routed): Promise<void> {
    const c = this.clients.get(m.speaker) ?? this.clients.get("intake")!;
    await c.sendMessage({ to: `#${m.channel}`, text: m.text, metadata: m.raw as Record<string, unknown> });
    console.log(`[relay #${m.channel}] ${m.text}`);
  }

  async say(text: string): Promise<void> {
    await this.clients.get("ops-bot")!.sendMessage({ to: "#ops", text });
  }

  onOps(handler: (text: string) => void): void { this.opsHandler = handler; }
  onOrder(handler: OrderListener): void { this.orderListeners.push(handler); }
  onBid(handler: BidListener): void { this.bidListeners.push(handler); }

  async sendHuman(text: string): Promise<void> {
    await this.clients.get("human")!.sendMessage({ to: "#ops", text });
  }
}

/** No Relay: prints what would be posted. Lets you test the sim <-> bridge half on its own. */
export class DryHub implements AgentHub {
  private opsHandler: (text: string) => void = () => {};
  private orderListeners: OrderListener[] = [];
  private bidListeners: BidListener[] = [];
  async start(): Promise<void> { console.log("[dry] not connected to Agent Relay"); }
  async send(m: Routed): Promise<void> {
    console.log(`#${m.channel.padEnd(6)} ${m.text}`);
    if (m.channel === "orders" && m.speaker === "intake") {
      for (const listener of this.orderListeners) listener(m.text);
    }
    if (m.channel === "bids" && ROBOT_NAMES.includes(m.speaker)) {
      for (const listener of this.bidListeners) listener(m.text);
    }
  }
  async say(text: string): Promise<void> { console.log(`#ops    ops-bot: ${text}`); }
  onOps(handler: (text: string) => void): void { this.opsHandler = handler; }
  onOrder(handler: OrderListener): void { this.orderListeners.push(handler); }
  onBid(handler: BidListener): void { this.bidListeners.push(handler); }
  async sendHuman(text: string): Promise<void> { console.log(`#ops    human: ${text}`); this.opsHandler(text); }
}
