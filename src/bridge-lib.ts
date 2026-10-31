// Pure logic for the SWARM <-> Agent Relay bridge. No network, so it is unit-testable.

export type Feed = { t: string; tick?: number; [k: string]: unknown };

export type Channel = "orders" | "bids" | "fleet";

export interface Routed {
  channel: Channel;
  speaker: string; // Relay agent name that "says" this line
  text: string; // human-readable line shown in the channel
  droppable: boolean; // may be dropped under load (bids are chatty)
  raw: Feed; // original sim message, attached as metadata
}

/** The seam to Agent Relay. RelayHub is the real one; DryHub prints to the terminal. */
export interface Hub {
  start(): Promise<void>;
  send(msg: Routed): Promise<void>;
  /** ops-bot replies in #ops */
  say(text: string): Promise<void>;
  /** called when a human message arrives in #ops */
  onOps(handler: (text: string) => void): void;
  /** what the terminal user types (goes through Relay in RelayHub) */
  sendHuman(text: string): Promise<void>;
}

const xy = (p: unknown): string => (Array.isArray(p) ? `(${p[0]},${p[1]})` : String(p));

/** Map one swarm-world feed message to a Relay channel line. Returns null for messages we don't mirror. */
export function routeOf(m: Feed): Routed | null {
  const o = m.order;
  const r = typeof m.robot === "string" ? m.robot : undefined;
  switch (m.t) {
    case "ORDER_POSTED":
      return {
        channel: "orders", speaker: "intake", droppable: false, raw: m,
        text: m.pickup_b
          ? `New dual assembly order o${o}${m.priority ? " [RUSH]" : ""}: Part A at ${xy(m.pickup)} + Part B at ${xy(m.pickup_b)} -> drop ${xy(m.dropoff)}. Swarm pairing required!`
          : `New order o${o}${m.priority ? " [RUSH]" : ""}: pick ${xy(m.pickup)} -> drop ${xy(m.dropoff)}. Who bids?`,
      };
    case "BID":
      return { channel: "bids", speaker: r ?? "intake", droppable: true, raw: m, text: `${r}: bid ${m.cost} on o${o}` };
    case "AWARD":
      return {
        channel: "fleet", speaker: r ?? "intake", droppable: false, raw: m,
        text: m.part ? `${r}: awarded Part ${m.part} of o${o} (cost ${m.cost})` : `${r}: lowest bid (${m.cost}), taking o${o}`,
      };
    case "PART_DELIVERED":
      return { channel: "fleet", speaker: r ?? "intake", droppable: false, raw: m, text: `📦 ${r}: delivered Part ${m.part} for o${o} to station` };
    case "DELIVERED":
      return {
        channel: "fleet", speaker: r ?? "intake", droppable: false, raw: m,
        text: `🎉 o${o} assembled and delivered! ${Array.isArray(m.team) ? `Collaborators: ${m.team.join(" + ")}` : ""}`,
      };
    case "TASK_ORPHANED":
      return {
        channel: "fleet", speaker: "intake", droppable: false, raw: m,
        text: `Heartbeat lost from ${m.lastRobot}. o${o}${m.part ? ` Part ${m.part}` : ""} is orphaned, requesting emergency relay!`,
      };
    case "FAULT_INJECTED":
      return {
        channel: "fleet", speaker: "intake", droppable: false, raw: m,
        text: `Operator injected a fault: ${m.robot ?? m.target}`,
      };
    case "ROBOT_CHAT":
      return {
        channel: "fleet", speaker: r ?? "intake", droppable: true, raw: m,
        text: `💬 ${r ?? "unknown"}: ${m.text ?? ""}`,
      };
    case "AGENT_BID":
      return { channel: "bids", speaker: r ?? "intake", droppable: true, raw: m, text: `${r}: agent bid ${m.cost} on o${o}` };
    default:
      return null;
  }
}

/** Parse a line typed in #ops into a sim command. Returns null if it isn't a command. */
export function parseOpsCommand(text: string): Record<string, unknown> | null {
  const t = text.trim().toLowerCase();
  if (!t) return null;
  if (t.includes("rush") || t.includes("urgent")) return { cmd: "rush" };
  if (/kill\s+(the\s+)?dispatcher/.test(t)) return { cmd: "kill_dispatcher" };
  const k = t.match(/kill\s+(?:robot\s+)?(r\d+)/);
  if (k) return { cmd: "kill", robot: k[1] };
  if (/^kill\b/.test(t)) return { cmd: "kill" };
  if (t.startsWith("chaos")) return { cmd: "chaos" };
  if (t === "pause" || t === "resume" || t === "reset") return { cmd: t };
  const s = t.match(/^speed\s+(\d+(?:\.\d+)?)/);
  if (s) return { cmd: "speed", x: Number(s[1]) };
  return null;
}

/** Drops feed messages we've already mirrored (the sim re-sends the last few every tick). */
export class Dedup {
  private seen = new Set<string>();
  private lastTick = -1;
  constructor(private cap = 5000) {}

  noteTick(tick: number): void {
    if (tick < this.lastTick) this.seen.clear(); // sim was reset
    this.lastTick = tick;
  }

  fresh(m: Feed): boolean {
    const key = JSON.stringify(m);
    if (this.seen.has(key)) return false;
    this.seen.add(key);
    if (this.seen.size > this.cap) {
      const oldest = this.seen.values().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    return true;
  }
}

/** Bounded-concurrency sender. When backed up, chatty (droppable) messages are skipped, never the important ones. */
export class SendQueue {
  private q: Routed[] = [];
  private active = 0;
  dropped = 0;
  constructor(
    private worker: (m: Routed) => Promise<void>,
    private concurrency = 4,
    private maxBacklog = 120,
  ) {}

  push(m: Routed): void {
    if (m.droppable && this.q.length > this.maxBacklog) {
      this.dropped++;
      return;
    }
    this.q.push(m);
    this.pump();
  }

  get backlog(): number {
    return this.q.length;
  }

  private pump(): void {
    while (this.active < this.concurrency && this.q.length) {
      const job = this.q.shift()!;
      this.active++;
      this.worker(job)
        .catch((e) => console.warn("send failed:", String(e).slice(0, 160)))
        .finally(() => {
          this.active--;
          this.pump();
        });
    }
  }
}
