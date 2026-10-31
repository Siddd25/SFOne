/**
 * Collaborative multi-agent robot fleet with an Orchestrator Agent.
 *
 * Auctions are driven by the simulator snapshot, not by the one-shot ORDER_POSTED
 * event: the simulator announces an order once, so an auction that ends without a
 * winner must be able to run again later. Every round is time-boxed to the brain's
 * speed (an LLM needs seconds, the heuristic answers in milliseconds) and a round
 * that wins nothing is retried after a cooldown instead of being forgotten.
 */
import type { Feed, Routed } from "./bridge-lib.js";
import type { AgentHub } from "./relay-hub.js";
import type { DecisionBrain } from "./decision-brain.js";

type Point = [number, number];
type Part = "A" | "B";
type RobotState = { id: string; x: number; y: number; battery: number; state: string; alive: boolean };
type OrderState = {
  id: number;
  status: string;
  pickup: Point;
  pickup_b?: Point | null;
  dropoff: Point;
  priority?: number | boolean;
  robot?: string | null;
  robot_b?: string | null;
  parts_delivered?: string[];
};
type SubtaskBid = {
  order: number;
  robot: string;
  cost: number;
  part: Part;
  rationale?: string;
  commentary?: string;
};

const distance = (a: Point, b: Point): number => Math.abs(a[0] - b[0]) + Math.abs(a[1] - b[1]);

/** Auction tuning. */
const TUNING = {
  /** Hard deadline for an LLM brain; the round usually closes much earlier, on bids. */
  llmWindowMs: 8_000,
  /** Bids per part that are enough to award without waiting for the whole pool. */
  bidsToClose: 2,
  /** Bid window for the local heuristic, which answers immediately. */
  heuristicWindowMs: 400,
  /** Cooldown before re-auctioning an order that produced no usable claim. */
  retryCooldownMs: 1_000,
  /** Shorter cooldown when bids existed but a faster round had already taken those robots. */
  contendedRetryMs: 200,
  /** How many orders may hold an open auction at the same time. */
  maxOpenAuctions: 4,
  /** Only the robots nearest a pickup are asked, so one round stays small. */
  candidatesPerPart: 3,
  /** Cap on concurrent brain calls, so a burst of orders cannot stampede the model. */
  maxConcurrentDecisions: 8,
  /** How long a claimed robot stays out of new auctions while the simulator catches up. */
  reservationMs: 2_000,
  /** Backoff when a whole round produced no answer at all (slow or failing model). */
  silentRoundBackoffMs: 3_000,
};

/** Parts of an order that still need a robot. */
function neededParts(order: OrderState): Part[] {
  const delivered = new Set(order.parts_delivered ?? []);
  const needs: Part[] = [];
  if (!order.robot && !delivered.has("A")) needs.push("A");
  if (order.pickup_b && !order.robot_b && !delivered.has("B")) needs.push("B");
  return needs;
}

function pickupFor(order: OrderState, part: Part): Point {
  return part === "B" && order.pickup_b ? order.pickup_b : order.pickup;
}

/** Strips a leading robot/emoji prefix so radio chatter is not decorated twice. */
function cleanCommentary(robotId: string, text: string): string {
  return text
    .replace(/^[\s💬🗨️]*/u, "")
    .replace(new RegExp(`^${robotId}\\s*[:\\-–]\\s*`, "i"), "")
    .trim();
}

/** Small FIFO semaphore for outbound model calls. */
class Limiter {
  private active = 0;
  private readonly waiting: Array<() => void> = [];
  constructor(private readonly max: number) {}

  async run<T>(job: () => Promise<T>): Promise<T> {
    if (this.active >= this.max) await new Promise<void>((resolve) => this.waiting.push(resolve));
    this.active++;
    try {
      return await job();
    } finally {
      this.active--;
      this.waiting.shift()?.();
    }
  }
}

class RobotAgent {
  constructor(public readonly id: string, private state?: RobotState) {}

  update(state: RobotState): void { this.state = state; }
  getState(): RobotState | undefined { return this.state; }
  position(): Point | null { return this.state ? [this.state.x, this.state.y] : null; }

  /** Only idle, living robots with usable charge may be asked to bid. */
  available(): boolean {
    const s = this.state;
    return Boolean(s && s.alive && s.state === "IDLE" && s.battery > 0.25);
  }
}

export class RobotAgentFleet {
  private readonly robots = new Map<string, RobotAgent>();
  private readonly orders = new Map<number, OrderState>();
  private readonly openAuctions = new Set<number>();
  private readonly retryAt = new Map<number, number>();
  private readonly announced = new Set<number>();
  /** Simulator tick of the last snapshot, so a reset (tick back to zero) clears stale state. */
  private lastTick = 0;
  private readonly limiter = new Limiter(TUNING.maxConcurrentDecisions);
  /** Robots already claimed by another in-flight auction, until the snapshot shows them busy. */
  private readonly reservedUntil = new Map<string, number>();
  private readonly windowMs: number;

  constructor(
    private readonly hub: AgentHub,
    private readonly claim: (robotId: string, orderId: number, part: Part, cost: number) => void,
    private readonly brain: DecisionBrain,
    private readonly sendSimEvent?: (msg: Record<string, unknown>) => void,
    robotIds = Array.from({ length: 8 }, (_, i) => `r${i + 1}`),
  ) {
    for (const id of robotIds) this.robots.set(id, new RobotAgent(id));
    this.windowMs = brain.mode === "llm" ? TUNING.llmWindowMs : TUNING.heuristicWindowMs;
  }

  /** Called on every simulator snapshot: the fleet's whole world view, and its auction clock. */
  updateFromSnapshot(snapshot: unknown): void {
    if (!snapshot || typeof snapshot !== "object") return;
    const state = snapshot as { robots?: unknown; orders?: unknown; tick?: unknown };

    // A reset restarts the tick counter; order ids alone cannot tell a reset from a
    // delivered order, so the tick is the only reliable signal.
    if (typeof state.tick === "number") {
      if (state.tick < this.lastTick) this.forgetWorld();
      this.lastTick = state.tick;
    }

    if (Array.isArray(state.robots)) {
      for (const raw of state.robots) {
        const robot = raw as RobotState;
        if (typeof robot.id !== "string" || !this.robots.has(robot.id)) continue;
        this.robots.get(robot.id)!.update(robot);
        // The simulator accepted the claim once the robot is no longer idle.
        if (robot.state !== "IDLE") this.reservedUntil.delete(robot.id);
      }
    }

    if (Array.isArray(state.orders)) {
      // The snapshot only carries open orders, so anything missing is delivered or gone.
      const live = new Set<number>();
      for (const raw of state.orders) {
        const order = raw as OrderState;
        if (typeof order.id !== "number" || !Array.isArray(order.pickup)) continue;
        live.add(order.id);
        this.orders.set(order.id, order);
        this.announce(order);
      }
      for (const id of [...this.orders.keys()]) {
        if (!live.has(id)) {
          this.orders.delete(id);
          this.retryAt.delete(id);
        }
      }
    }

    this.pump();
  }

  /** The simulator restarted its order counter, so nothing learned from the old world applies. */
  private forgetWorld(): void {
    this.orders.clear();
    this.retryAt.clear();
    this.openAuctions.clear();
    this.announced.clear();
    this.reservedUntil.clear();
    this.lastTick = 0;
  }

  /** Relay chatter must never take the bridge down, so failures are logged, not thrown. */
  private say(text: string): void {
    this.hub.say(text).catch((error) => console.warn(`relay say failed: ${String(error).slice(0, 120)}`));
  }

  /**
   * Bids are posted in the background on purpose: an auction must not wait for the
   * relay to accept a message. A slow or unavailable relay then costs chatter, not
   * the award, which is what keeps robots working when the network stalls.
   */
  private post(message: Routed): void {
    this.hub.send(message).catch((error) => console.warn(`relay send failed: ${String(error).slice(0, 120)}`));
  }

  /** The simulator announces each order once, so the orchestrator decomposition is posted once. */
  private announce(order: OrderState): void {
    if (!order.pickup_b || this.announced.has(order.id)) return;
    this.announced.add(order.id);
    const text = `📦 Orchestrator: Dual-item Order #${order.id} decomposed: `
      + `Part A at (${order.pickup}) + Part B at (${order.pickup_b}) -> Station (${order.dropoff}). `
      + "Requesting collaborative robot team!";
    this.say(text);
    this.sendSimEvent?.({ cmd: "chat", from: "Orchestrator", text });
  }

  /** True while no other auction has claimed this robot. */
  private isFree(robot: RobotAgent, now = Date.now()): boolean {
    const until = this.reservedUntil.get(robot.id);
    if (until === undefined) return true;
    if (until <= now) {
      this.reservedUntil.delete(robot.id);
      return true;
    }
    return false;
  }

  /** Reserves the winners so a slower, older round cannot nominate the same robots again. */
  private reserve(robotId: string): void {
    this.reservedUntil.set(robotId, Date.now() + TUNING.reservationMs);
  }

  /** Opens auctions for the oldest, highest-priority unclaimed work while robots are free. */
  private pump(): void {
    if (this.openAuctions.size >= TUNING.maxOpenAuctions) return;
    const now = Date.now();
    const idle = [...this.robots.values()].filter((robot) => robot.available() && this.isFree(robot, now));
    if (!idle.length) return;

    const candidates = [...this.orders.values()]
      .filter((order) => order.status !== "DELIVERED" && neededParts(order).length > 0)
      .sort((a, b) => Number(Boolean(b.priority)) - Number(Boolean(a.priority)) || a.id - b.id);

    for (const order of candidates) {
      if (this.openAuctions.size >= TUNING.maxOpenAuctions) return;
      if (this.openAuctions.has(order.id)) continue;
      if ((this.retryAt.get(order.id) ?? 0) > now) continue;

      const parts = neededParts(order);
      const pools = new Map<Part, RobotAgent[]>();
      for (const part of parts) {
        const target = pickupFor(order, part);
        const pool = [...idle]
          .sort((a, b) => distance(a.position() ?? target, target) - distance(b.position() ?? target, target))
          .slice(0, TUNING.candidatesPerPart);
        if (pool.length) pools.set(part, pool);
      }
      if (!pools.size) return;
      this.startAuction(order, pools);
    }
  }

  /** Asks the nearby robots to bid, then awards the order when they answer or the window expires. */
  private startAuction(order: OrderState, pools: Map<Part, RobotAgent[]>): void {
    this.openAuctions.add(order.id);
    const settled = new Map<Part, SubtaskBid[]>([...pools.keys()].map((part) => [part, []]));
    const finished = { done: false, answered: 0, expected: 0 };
    // Award as soon as every part has a couple of bids: waiting for the slowest model
    // answer costs throughput and, when the model is down, wastes the whole window.
    const enough = (): boolean => [...settled.keys()].every((part) =>
      settled.get(part)!.length >= Math.min(TUNING.bidsToClose, pools.get(part)!.length));

    let finish = (): void => {};
    const done = new Promise<void>((resolve) => { finish = resolve; });

    for (const [part, pool] of pools) {
      finished.expected += pool.length;
      for (const robot of pool) {
        void this.evaluate(order, part, robot)
          .then((bid) => {
            finished.answered++;
            if (finished.done) return;
            if (bid) settled.get(part)!.push(bid);
            if (enough() || finished.answered >= finished.expected) finish();
          })
          .catch((error) => {
            finished.answered++;
            console.warn(`${robot.id}: bid failed (${String(error).slice(0, 120)})`);
            if (finished.answered >= finished.expected) finish();
          });
      }
    }

    const window = new Promise<void>((resolve) => setTimeout(resolve, this.windowMs));
    void Promise.race([done, window]).then(() => {
      finished.done = true;
      this.closeAuction(order, settled, finished.answered);
    });
  }

  /** One robot weighing one part of one order; posts the bid and its radio message. */
  private async evaluate(order: OrderState, part: Part, robot: RobotAgent): Promise<SubtaskBid | null> {
    const state = robot.getState();
    if (!state || !robot.available()) return null;

    const targetPickup = pickupFor(order, part);
    const baseCost = distance([state.x, state.y], targetPickup)
      + distance(targetPickup, order.dropoff)
      + (1 - state.battery) * 10
      - (order.priority ? 2 : 0);

    const decision = await this.limiter.run(() => this.brain.decide({
      robot: { id: state.id, position: [state.x, state.y], battery: state.battery, state: state.state },
      order: { id: order.id, pickup: targetPickup, dropoff: order.dropoff, priority: Boolean(order.priority) },
      baseCost,
    }));

    if (decision.action === "decline") return null;

    const cost = Number(Math.max(1, baseCost + decision.costModifier).toFixed(1));
    const bid: SubtaskBid = {
      order: order.id,
      robot: state.id,
      cost,
      part,
      rationale: decision.rationale,
      commentary: decision.commentary,
    };

    this.post({
      channel: "bids",
      speaker: state.id,
      droppable: true,
      text: `${state.id}: agent bid ${cost} on o${order.id} Part ${part} `
        + `(battery ${(state.battery * 100).toFixed(0)}%; ${decision.rationale.slice(0, 80)})`,
      raw: { t: "AGENT_BID", order: order.id, part, robot: state.id, cost } as Feed,
    });
    this.sendSimEvent?.({ cmd: "agent_bid", robot: state.id, order: order.id, part, cost });

    if (decision.commentary) {
      const text = cleanCommentary(state.id, decision.commentary);
      this.post({
        channel: "fleet",
        speaker: state.id,
        droppable: true,
        text: `💬 ${state.id}: ${text}`,
        raw: { t: "ROBOT_CHAT", robot: state.id, text } as Feed,
      });
      this.sendSimEvent?.({ cmd: "chat", from: state.id, text: `💬 ${state.id}: ${text}` });
    }

    return bid;
  }

  /** Awards to the cheapest robots that are still idle; anything unclaimed is re-auctioned later. */
  private closeAuction(order: OrderState, settled: Map<Part, SubtaskBid[]>, answered = 1): void {
    this.openAuctions.delete(order.id);
    const now = Date.now();
    const usable = (bid: SubtaskBid): boolean => {
      const robot = this.robots.get(bid.robot);
      return Boolean(robot?.available() && this.isFree(robot, now));
    };
    const ranked = (part: Part): SubtaskBid[] =>
      [...(settled.get(part) ?? [])].filter(usable).sort((a, b) => a.cost - b.cost);

    const poolA = ranked("A");
    const poolB = ranked("B");

    if (!poolA.length && !poolB.length) {
      // Nobody answered at all: the model is slow or rate limited, so back off harder
      // instead of spending another round of tokens immediately. Bids that were all
      // taken by a faster round are cheap to redo, so those retry almost at once.
      const hadBids = [...settled.values()].some((bids) => bids.length > 0);
      const wait = answered === 0
        ? TUNING.silentRoundBackoffMs
        : hadBids ? TUNING.contendedRetryMs : TUNING.retryCooldownMs;
      console.log(`o${order.id}: no usable bid in ${this.windowMs} ms (${answered} answered); retrying in ${wait} ms.`);
      this.retryAt.set(order.id, Date.now() + wait);
      return;
    }

    // Hold the order briefly so a claim can land in the simulator before the next round.
    this.retryAt.set(order.id, Date.now() + TUNING.retryCooldownMs);

    if (!order.pickup_b) {
      const winner = poolA[0];
      if (!winner) return;
      const teamMsg = `🏆 Orchestrator: ${winner.robot} won o${order.id} (cost ${winner.cost}). Heading to pickup.`;
      this.sendSimEvent?.({ cmd: "chat", from: "Auction Bus", text: teamMsg });
      this.reserve(winner.robot);
      this.claim(winner.robot, order.id, "A", winner.cost);
      return;
    }

    // Dual assembly: the cheapest pair of distinct robots, so both parts move in parallel.
    let bestPair: [SubtaskBid, SubtaskBid] | null = null;
    let bestCost = Infinity;
    for (const a of poolA) {
      for (const b of poolB) {
        if (a.robot === b.robot) continue;
        if (a.cost + b.cost < bestCost) {
          bestCost = a.cost + b.cost;
          bestPair = [a, b];
        }
      }
    }

    const pair = bestPair ?? (poolA[0] && poolB[0] && poolA[0].robot !== poolB[0].robot
      ? [poolA[0], poolB[0]] as [SubtaskBid, SubtaskBid]
      : null);

    if (!pair) {
      // Only one part could field a bid; ship that part and let the other re-auction.
      const single = poolA[0] ?? poolB[0];
      if (single) {
        this.reserve(single.robot);
        this.claim(single.robot, order.id, single.part, single.cost);
      }
      return;
    }

    const [winnerA, winnerB] = pair;
    const teamMsg = `🤝 Orchestrator: Team formed for Order #${order.id}: ${winnerA.robot} (Part A, ${winnerA.cost}) `
      + `+ ${winnerB.robot} (Part B, ${winnerB.cost}). Parallel execution launched!`;
    this.say(teamMsg);
    this.sendSimEvent?.({ cmd: "chat", from: "Orchestrator", text: teamMsg });

    const msgA = `🤝 ${winnerA.robot}: Teaming with ${winnerB.robot} on o${order.id}! Heading to pick Part A at (${order.pickup}).`;
    const msgB = `🤝 ${winnerB.robot}: Partner confirmed! En route to pickup Part B at (${order.pickup_b}).`;
    this.say(msgA);
    this.say(msgB);
    this.sendSimEvent?.({ cmd: "chat", from: winnerA.robot, text: msgA });
    this.sendSimEvent?.({ cmd: "chat", from: winnerB.robot, text: msgB });

    this.reserve(winnerA.robot);
    this.reserve(winnerB.robot);
    this.claim(winnerA.robot, order.id, "A", winnerA.cost);
    this.claim(winnerB.robot, order.id, "B", winnerB.cost);
  }
}
