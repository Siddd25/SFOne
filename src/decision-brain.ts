/** Per-robot decision policy. Uses OpenAI when OPENAI_API_KEY is configured. */
export type Decision = { action: "bid" | "decline"; costModifier: number; rationale: string; commentary: string };
export type DecisionContext = {
  robot: { id: string; position: [number, number]; battery: number; state: string };
  order: { id: number; pickup: [number, number]; dropoff: [number, number]; priority: boolean };
  baseCost: number;
};

export interface DecisionBrain {
  readonly mode: "llm" | "heuristic";
  decide(context: DecisionContext): Promise<Decision>;
}

const fallback = (context: DecisionContext): Decision => {
  const batteryPct = Math.round(context.robot.battery * 100);
  if (context.robot.battery <= 0.25) {
    return {
      action: "decline", costModifier: 0,
      rationale: "battery too low",
      commentary: `Battery at ${batteryPct}% — too low to take new orders, heading to charge.`,
    };
  }
  if (context.robot.state !== "IDLE") {
    return {
      action: "decline", costModifier: 0,
      rationale: "currently busy",
      commentary: `Can't take o${context.order.id}, still finishing my current delivery.`,
    };
  }
  const dist = Math.abs(context.robot.position[0] - context.order.pickup[0])
    + Math.abs(context.robot.position[1] - context.order.pickup[1]);
  const urgency = context.order.priority ? " Rush order — bidding aggressively!" : "";
  return {
    action: "bid", costModifier: 0,
    rationale: "local battery and travel-cost policy",
    commentary: `I'm ${dist} cells from pickup, battery ${batteryPct}%. Bidding ${context.baseCost.toFixed(1)}.${urgency}`,
  };
};

const schema = {
  type: "object",
  properties: {
    action: { type: "string", enum: ["bid", "decline"] },
    costModifier: { type: "number", minimum: -5, maximum: 5 },
    rationale: { type: "string" },
    commentary: { type: "string", description: "A short first-person message from the robot explaining its decision in a natural, human-like way. Max 120 chars." },
  },
  required: ["action", "costModifier", "rationale", "commentary"],
  additionalProperties: false,
};

function outputText(response: unknown): string | null {
  if (!response || typeof response !== "object") return null;
  const output = (response as { output?: unknown }).output;
  if (!Array.isArray(output)) return null;
  for (const item of output) {
    const content = (item as { content?: unknown }).content;
    if (!Array.isArray(content)) continue;
    for (const part of content) {
      const text = (part as { type?: unknown; text?: unknown }).text;
      if ((part as { type?: unknown }).type === "output_text" && typeof text === "string") return text;
    }
  }
  return null;
}

function firstJsonObject(text: string): string | null {
  const start = text.indexOf("{");
  if (start < 0) return null;
  let depth = 0, quoted = false, escaped = false;
  for (let i = start; i < text.length; i++) {
    const char = text[i];
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') quoted = false;
      continue;
    }
    if (char === '"') quoted = true;
    else if (char === "{") depth++;
    else if (char === "}" && --depth === 0) return text.slice(start, i + 1);
  }
  return null;
}

class OpenAIBrain implements DecisionBrain {
  readonly mode = "llm" as const;
  /** Consecutive failures; after a few, the fleet runs on the local policy for a while. */
  private failures = 0;
  private offlineUntil = 0;

  constructor(private readonly apiKey: string, private readonly model: string) {}

  async decide(context: DecisionContext): Promise<Decision> {
    // A dead network must not cost the demo its pace: skips the round-trip entirely.
    if (Date.now() < this.offlineUntil) return fallback(context);

    const controller = new AbortController();
    // Must land inside the fleet's bid window, or the local fallback arrives too late
    // to be counted and the order is re-auctioned instead of awarded.
    const timeout = setTimeout(() => controller.abort(), 6_000);
    try {
      const batteryPercent = Math.round(context.robot.battery * 100);
      const distToPickup = Math.abs(context.robot.position[0] - context.order.pickup[0])
        + Math.abs(context.robot.position[1] - context.order.pickup[1]);
      const payload = {
        robotId: context.robot.id,
        currentPosition: context.robot.position,
        batteryPercent: batteryPercent,
        batteryStatus: `${batteryPercent}% (${batteryPercent >= 70 ? "Full/High" : batteryPercent >= 40 ? "Good" : batteryPercent >= 25 ? "Adequate" : "Low"})`,
        currentState: context.robot.state,
        orderId: context.order.id,
        pickupLocation: context.order.pickup,
        dropoffLocation: context.order.dropoff,
        distanceToPickup: distToPickup,
        estimatedTotalCost: Number(context.baseCost.toFixed(1)),
        isRushOrder: context.order.priority,
      };

      const response = await fetch("https://api.openai.com/v1/responses", {
        method: "POST",
        headers: { Authorization: `Bearer ${this.apiKey}`, "Content-Type": "application/json" },
        signal: controller.signal,
        body: JSON.stringify({
          model: this.model,
          instructions: "You are an autonomous warehouse mobile robot agent in a multi-agent team (SWARM). "
            + "Evaluate this order and decide whether to BID or DECLINE. "
            + "CRITICAL: batteryPercent is on a 0% to 100% scale. 100% is FULL CHARGE! "
            + "Any robot with batteryPercent >= 25% is fully operational and SHOULD BID if IDLE. "
            + "NEVER decline due to battery if batteryPercent >= 25%. "
            + "Return a JSON object with: "
            + "action ('bid' or 'decline' if busy/dead), "
            + "costModifier (number between -5.0 and 5.0; use negative if very close to bid competitively), "
            + "rationale (under 10 words, e.g. 'closest with full battery'), "
            + "commentary (short, natural first-person radio message to teammate robots, e.g. 'I am 5 cells away with 100% battery, bidding 15.2 on o3', max 120 chars).",
          input: JSON.stringify(payload),
          reasoning: { effort: "none" },
          max_output_tokens: 100,
          text: { format: { type: "json_schema", name: "robot_decision", strict: true, schema } },
        }),
      });
      if (!response.ok) throw new Error(`OpenAI ${response.status}: ${(await response.text()).slice(0, 160)}`);
      const parsed = JSON.parse(firstJsonObject(outputText(await response.json()) ?? "") ?? "") as Partial<Decision>;
      this.failures = 0;
      if ((parsed.action !== "bid" && parsed.action !== "decline")
        || typeof parsed.costModifier !== "number" || typeof parsed.rationale !== "string") throw new Error("invalid decision");
      return {
        action: parsed.action,
        costModifier: Math.max(-5, Math.min(5, parsed.costModifier)),
        rationale: parsed.rationale,
        commentary: typeof parsed.commentary === "string" ? parsed.commentary.slice(0, 150) : parsed.rationale,
      };
    } catch (error) {
      this.failures++;
      if (this.failures >= 3 && this.offlineUntil < Date.now()) {
        this.offlineUntil = Date.now() + 30_000;
        console.warn(`LLM unavailable after ${this.failures} failures; local policy for 30 s.`);
      }
      console.warn(`${context.robot.id}: LLM decision failed; using local fallback (${String(error).slice(0, 120)})`);
      return fallback(context);
    } finally {
      clearTimeout(timeout);
    }
  }
}

export function createDecisionBrain(disabled = false): DecisionBrain {
  const key = process.env.OPENAI_API_KEY;
  if (!disabled && key) return new OpenAIBrain(key, process.env.OPENAI_MODEL ?? "gpt-6-luna");
  return { mode: "heuristic", decide: async (context) => fallback(context) };
}
