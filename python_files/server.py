"""SWARM server (Step 2).

Runs two worlds in lockstep from the same seed (baseline "central" vs "swarm"),
streams both snapshots to every client at 5 Hz, and accepts operator commands.
Every fault and every rush order is applied to BOTH worlds so comparisons are fair.

    python server.py                  # ws://localhost:8765
    python server.py --spawn 0.17     # busier warehouse

Protocol (JSON text frames)
  server -> client   {"type":"hello", ...}                    once on connect
                     {"type":"state","tick":N,"central":{..},"swarm":{..}}   every tick
                     {"type":"ack","cmd":"kill", ...}          reply to a command
  client -> server   {"cmd":"kill"}                 kill a robot carrying a package (same id in both worlds)
                     {"cmd":"kill","robot":"r3"}    kill a specific robot
                     {"cmd":"kill_dispatcher"}      central brain dies (swarm has none)
                     {"cmd":"rush"}                 priority order, identical in both worlds
                     {"cmd":"chaos"}                dispatcher + two robots at once
                     {"cmd":"pause"} {"cmd":"resume"} {"cmd":"speed","x":2}
                     {"cmd":"reset","seed":7}
"""
import argparse
import asyncio
import json
import random
import time

import websockets

from sim import TICK_SECONDS, W, H, World, free_cell


class Arena:
    def __init__(self, seed=7, spawn=0.17, agent_controlled=False):
        self.seed, self.spawn = seed, spawn
        self.agent_controlled = agent_controlled
        self.pick_rng = random.Random(0)
        self.paused, self.speed = False, 1.0
        self.reset()

    def reset(self, seed=None):
        if seed is not None:
            self.seed = int(seed)
        self.worlds = {m: World(m, seed=self.seed, spawn_prob=self.spawn,
                                agent_controlled=self.agent_controlled)
                       for m in ("central", "swarm")}

    def step(self):
        for w in self.worlds.values():
            w.step()

    def state(self):
        return {"type": "state", "tick": self.worlds["swarm"].tick, "paused": self.paused,
                "speed": self.speed,
                **{m: w.snapshot() for m, w in self.worlds.items()}}

    # -- fairness: pick ONE victim id and kill it in both worlds
    def pick_victim(self):
        c, s = self.worlds["central"].rob, self.worlds["swarm"].rob
        alive = [rid for rid in c if c[rid].alive and s[rid].alive]
        if not alive:
            return None
        both = [r for r in alive if c[r].state == "TO_DROP" and s[r].state == "TO_DROP"]
        either = [r for r in alive if c[r].state == "TO_DROP" or s[r].state == "TO_DROP"]
        return self.pick_rng.choice(both or either or alive)

    def kill(self, rid=None):
        rid = rid or self.pick_victim()
        if rid is None or not all(w.rob[rid].alive for w in self.worlds.values()):
            return {"ok": False, "error": "no such live robot"}
        for w in self.worlds.values():
            w.kill(rid)
        return {"ok": True, "robot": rid}

    def command(self, msg):
        cmd = msg.get("cmd")
        if cmd == "kill":
            return self.kill(msg.get("robot"))
        if cmd == "kill_dispatcher":
            for w in self.worlds.values():
                w.kill_dispatcher()
            return {"ok": True}
        if cmd == "rush":
            ids = [w.add_order(priority=1).id for w in self.worlds.values()]
            return {"ok": True, "orders": ids}
        if cmd == "task":
            # A task typed by a human, applied to both worlds so the race stays fair.
            p, d = msg.get("pickup"), msg.get("dropoff")
            try:
                pickup = free_cell(p[0], p[1])
                dropoff = free_cell(d[0], d[1])
                pb = msg.get("pickup_b")
                pickup_b = free_cell(pb[0], pb[1]) if pb else None
            except (TypeError, IndexError, ValueError):
                return {"ok": False, "error": "task needs pickup:[x,y] and dropoff:[x,y]"}
            priority = 1 if msg.get("priority") else 0
            ids = [w.add_custom_order(pickup, dropoff, priority, pickup_b).id for w in self.worlds.values()]
            return {"ok": True, "orders": ids, "pickup": list(pickup),
                    "pickup_b": list(pickup_b) if pickup_b else None, "dropoff": list(dropoff)}
        if cmd == "claim":
            return self.worlds["swarm"].claim(
                msg.get("robot"), msg.get("order"), msg.get("part", "A"), msg.get("cost")
            )
        if cmd == "chaos":
            for w in self.worlds.values():
                w.kill_dispatcher()
            killed = []
            for _ in range(2):
                r = self.kill()
                if r.get("ok"):
                    killed.append(r["robot"])
            return {"ok": True, "killed": killed}
        if cmd in ("pause", "resume"):
            self.paused = cmd == "pause"
            return {"ok": True}
        if cmd == "speed":
            self.speed = max(0.25, min(20.0, float(msg.get("x", 1))))
            return {"ok": True, "speed": self.speed}
        if cmd == "reset":
            self.reset(msg.get("seed"))
            return {"ok": True, "seed": self.seed}
        if cmd == "agent_bid":
            r, o, c = msg.get("robot"), msg.get("order"), msg.get("cost")
            self.worlds["swarm"].feed.append({"t": "AGENT_BID", "robot": r, "order": o, "cost": c, "tick": self.worlds["swarm"].tick})
            return {"ok": True}
        if cmd == "chat":
            text = msg.get("text", "")
            sender = msg.get("from", "human")
            for w in self.worlds.values():
                w.feed.append({"t": "ROBOT_CHAT", "robot": sender, "text": text, "tick": w.tick})
            return {"ok": True}
        return {"ok": False, "error": f"unknown cmd {cmd!r}"}


async def main(host, port, seed, spawn, agent_controlled):
    arena, clients = Arena(seed, spawn, agent_controlled), set()

    async def handler(ws):
        clients.add(ws)
        try:
            await ws.send(json.dumps({"type": "hello", "grid": [W, H],
                                      "tick_seconds": TICK_SECONDS, "seed": arena.seed}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    res = arena.command(msg)
                except Exception as e:                      # bad JSON etc. never kills the server
                    msg, res = {}, {"ok": False, "error": str(e)}
                await ws.send(json.dumps({"type": "ack", "cmd": msg.get("cmd"), **res}))
                websockets.broadcast(clients, json.dumps(arena.state()))   # show effect immediately
        finally:
            clients.discard(ws)

    async with websockets.serve(handler, host, port):
        print(f"SWARM server on ws://{host}:{port}  (seed {seed}, spawn {spawn})")
        next_t = time.perf_counter()
        while True:
            if arena.paused:
                await asyncio.sleep(0.05)
                next_t = time.perf_counter()
                continue
            arena.step()
            websockets.broadcast(clients, json.dumps(arena.state()))
            next_t += TICK_SECONDS / arena.speed
            delay = next_t - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            else:
                next_t = time.perf_counter()                 # fell behind: resync, don't spiral


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--spawn", type=float, default=0.17, help="order arrival probability per tick")
    ap.add_argument("--agent-controlled", action="store_true",
                    help="let Relay robot agents claim swarm orders instead of the internal auction")
    a = ap.parse_args()
    try:
        asyncio.run(main(a.host, a.port, a.seed, a.spawn, a.agent_controlled))
    except KeyboardInterrupt:
        pass
