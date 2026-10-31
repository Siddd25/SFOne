"""SWARM sim core (Step 1). Pure Python, no dependencies.

One tick = 0.2 s of simulated time (5 Hz). Two modes share the same world:
  - "central": one dispatcher with global knowledge; it only notices a dead
    robot when it polls (every `poll_ticks`).
  - "swarm":   orders are announced on a bus, robots bid, lowest cost wins,
    and peers detect a silent robot via missed heartbeats and re-auction
    its orphaned order.
Both worlds built from the same seed receive the identical order stream.
"""
import random
from collections import deque
from dataclasses import dataclass, field
from functools import lru_cache

W, H = 24, 14
TICK_SECONDS = 0.2
HEARTBEAT_TIMEOUT = 3     # ticks of silence before peers call a robot dead
AUCTION_TICKS = 2         # bid window
LOOKAHEAD = 3             # robots may bid for a next job when <= this many cells from their drop
FAILOVER_TICKS = 75       # central dispatcher failover time (15 s) after it is killed


# ---------------------------------------------------------------- layout
def _is_shelf(x, y):
    return y in (3, 4, 8, 9) and 3 <= x <= 20 and (x - 3) % 4 != 3


BLOCKED = frozenset((x, y) for x in range(W) for y in range(H) if _is_shelf(x, y))


def free_cell(x, y):
    """Clamp to the grid and step off a shelf, so a human-typed coordinate still works."""
    x, y = max(0, min(W - 1, int(x))), max(0, min(H - 1, int(y)))
    if (x, y) not in BLOCKED:
        return (x, y)
    for dy in (1, -1, 2, -2):
        if 0 <= y + dy < H and (x, y + dy) not in BLOCKED:
            return (x, y + dy)
    return (x, y)
CHARGERS = [(x, y) for x in (0, 1) for y in (2, 5, 8, 11)]
STATIONS = [(22, y) for y in (2, 5, 8, 11)]
PICKUPS = [(x, y) for y in (2, 5, 7, 10) for x in range(3, 21)]


@lru_cache(maxsize=None)
def path(a, b):
    """Shortest path (BFS) from a to b, excluding a. Static grid, so cached."""
    if a == b:
        return ()
    prev, q = {a: None}, deque([a])
    while q:
        c = q.popleft()
        if c == b:
            break
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            n = (c[0] + dx, c[1] + dy)
            if 0 <= n[0] < W and 0 <= n[1] < H and n not in BLOCKED and n not in prev:
                prev[n] = c
                q.append(n)
    if b not in prev:
        return ()
    out, c = [], b
    while c != a:
        out.append(c)
        c = prev[c]
    return tuple(reversed(out))


# ---------------------------------------------------------------- entities
@dataclass
class Order:
    id: int
    pickup: tuple                  # Part A pickup
    dropoff: tuple                 # Target delivery station
    priority: int
    created: int
    pickup_b: tuple | None = None  # Part B pickup for collaborative dual assembly
    status: str = "QUEUED"         # QUEUED -> IN_PROGRESS -> DELIVERED
    robot: str | None = None       # Part A robot (or central sequential robot)
    robot_b: str | None = None     # Part B robot (for swarm dual collaboration)
    parts_delivered: set = field(default_factory=set)  # {"A", "B"}
    done: int | None = None
    retry_at: int = 0


@dataclass
class Robot:
    id: str
    pos: tuple
    battery: float = 1.0
    state: str = "IDLE"           # IDLE TO_PICK TO_PICK_B TO_DROP TO_CHARGE CHARGING DEAD
    alive: bool = True
    order: int | None = None
    part: str = "A"               # which part ("A" or "B")
    next_order: int | None = None   # job lined up while finishing the current one
    path: deque = field(default_factory=deque)


# ---------------------------------------------------------------- world
class World:
    def __init__(self, mode, seed=7, n_robots=8, poll_ticks=50, spawn_prob=0.12,
                 agent_controlled=False):
        assert mode in ("central", "swarm")
        self.mode, self.poll_ticks, self.spawn_prob = mode, poll_ticks, spawn_prob
        self.agent_controlled = agent_controlled and mode == "swarm"
        self.rng = random.Random(seed)            # order stream (identical per mode)
        self.cmd_rng = random.Random(seed + 1)    # operator commands
        self.tick = 0
        self.robots = [Robot(f"r{i + 1}", CHARGERS[i]) for i in range(n_robots)]
        self.rob = {r.id: r for r in self.robots}
        self.orders: dict[int, Order] = {}
        self.next_order = 1
        self.inbox, self.outbox, self.feed = [], [], []
        self.auctions: dict[int, dict] = {}
        self.last_seen = {r.id: 0 for r in self.robots}   # heartbeat bookkeeping
        self.deaths: dict[str, int] = {}
        self.recov: dict[int, dict] = {}
        self.stats = {"distance": 0, "idle_ticks": 0}
        self.dispatcher_up = True          # only meaningful in central mode
        self.dispatcher_back_at = 0

    # ---- bus
    def post(self, msg):
        msg["tick"] = self.tick
        self.outbox.append(msg)       # visible to everyone next tick (models latency)
        self.feed.append(msg)

    # ---- operator commands (these become the demo buttons)
    def _create_order(self, rng, priority=0):
        pA = rng.choice(PICKUPS)
        pB = rng.choice([p for p in PICKUPS if abs(p[0] - pA[0]) + abs(p[1] - pA[1]) >= 8] or PICKUPS)
        drop = rng.choice(STATIONS)
        return Order(self.next_order, pA, drop, priority, self.tick, pickup_b=pB)

    def add_order(self, priority=0):
        o = self._create_order(self.cmd_rng, priority)
        self.orders[o.id] = o
        self.next_order += 1
        return o

    def add_custom_order(self, pickup, dropoff, priority=0, pickup_b=None):
        """A task typed by a human: same auction path, coordinates instead of random."""
        o = Order(self.next_order, tuple(pickup), tuple(dropoff), priority, self.tick,
                  pickup_b=tuple(pickup_b) if pickup_b else None)
        self.orders[o.id] = o
        self.next_order += 1
        return o

    def kill(self, robot_id):
        r = self.rob[robot_id]
        r.alive, r.state = False, "DEAD"
        self.deaths[robot_id] = self.tick
        self.feed.append({"t": "FAULT_INJECTED", "robot": robot_id, "tick": self.tick})

    def kill_dispatcher(self):
        """Central mode: the brain dies and returns after FAILOVER_TICKS. Swarm: no such node."""
        if self.mode != "central":
            self.feed.append({"t": "FAULT_INJECTED", "target": "dispatcher",
                              "note": "no central node exists", "tick": self.tick})
            return False
        self.dispatcher_up = False
        self.dispatcher_back_at = self.tick + FAILOVER_TICKS
        self.feed.append({"t": "FAULT_INJECTED", "target": "dispatcher", "tick": self.tick})
        return True

    # ---- main loop
    def step(self):
        self.tick += 1
        self.inbox, self.outbox = self.outbox, []
        if self.rng.random() < self.spawn_prob:
            o = self._create_order(self.rng, 0)
            self.orders[o.id] = o
            self.next_order += 1
        (self._swarm if self.mode == "swarm" else self._central)()
        self._move()

    # ---- shared helpers
    def _needs_parts(self, o):
        needed = []
        if o.robot is None and "A" not in o.parts_delivered:
            needed.append("A")
        if o.pickup_b and o.robot_b is None and "B" not in o.parts_delivered:
            needed.append("B")
        return needed

    def _queued(self):
        return sorted((o for o in self.orders.values() if o.status != "DELIVERED" and self._needs_parts(o)),
                      key=lambda o: (-o.priority, o.id))

    def _assign_central(self, r, o):
        o.status, o.robot = "IN_PROGRESS", r.id
        r.order, r.part = o.id, "A"
        r.state = "TO_PICK"
        r.path = deque(path(r.pos, o.pickup))
        rec = self.recov.get(o.id)
        if rec and "reassigned" not in rec:
            rec["reassigned"] = self.tick

    def _silent_id(self, robot_id):
        return self.tick - self.last_seen.get(robot_id, 0) > HEARTBEAT_TIMEOUT

    def _orphan_part(self, o, part):
        rid = o.robot if part == "A" else o.robot_b
        if not rid:
            return
        r = self.rob[rid]
        if part in o.parts_delivered:
            return
        if r.state == "TO_DROP":
            if part == "A":
                o.pickup = r.pos
            else:
                o.pickup_b = r.pos
        if part == "A":
            o.robot = None
        else:
            o.robot_b = None
        o.status = "QUEUED"
        self.recov[o.id] = {"death": self.deaths.get(rid, self.tick), "detected": self.tick}
        self.post({"t": "TASK_ORPHANED", "order": o.id, "part": part, "lastRobot": rid})

    def _cost_central(self, r, o):
        start = r.pos
        extra = 0
        if r.state == "TO_DROP" and r.order and r.order in self.orders:
            start, extra = self.orders[r.order].dropoff, len(r.path)
        d_A = len(path(start, o.pickup))
        d_AB = len(path(o.pickup, o.pickup_b)) if o.pickup_b else 0
        p_final = o.pickup_b or o.pickup
        d_drop = len(path(p_final, o.dropoff))
        return extra + d_A + d_AB + d_drop + (1 - r.battery) * 10

    def _cost_subtask(self, r, target_pickup, target_dropoff):
        start = r.pos
        extra = 0
        if r.state == "TO_DROP" and r.order and r.order in self.orders:
            start, extra = self.orders[r.order].dropoff, len(r.path)
        return extra + len(path(start, target_pickup)) + len(path(target_pickup, target_dropoff)) + (1 - r.battery) * 10

    def _available(self, r):
        if not r.alive or r.battery <= 0.25:
            return False
        if r.state == "IDLE":
            return True
        return r.state == "TO_DROP" and r.next_order is None and len(r.path) <= LOOKAHEAD

    # ---- SWARM: announce -> bid -> award, plus peer watchdog
    def _swarm(self):
        for o in self.orders.values():                       # peer watchdog
            if o.status == "IN_PROGRESS":
                if o.robot and self._silent_id(o.robot):
                    self._orphan_part(o, "A")
                if o.robot_b and self._silent_id(o.robot_b):
                    self._orphan_part(o, "B")
        for o in self._queued():
            needed = self._needs_parts(o)
            if not needed:
                continue
            if o.id not in self.auctions or self.auctions[o.id].get("needed") != needed:
                self.auctions[o.id] = {"open": self.tick, "needed": needed}
                self.post({
                    "t": "ORDER_POSTED",
                    "order": o.id,
                    "pickup": o.pickup,
                    "pickup_b": o.pickup_b,
                    "dropoff": o.dropoff,
                    "priority": o.priority,
                    "requires": needed,
                })
        if not self.agent_controlled:
            # Internal swarm fallback for headless comparisons
            for oid in list(self.auctions.keys()):
                o = self.orders[oid]
                needed = self._needs_parts(o)
                avail = [r for r in self.robots if self._available(r)]
                if "A" in needed and avail:
                    rA = min(avail, key=lambda r: self._cost_subtask(r, o.pickup, o.dropoff))
                    self._internal_claim(rA.id, o.id, "A")
                    avail = [r for r in avail if r.id != rA.id]
                if "B" in needed and avail:
                    rB = min(avail, key=lambda r: self._cost_subtask(r, o.pickup_b or o.pickup, o.dropoff))
                    self._internal_claim(rB.id, o.id, "B")
                if not self._needs_parts(o):
                    self.auctions.pop(oid, None)

    def _internal_claim(self, robot_id, order_id, part="A"):
        r, o = self.rob.get(robot_id), self.orders.get(order_id)
        if not r or not o or not self._available(r): return
        part = str(part).upper()
        if part == "A":
            o.robot = r.id; pickup_loc = o.pickup
        else:
            o.robot_b = r.id; pickup_loc = o.pickup_b or o.pickup
        o.status = "IN_PROGRESS"
        r.order, r.part = o.id, part
        r.state = "TO_PICK"
        r.path = deque(path(r.pos, pickup_loc))
        self.post({"t": "AWARD", "order": o.id, "part": part, "robot": r.id, "cost": round(self._cost_subtask(r, pickup_loc, o.dropoff), 1)})

    def claim(self, robot_id, order_id, part="A", cost=None):
        """Validate a winning external-agent claim for a specific part of an order."""
        if not self.agent_controlled:
            return {"ok": False, "error": "swarm is not in agent-controlled mode"}
        r, o = self.rob.get(robot_id), self.orders.get(order_id)
        if r is None or o is None:
            return {"ok": False, "error": "unknown robot or order"}
        if o.status == "DELIVERED" or not self._available(r):
            return {"ok": False, "error": "order already delivered or robot unavailable"}

        part = str(part or "A").upper()
        if part == "A":
            if o.robot and o.robot != r.id and self.rob[o.robot].alive and self.rob[o.robot].order == o.id:
                return {"ok": False, "error": f"Part A already claimed by {o.robot}"}
            o.robot = r.id
            pickup_loc = o.pickup
        else:
            if o.robot_b and o.robot_b != r.id and self.rob[o.robot_b].alive and self.rob[o.robot_b].order == o.id:
                return {"ok": False, "error": f"Part B already claimed by {o.robot_b}"}
            o.robot_b = r.id
            pickup_loc = o.pickup_b or o.pickup

        o.status = "IN_PROGRESS"
        r.order, r.part = o.id, part
        r.state = "TO_PICK"
        r.path = deque(path(r.pos, pickup_loc))

        rec = self.recov.get(o.id)
        if rec and "reassigned" not in rec:
            rec["reassigned"] = self.tick

        self.post({"t": "AWARD", "order": o.id, "part": part, "robot": r.id,
                   "cost": round(float(cost if cost is not None else len(r.path)), 1),
                   "source": "agent"})
        return {"ok": True, "robot": r.id, "order": o.id, "part": part}

    def chat(self, robot_id, text):
        """Post a general chat message from a robot to the feed."""
        self.feed.append({"t": "ROBOT_CHAT", "robot": robot_id, "text": text, "tick": self.tick})

    # ---- CENTRAL baseline: global knowledge, but single-robot sequential with polling
    def _central(self):
        if not self.dispatcher_up:
            if self.tick < self.dispatcher_back_at:
                return
            self.dispatcher_up = True
            self.feed.append({"t": "DISPATCHER_RESTORED", "tick": self.tick})
        if self.tick % self.poll_ticks == 0:
            for o in self.orders.values():
                if o.status == "IN_PROGRESS" and o.robot and self._silent_id(o.robot):
                    self._orphan_part(o, "A")
        for o in self._queued():
            if o.status == "IN_PROGRESS" and o.robot:
                continue
            idle = [r for r in self.robots if self._available(r)]
            if not idle:
                break
            r = min(idle, key=lambda r: self._cost_central(r, o))
            self._assign_central(r, o)
            self.post({"t": "ASSIGN", "order": o.id, "robot": r.id})

    # ---- movement / lifecycle
    def _move(self):
        for r in self.robots:
            if not r.alive:
                continue
            self.last_seen[r.id] = self.tick                 # heartbeat
            if r.state in ("TO_PICK", "TO_PICK_B", "TO_DROP", "TO_CHARGE"):
                if r.path:
                    r.pos = r.path.popleft()
                    r.battery -= 0.003
                    self.stats["distance"] += 1
                if not r.path:
                    self._arrive(r)
            elif r.state == "CHARGING":
                r.battery = min(1.0, r.battery + 0.02)
                if r.battery >= 0.9:
                    r.state = "IDLE"
            elif r.state == "IDLE":
                self.stats["idle_ticks"] += 1
                if r.battery < 0.3:
                    goal = min(CHARGERS, key=lambda c: len(path(r.pos, c)))
                    r.state, r.path = "TO_CHARGE", deque(path(r.pos, goal))

    def _arrive(self, r):
        if r.state == "TO_PICK":
            o = self.orders[r.order]
            if self.mode == "central" and o.pickup_b:
                r.state = "TO_PICK_B"
                r.path = deque(path(r.pos, o.pickup_b))
                self.post({"t": "PART_PICKED", "order": o.id, "part": "A", "robot": r.id})
            else:
                o.status = "IN_PROGRESS"
                r.state = "TO_DROP"
                r.path = deque(path(r.pos, o.dropoff))
                self.post({"t": "PART_PICKED", "order": o.id, "part": getattr(r, "part", "A"), "robot": r.id})
        elif r.state == "TO_PICK_B":
            o = self.orders[r.order]
            r.state = "TO_DROP"
            r.path = deque(path(r.pos, o.dropoff))
            self.post({"t": "PART_PICKED", "order": o.id, "part": "B", "robot": r.id})
        elif r.state == "TO_DROP":
            o = self.orders[r.order]
            if self.mode == "central":
                o.status, o.done = "DELIVERED", self.tick
                r.state, r.order = "IDLE", None
                self.post({"t": "DELIVERED", "order": o.id, "robot": r.id})
            else:
                part = getattr(r, "part", "A")
                o.parts_delivered.add(part)
                r.state, r.order = "IDLE", None
                self.post({"t": "PART_DELIVERED", "order": o.id, "part": part, "robot": r.id})
                if o.pickup_b is None or {"A", "B"}.issubset(o.parts_delivered):
                    o.status, o.done = "DELIVERED", self.tick
                    team = [p for p in (o.robot, o.robot_b) if p]
                    self.post({"t": "DELIVERED", "order": o.id, "robot": r.id, "team": team})
            if r.next_order is not None and r.next_order in self.orders:
                n = self.orders[r.next_order]
                r.next_order, r.order, r.state, r.part = None, n.id, "TO_PICK", "A"
                r.path = deque(path(r.pos, n.pickup))
        elif r.state == "TO_CHARGE":
            r.state = "CHARGING"

    # ---- outputs
    def metrics(self):
        done = [o for o in self.orders.values() if o.status == "DELIVERED"]
        rec = [(v["reassigned"] - v["death"]) * TICK_SECONDS
               for v in self.recov.values() if "reassigned" in v]
        open_orders = [o for o in self.orders.values() if o.status != "DELIVERED"]
        return {
            "delivered": len(done),
            "avg_order_time_s": round(sum((o.done - o.created) for o in done) * TICK_SECONDS
                                      / max(1, len(done)), 1),
            "recovery_s": [round(x, 1) for x in rec],
            "backlog": len(open_orders),
            "oldest_wait_s": round(max((self.tick - o.created for o in open_orders),
                                       default=0) * TICK_SECONDS, 1),
            "distance": self.stats["distance"],
            "idle_ticks": self.stats["idle_ticks"],
        }

    def snapshot(self, feed_tail=16):
        """JSON-serialisable state for the Unity/HTML viewer."""
        return {
            "tick": self.tick, "mode": self.mode, "grid": [W, H],
            "dispatcher": ("none" if self.mode == "swarm"
                           else "up" if self.dispatcher_up else "down"),
            "robots": [{"id": r.id, "x": r.pos[0], "y": r.pos[1], "state": r.state,
                        "battery": round(r.battery, 2), "order": r.order, "part": getattr(r, "part", "A"),
                        "alive": r.alive,
                        "path": [list(p) for p in list(r.path)[:30]],
                        "target": list(self.orders[r.order].dropoff) if r.order and r.state == "TO_DROP" and r.order in self.orders
                                  else (list(self.orders[r.order].pickup_b) if r.order and r.state == "TO_PICK_B" and r.order in self.orders
                                  else (list(self.orders[r.order].pickup if getattr(r, 'part', 'A') == 'A' else self.orders[r.order].pickup_b or self.orders[r.order].pickup) if r.order and r.state == "TO_PICK" and r.order in self.orders
                                  else None)),
                        "nextOrder": r.next_order}
                       for r in self.robots],
            "orders": [{"id": o.id, "status": o.status, "pickup": o.pickup,
                        "pickup_b": o.pickup_b, "dropoff": o.dropoff, "priority": o.priority,
                        "robot": o.robot, "robot_b": o.robot_b,
                        "parts_delivered": list(o.parts_delivered)}
                       for o in self.orders.values() if o.status != "DELIVERED"],
            "metrics": self.metrics(),
            "feed": self.feed[-feed_tail:],
        }
