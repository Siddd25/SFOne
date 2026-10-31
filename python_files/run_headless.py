"""Run the identical scenario in both modes and compare. No UI needed.

    python run_headless.py
"""
from sim import World, TICK_SECONDS

TICKS = 900
KILL_AFTER = 150          # kill the first robot found carrying a package after this tick


def run(mode, seed=7):
    w = World(mode, seed=seed)
    victim = None
    for _ in range(TICKS):
        w.step()
        if victim is None and w.tick >= KILL_AFTER:
            carrying = [r for r in w.robots if r.state == "TO_DROP"]
            if carrying:
                victim = carrying[0].id
                w.kill(victim)
    return w, victim


if __name__ == "__main__":
    print(f"{TICKS} ticks = {TICKS * TICK_SECONDS:.0f}s simulated, 8 robots, seed 7\n")
    for mode in ("central", "swarm"):
        w, victim = run(mode)
        m = w.metrics()
        print(f"[{mode:7}] killed {victim} | recovery {m['recovery_s']} s | "
              f"delivered {m['delivered']} | avg order {m['avg_order_time_s']} s | "
              f"backlog {m['backlog']} | distance {m['distance']}")
    print("\nLast swarm feed messages:")
    for msg in w.feed[-8:]:
        print("  ", msg)
