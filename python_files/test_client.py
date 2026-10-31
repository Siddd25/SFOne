"""Smoke test for server.py. Start the server first, then:  python test_client.py"""
import asyncio, json
import websockets

async def recv_state(ws):
    while True:
        m = json.loads(await asyncio.wait_for(ws.recv(), 5))
        if m["type"] == "state":
            return m

async def send(ws, **cmd):
    await ws.send(json.dumps(cmd))
    while True:
        m = json.loads(await asyncio.wait_for(ws.recv(), 5))
        if m["type"] == "ack":
            return m

async def main():
    async with websockets.connect("ws://localhost:8765") as ws:
        hello = json.loads(await ws.recv()); assert hello["type"] == "hello"; print("hello:", hello)
        await send(ws, cmd="speed", x=10)                      # run fast for the test
        for _ in range(60): st = await recv_state(ws)
        print("tick", st["tick"], "| robots", len(st["swarm"]["robots"]), "| dispatcher", st["central"]["dispatcher"], st["swarm"]["dispatcher"])

        ack = await send(ws, cmd="kill"); print("kill ->", ack)
        st = await recv_state(ws)
        dead = lambda w: sorted(r["id"] for r in st[w]["robots"] if not r["alive"])
        assert dead("central") == dead("swarm") == [ack["robot"]], "same robot must die in both worlds"
        print("same victim in both worlds: OK")

        ack = await send(ws, cmd="rush"); print("rush ->", ack)
        assert ack["orders"][0] == ack["orders"][1]
        st = await recv_state(ws)
        pri = lambda w: sorted((o["pickup"], o["dropoff"]) for o in st[w]["orders"] if o["priority"] == 1)
        assert pri("central") == pri("swarm") != []
        print("identical rush order in both worlds: OK")

        print("kill_dispatcher ->", await send(ws, cmd="kill_dispatcher"))
        st = await recv_state(ws)
        assert st["central"]["dispatcher"] == "down" and st["swarm"]["dispatcher"] == "none"
        print("dispatcher: central", st["central"]["dispatcher"], "| swarm", st["swarm"]["dispatcher"])

        for _ in range(250): st = await recv_state(ws)          # let the 15 s failover elapse (sped up)
        print("after outage: central", st["central"]["dispatcher"])
        print("metrics central:", st["central"]["metrics"])
        print("metrics swarm  :", st["swarm"]["metrics"])
        print("chaos ->", await send(ws, cmd="chaos"))
        print("bad cmd ->", await send(ws, cmd="nope"))
        await ws.send("not json"); 
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), 5))
            if m["type"] == "ack": print("bad json ->", m); break
        print("reset ->", await send(ws, cmd="reset", seed=7))
        st = await recv_state(ws); assert st["tick"] <= 2; print("reset OK, tick", st["tick"])
    print("\nALL CHECKS PASSED")

asyncio.run(main())
