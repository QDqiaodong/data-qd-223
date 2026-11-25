from example_upstream import upstream
from router import Router

now = [0]
with upstream("primary") as (a, primary), upstream("secondary") as (b, _):
    route = Router((a, b), clock=lambda: now[0])
    print("normal:", route.get("/resource"))
    print("normal:", route.get("/resource"))
    primary["status"] = 503
    print("failover:", route.get("/resource"))
    primary["status"] = 200
    print("rotation:", route.get("/resource"))
    print("cooldown:", route.get("/resource"))
    now[0] = 5
    route.get("/resource")
    print("recovery:", route.get("/resource"))
