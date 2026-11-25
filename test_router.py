import unittest
from example_upstream import upstream
from router import Router, Unavailable


class RouterTest(unittest.TestCase):
    def test_failover_cooldown_and_reentry(self):
        now = [0]
        with upstream("a") as (a, sa), upstream("b") as (b, sb):
            proxy = Router((a, b), clock=lambda: now[0])
            self.assertEqual(proxy.get("/first"), (200, b"a"))
            self.assertEqual(proxy.get("/second"), (200, b"b"))
            sa["status"] = 503
            self.assertEqual(proxy.get("/failure"), (200, b"b"))
            proxy.get("/third")
            proxy.get("/fourth")
            self.assertEqual(sa["requests"], ["/first", "/failure"])
            sa["status"] = 200
            now[0] = 5
            proxy.get("/again")
            self.assertEqual(proxy.get("/recovered"), (200, b"a"))
            self.assertEqual(sa["requests"][-1], "/recovered")

    def test_all_failed_and_4xx_not_retried(self):
        with upstream("a") as (a, sa), upstream("b") as (b, sb):
            proxy = Router((a, b), clock=lambda: 1)
            sa["status"], sb["status"] = 404, 502
            self.assertEqual(proxy.get("/missing"), (404, b"a"))
            self.assertEqual(sb["requests"], [])
            sa["status"] = 500
            with self.assertRaises(Unavailable):
                proxy.get("/broken")
            self.assertEqual(sb["requests"], ["/broken"])
