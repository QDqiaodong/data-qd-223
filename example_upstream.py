"""Local HTTP fixture used to demonstrate real upstream requests."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


@contextmanager
def upstream(name):
    state = {"status": 200, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"].append(self.path)
            body = name.encode()
            self.send_response(state["status"])
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=lambda: server.serve_forever(poll_interval=0.02))
        worker.start()
        try:
            yield server.server_address, state
        finally:
            server.shutdown()
            worker.join()
