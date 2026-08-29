import json
import logging
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alert-webhook")


class Handler(BaseHTTPRequestHandler):
    def _process(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {}
        for alert in payload.get("alerts", []):
            labels = alert.get("labels", {})
            log.info(
                "[ALERT %s] %s | instance=%s | %s",
                alert.get("status", "?"),
                labels.get("alertname", "?"),
                labels.get("instance", "?"),
                alert.get("annotations", {}).get("description", ""),
            )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_POST(self):
        self._process()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
