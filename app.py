from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time

USERS = {}
TOKENS = {}
ORDERS = []
V2_ORDERS = []
TRADES = []


class Handler(BaseHTTPRequestHandler):
    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _send_no_content(self, status: int):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_gbuf(self, status: int, obj: dict):
        body = encode_message(obj)
        self.send_response(status)
        self.send_header("Content-Type", "application/x-galacticbuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _get_authenticated_user(self):
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:].strip()
        if not token:
            return None
        return TOKENS.get(token)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/orders":
            self.handle_list_orders(parsed)

        elif parsed.path == "/trades":
            self.handle_list_trades()

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/register":
            self.handle_register()
        elif self.path == "/login":
            self.handle_login()
        elif self.path == "/orders":
            self.handle_submit_order()
        elif self.path == "/v2/orders":
            self.handle_submit_order_v2()
        elif self.path == "/trades":
            self.handle_take_order()
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        if self.path == "/user/password":
            self.handle_change_password()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_register(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()

        if not username or not password:
            self._send_no_content(400)
            return

        if username in USERS:
            self._send_no_content(409)
            return

        USERS[username] = password
        self._send_no_content(204)

    def handle_login(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(401)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()

        if not username or not password:
            self._send_no_content(401)
            return

        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        token = uuid.uuid4().hex
        TOKENS[token] = username

        self._send_gbuf(200, {"token": token})

    def handle_change_password(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        old_password = (data.get("old_password") or "").strip()
        new_password = (data.get("new_password") or "").strip()

        if not username or not old_password or not new_password:
            self._send_no_content(400)
            return

        user_password = USERS.get(username)
        if user_password is None:
            self._send_no_content(401)
            return

        if user_password != old_password:
            self._send_no_content(401)
            return

        try:
            USERS[username] = new_password
            tokens_to_delete = [t for t, u in TOKENS.items() if u == username]
            for t in tokens_to_delete:
                del TOKENS[t]
        except Exception:
            self._send_no_content(500)
            return

        self._send_no_content(204)

    def handle_list_orders(self, parsed):
        qs = parse_qs(parsed.query)

        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        matching = [
            o for o in ORDERS
            if o.get("active", True)
            and int(o.get("delivery_start", 0)) == delivery_start
            and int(o.get("delivery_end", 0)) == delivery_end
        ]

        matching.sort(key=lambda o: int(o["price"]))

        orders_payload = []
        for o in matching:
            orders_payload.append({
                "order_id": str(o["order_id"]),
                "price": int(o["price"]),
                "quantity": int(o["quantity"]),
                "delivery_start": int(o["delivery_start"]),
                "delivery_end": int(o["delivery_end"]),
            })

        self._send_gbuf(200, {"orders": orders_payload})

    def handle_submit_order(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        try:
            price = int(data.get("price"))
            quantity = int(data.get("quantity"))
            delivery_start = int(data.get("delivery_start"))
            delivery_end = int(data.get("delivery_end"))
        except Exception:
            self._send_no_content(400)
            return

        if quantity <= 0:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        order_id = uuid.uuid4().hex
        order = {
            "order_id": order_id,
            "seller_id": username,
            "price": price,
            "quantity": quantity,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "active": True,
        }
        ORDERS.append(order)

        self._send_gbuf(200, {"order_id": order_id})

    def handle_submit_order_v2(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        side = (data.get("side") or "").strip()
        try:
            price = int(data.get("price"))
            quantity = int(data.get("quantity"))
            delivery_start = int(data.get("delivery_start"))
            delivery_end = int(data.get("delivery_end"))
        except Exception:
            self._send_no_content(400)
            return

        if side not in ("buy", "sell"):
            self._send_no_content(400)
            return

        if quantity <= 0:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start:
            self._send_no_content(400)
            return
        if delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        incoming = {
            "order_id": order_id,
            "side": side,
            "owner": username,
            "price": price,
            "quantity": quantity,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "status": "ACTIVE",
            "active": True,
            "created_at": now_ms,
        }

        remaining = quantity
        filled_quantity = 0

        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("active", True)
                and o["side"] == "sell"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] <= price
            ]
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in V2_ORDERS
                if o.get("active", True)
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] >= price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if remaining <= 0:
                break
            if not resting.get("active", True) or resting["quantity"] <= 0:
                continue

            if side == "buy" and price < resting["price"]:
                continue
            if side == "sell" and price > resting["price"]:
                continue

            trade_qty = min(remaining, resting["quantity"])
            if trade_qty <= 0:
                continue

            if side == "buy":
                buyer_id = username
                seller_id = resting["owner"]
            else:
                buyer_id = resting["owner"]
                seller_id = username

            trade_price = resting["price"]
            trade_id = uuid.uuid4().hex
            ts = int(time.time() * 1000)

            TRADES.append({
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
            })

            remaining -= trade_qty
            filled_quantity += trade_qty

            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"
                resting["active"] = False

        incoming["quantity"] = remaining
        if remaining <= 0:
            incoming["status"] = "FILLED"
            incoming["active"] = False
        else:
            incoming["status"] = "ACTIVE"
            incoming["active"] = True
            V2_ORDERS.append(incoming)

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": incoming["status"],
            "filled_quantity": filled_quantity,
        })

    def handle_list_trades(self):
        trades_sorted = sorted(TRADES, key=lambda t: int(t["timestamp"]), reverse=True)

        trades_payload = []
        for t in trades_sorted:
            trades_payload.append({
                "trade_id": str(t["trade_id"]),
                "buyer_id": str(t["buyer_id"]),
                "seller_id": str(t["seller_id"]),
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "timestamp": int(t["timestamp"]),
            })

        self._send_gbuf(200, {"trades": trades_payload})

    def handle_take_order(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        order_id = (data.get("order_id") or "").strip()
        if not order_id:
            self._send_no_content(400)
            return

        order = None
        for o in ORDERS:
            if o.get("order_id") == order_id and o.get("active", True):
                order = o
                break

        if not order:
            self._send_no_content(404)
            return

        order["active"] = False
        ORDERS.remove(order)

        trade_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        trade = {
            "trade_id": trade_id,
            "buyer_id": username,
            "seller_id": order["seller_id"],
            "price": int(order["price"]),
            "quantity": int(order["quantity"]),
            "timestamp": now_ms,
        }
        TRADES.append(trade)

        self._send_gbuf(200, {"trade_id": trade_id})


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
