from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time
import base64
import hashlib

USERS = {}
TOKENS = {}

ORDERS = []
V2_ORDERS = []
TRADES = []

BALANCES = {}
COLLATERAL = {}

DNA_SAMPLES = {}

TRADE_STREAM_CLIENTS = []
ORDER_BOOK_STREAM_CLIENTS = []
EXECUTION_REPORT_CLIENTS = {}


class Handler(BaseHTTPRequestHandler):

    def _check_trading_window(self, delivery_start: int):
        now_ms = int(time.time() * 1000)

        OPEN_MS = 15 * 24 * 60 * 60 * 1000  # 15 days
        CLOSE_MS = 60 * 1000  # 1 min

        open_time = delivery_start - OPEN_MS
        close_time = delivery_start - CLOSE_MS

        if now_ms < open_time:
            self.send_response(425)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        if now_ms > close_time:
            self.send_response(451)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        return True

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

    def _broadcast_order_book_change(self, order: dict, change_type: str):
        if not ORDER_BOOK_STREAM_CLIENTS:
            return

        payload = encode_message({
            "order_id": str(order["order_id"]),
            "side": order["side"],
            "price": int(order["price"]),
            "quantity": int(order["quantity"]),
            "delivery_start": int(order["delivery_start"]),
            "delivery_end": int(order["delivery_end"]),
            "change_type": change_type,  # ADD, MODIFY, REMOVE
            "timestamp": int(time.time() * 1000),  # Current timestamp
        })

        frame = self._ws_build_binary_frame(payload)

        for sock in list(ORDER_BOOK_STREAM_CLIENTS):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    ORDER_BOOK_STREAM_CLIENTS.remove(sock)
                except ValueError:
                    pass

    def _ws_build_binary_frame(self, payload: bytes) -> bytes:
        fin_opcode = 0x82  # Final frame with binary message
        length = len(payload)

        if length < 126:
            header = bytes([fin_opcode, length])
        elif length < (1 << 16):
            header = bytes([fin_opcode, 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([fin_opcode, 127]) + length.to_bytes(8, "big")

        return header + payload

    def handle_order_book_stream(self):
        if self.command != "GET":
            self.send_response(405)
            self.end_headers()
            return

        key = self.headers.get("Sec-WebSocket-Key")
        upgrade = (self.headers.get("Upgrade") or "").lower()
        connection = (self.headers.get("Connection") or "").lower()

        if not key or "websocket" not in upgrade or "upgrade" not in connection:
            self.send_response(400)
            self.end_headers()
            return

        # Generate WebSocket accept response
        accept_seed = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        accept = base64.b64encode(
            hashlib.sha1(accept_seed.encode("utf-8")).digest()
        ).decode("utf-8")

        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

        self._is_websocket = True
        sock = self.request
        ORDER_BOOK_STREAM_CLIENTS.append(sock)

        try:
            while True:
                data = sock.recv(1024)
                if not data:
                    break
        except Exception:
            pass
        finally:
            try:
                ORDER_BOOK_STREAM_CLIENTS.remove(sock)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/v2/stream/order-book":
            self.handle_order_book_stream()

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # Handle registration, login, order submission, etc.
        if self.path == "/register":
            self.handle_register()
        elif self.path == "/login":
            self.handle_login()
        elif self.path == "/orders":
            self.handle_submit_order()
        elif self.path == "/v2/orders":
            self.handle_submit_order_v2()
        elif self.path == "/v2/bulk-operations":
            self.handle_bulk_operations()
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
        execution_type = (data.get("execution_type") or "GTC").strip() or "GTC"
        if execution_type not in ("GTC", "IOC", "FOK"):
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

        if not self._check_trading_window(delivery_start):
            return

        if not self._check_collateral_create(username, side, price, quantity):
            self.send_response(402)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        remaining = quantity
        filled_quantity = 0
        original_quantity = quantity

        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
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
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] >= price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if resting.get("owner") == username:
                self._send_no_content(412)
                return

        if execution_type == "FOK":
            total_possible = 0
            for resting in candidates:
                if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                    continue
                total_possible += resting["quantity"]
                if total_possible >= quantity:
                    break

            if total_possible < quantity:
                cancel_snapshot = {
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": quantity,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "CANCELLED",
                    "original_quantity": quantity,
                }
                self._broadcast_execution_report_for_order(cancel_snapshot)
                self._send_gbuf(200, {
                    "order_id": order_id,
                    "status": "CANCELLED",
                    "filled_quantity": 0,
                })
                return

        for resting in candidates:
            if remaining <= 0:
                break

            if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
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

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
                "delivery_start": delivery_start,
                "delivery_end": delivery_end,
                "source": "v2",
            }
            TRADES.append(trade)
            self._apply_trade_balances(buyer_id, seller_id, trade_price, trade_qty)
            self._broadcast_trade(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty

            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"
                self._broadcast_order_book_change(resting, "REMOVE")
            else:
                self._broadcast_order_book_change(resting, "MODIFY")

            self._broadcast_execution_report_for_order(resting)

        if execution_type == "GTC":
            if remaining > 0:
                status = "ACTIVE"
                new_order = {
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": remaining,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "ACTIVE",
                    "created_at": now_ms,
                    "original_quantity": original_quantity,
                }
                V2_ORDERS.append(new_order)
                self._broadcast_order_book_change(new_order, "ADD")
            else:
                status = "FILLED"
        elif execution_type == "IOC":
            status = "FILLED" if remaining <= 0 else "CANCELLED"
        else:
            status = "FILLED"

        order_snapshot = {
            "order_id": order_id,
            "side": side,
            "owner": username,
            "price": price,
            "quantity": remaining,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "status": status,
            "original_quantity": original_quantity,
        }
        self._broadcast_execution_report_for_order(order_snapshot)

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        })

    def handle_bulk_operations(self):
        try:
            body = self._read_body()
            data = decode_message(body)
        except Exception:
            return self._send_no_content(400)

        contracts = data.get("contracts")
        if not isinstance(contracts, list) or not contracts:
            return self._send_no_content(400)

        staged_operations = []

        for contract in contracts:
            try:
                ds = int(contract["delivery_start"])
                de = int(contract["delivery_end"])
            except Exception:
                return self._send_no_content(400)

            ops = contract.get("operations")
            if not isinstance(ops, list) or not ops:
                return self._send_no_content(400)

            HOUR_MS = 3600000
            if (ds % HOUR_MS) != 0 or (de % HOUR_MS) != 0:
                return self._send_no_content(400)
            if de <= ds or de - ds != HOUR_MS:
                return self._send_no_content(400)

            if not self._check_trading_window(ds):
                return

            for op in ops:
                optype = op.get("type")
                token = op.get("participant_token", "")
                username = TOKENS.get(token)
                if not username:
                    return self._send_no_content(401)

                if optype == "create":
                    result = self._bulk_sim_create(username, op, ds, de, staged_operations)
                elif optype == "modify":
                    result = self._bulk_sim_modify(username, op, ds, de, staged_operations)
                elif optype == "cancel":
                    result = self._bulk_sim_cancel(username, op, ds, de, staged_operations)
                else:
                    return self._send_no_content(400)

                if not result["ok"]:
                    return self._send_no_content(result["status"])

                staged_operations.append(result)

        for result in staged_operations:
            if result["action"] == "create":
                order_data = result["order"]
                if order_data is not None and result.get("status") == "ACTIVE":
                    V2_ORDERS.append(order_data)
                    self._broadcast_order_book_change(order_data, "ADD")

                for trade in result.get("trades", []):
                    TRADES.append(trade)
                    self._apply_trade_balances(
                        trade["buyer_id"],
                        trade["seller_id"],
                        trade["price"],
                        trade["quantity"]
                    )
                    self._broadcast_trade(trade)

            elif result["action"] == "modify":
                order_id = result["order_id"]
                target = next(o for o in V2_ORDERS if o["order_id"] == order_id)
                target["price"] = result["new_price"]
                target["quantity"] = result["new_quantity"]
                target["status"] = result["status"]
                if "created_at" in result:
                    target["created_at"] = result["created_at"]

                if target["status"] == "ACTIVE":
                    self._broadcast_order_book_change(target, "MODIFY")
                else:
                    self._broadcast_order_book_change(target, "REMOVE")

                for trade in result.get("trades", []):
                    TRADES.append(trade)
                    self._apply_trade_balances(
                        trade["buyer_id"],
                        trade["seller_id"],
                        trade["price"],
                        trade["quantity"]
                    )
                    self._broadcast_trade(trade)

            elif result["action"] == "cancel":
                order_id = result["order_id"]
                target = next(o for o in V2_ORDERS if o["order_id"] == order_id)
                target["status"] = "CANCELLED"
                target["quantity"] = 0
                self._broadcast_order_book_change(target, "REMOVE")

        results = []
        for result in staged_operations:
            entry = {
                "type": result["action"],
                "order_id": result["order_id"],
            }
            if "status" in result:
                entry["status"] = result["status"]
            results.append(entry)

        return self._send_gbuf(200, {"results": results})

    def handle_dna_submit(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()
        dna_sample = (data.get("dna_sample") or "").strip()

        if not username or not password or not dna_sample:
            self._send_no_content(400)
            return

        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        samples = DNA_SAMPLES.setdefault(username, [])
        if dna_sample not in samples:
            samples.append(dna_sample)

        self._send_no_content(204)

    def handle_dna_login(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        dna_sample = (data.get("dna_sample") or "").strip()

        if not username or not dna_sample:
            self._send_no_content(400)
            return

        if username not in USERS:
            self._send_no_content(401)
            return

        if username not in DNA_SAMPLES or not DNA_SAMPLES[username]:
            self._send_no_content(401)
            return

        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        matched = False
        for ref in DNA_SAMPLES[username]:
            if self._dna_matches(ref, dna_sample):
                matched = True
                break

        if not matched:
            self._send_no_content(401)
            return

        token = uuid.uuid4().hex
        TOKENS[token] = username

        self._send_gbuf(200, {"token": token})

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

    def _check_collateral_create(self, username: str, side: str, price: int, quantity: int) -> bool:
        coll = COLLATERAL.get(username)
        if coll is None:
            return True
        if not ((side == "buy" and price > 0) or (side == "sell" and price < 0)):
            return True
        base = self._compute_potential_balance(username)
        if side == "buy":
            delta = -price * quantity
        else:
            delta = price * quantity
        potential_after = base + delta
        return potential_after >= -coll

    def _check_collateral_modify(self, username: str, order_id: str, new_price: int, new_quantity: int) -> bool:
        coll = COLLATERAL.get(username)
        if coll is None:
            return True

        base = BALANCES.get(username, 0)
        side_for_target = None

        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            qty = int(o.get("quantity", 0))
            if qty <= 0:
                continue
            price = int(o["price"])
            side = o["side"]

            if o.get("order_id") == order_id:
                qty = new_quantity
                price = new_price
                side = o["side"]
                side_for_target = side

            if side == "buy":
                base -= price * qty
            else:
                base += price * qty

        if side_for_target is None:
            return True

        if not ((side_for_target == "buy" and new_price > 0) or (side_for_target == "sell" and new_price < 0)):
            return True

        return base >= -coll

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
        execution_type = (data.get("execution_type") or "GTC").strip() or "GTC"
        if execution_type not in ("GTC", "IOC", "FOK"):
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

        if not self._check_trading_window(delivery_start):
            return

        if not self._check_collateral_create(username, side, price, quantity):
            self.send_response(402)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        remaining = quantity
        filled_quantity = 0
        original_quantity = quantity

        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
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
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] >= price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if resting.get("owner") == username:
                self._send_no_content(412)
                return

        if execution_type == "FOK":
            total_possible = 0
            for resting in candidates:
                if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                    continue
                total_possible += resting["quantity"]
                if total_possible >= quantity:
                    break

            if total_possible < quantity:
                cancel_snapshot = {
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": quantity,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "CANCELLED",
                    "original_quantity": quantity,
                }
                self._broadcast_execution_report_for_order(cancel_snapshot)
                self._send_gbuf(200, {
                    "order_id": order_id,
                    "status": "CANCELLED",
                    "filled_quantity": 0,
                })
                return

        for resting in candidates:
            if remaining <= 0:
                break

            if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
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

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
                "delivery_start": delivery_start,
                "delivery_end": delivery_end,
                "source": "v2",
            }
            TRADES.append(trade)
            self._apply_trade_balances(buyer_id, seller_id, trade_price, trade_qty)
            self._broadcast_trade(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty

            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"
                self._broadcast_order_book_change(resting, "REMOVE")
            else:
                self._broadcast_order_book_change(resting, "MODIFY")

            self._broadcast_execution_report_for_order(resting)

        if execution_type == "GTC":
            if remaining > 0:
                status = "ACTIVE"
                new_order = {
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": remaining,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "ACTIVE",
                    "created_at": now_ms,
                    "original_quantity": original_quantity,
                }
                V2_ORDERS.append(new_order)
                self._broadcast_order_book_change(new_order, "ADD")
            else:
                status = "FILLED"
        elif execution_type == "IOC":
            status = "FILLED" if remaining <= 0 else "CANCELLED"
        else:
            status = "FILLED"

        order_snapshot = {
            "order_id": order_id,
            "side": side,
            "owner": username,
            "price": price,
            "quantity": remaining,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "status": status,
            "original_quantity": original_quantity,
        }
        self._broadcast_execution_report_for_order(order_snapshot)

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        })

    def handle_modify_order(self, order_id: str):
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

        if "price" not in data or "quantity" not in data:
            self._send_no_content(400)
            return

        try:
            new_price = int(data.get("price"))
            new_quantity = int(data.get("quantity"))
        except Exception:
            self._send_no_content(400)
            return

        if new_quantity <= 0:
            self._send_no_content(400)
            return

        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return

        if order.get("owner") != username:
            self._send_no_content(403)
            return

        side = order["side"]
        delivery_start = order["delivery_start"]
        delivery_end = order["delivery_end"]

        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "sell"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["order_id"] != order_id
                and o["price"] <= new_price
            ]
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["order_id"] != order_id
                and o["price"] >= new_price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if resting.get("owner") == username:
                self._send_no_content(412)
                return

        if not self._check_collateral_modify(username, order_id, new_price, new_quantity):
            self.send_response(402)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        old_price = order["price"]
        old_quantity = order["quantity"]

        orig = order.get("original_quantity", old_quantity)
        filled_so_far = orig - old_quantity
        order["original_quantity"] = filled_so_far + new_quantity

        order["price"] = new_price
        order["quantity"] = new_quantity

        now_ms = int(time.time() * 1000)
        if new_price != old_price or new_quantity > old_quantity:
            order["created_at"] = now_ms

        remaining = order["quantity"]
        filled_quantity = 0

        for resting in candidates:
            if remaining <= 0:
                break
            if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                continue

            if side == "buy" and new_price < resting["price"]:
                continue
            if side == "sell" and new_price > resting["price"]:
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

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
                "delivery_start": delivery_start,
                "delivery_end": delivery_end,
                "source": "v2",
            }
            TRADES.append(trade)
            self._apply_trade_balances(buyer_id, seller_id, trade_price, trade_qty)
            self._broadcast_trade(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty
            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"
                self._broadcast_order_book_change(resting, "REMOVE")
            else:
                self._broadcast_order_book_change(resting, "MODIFY")

            self._broadcast_execution_report_for_order(resting)

        order["quantity"] = remaining
        if remaining <= 0:
            order["quantity"] = 0
            order["status"] = "FILLED"

        if order["status"] == "ACTIVE":
            self._broadcast_order_book_change(order, "MODIFY")
        else:
            self._broadcast_order_book_change(order, "REMOVE")

        self._broadcast_execution_report_for_order(order)

        self._send_gbuf(200, {
            "order_id": order["order_id"],
            "status": order["status"],
            "filled_quantity": filled_quantity,
        })

    def handle_cancel_order(self, order_id: str):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return

        if order.get("owner") != username:
            self._send_no_content(403)
            return

        order["status"] = "CANCELLED"

        self._broadcast_order_book_change(order, "REMOVE")
        self._broadcast_execution_report_for_order(order)

        self._send_no_content(204)

    def handle_v2_order_book(self, parsed):
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

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        OPEN_MS = 15 * 24 * 60 * 60 * 1000
        CLOSE_MS = 60 * 1000
        now_ms = int(time.time() * 1000)

        open_time = delivery_start - OPEN_MS
        close_time = delivery_start - CLOSE_MS

        if not (open_time <= now_ms <= close_time):
            return self._send_gbuf(200, {"bids": [], "asks": []})

        bids = []
        asks = []

        for o in V2_ORDERS:
            if o.get("status") != "ACTIVE":
                continue
            if o["quantity"] <= 0:
                continue
            if o["delivery_start"] != delivery_start or o["delivery_end"] != delivery_end:
                continue

            entry = {
                "order_id": o["order_id"],
                "price": o["price"],
                "quantity": o["quantity"],
            }

            if o["side"] == "buy":
                bids.append((o, entry))
            else:
                asks.append((o, entry))

        bids.sort(key=lambda x: (-x[0]["price"], x[0].get("created_at", 0)))
        asks.sort(key=lambda x: (x[0]["price"], x[0].get("created_at", 0)))

        bids_payload = [e for _, e in bids]
        asks_payload = [e for _, e in asks]

        self._send_gbuf(200, {"bids": bids_payload, "asks": asks_payload})

    def handle_my_orders(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        my_active = [
            o for o in V2_ORDERS
            if o.get("owner") == username
            and o.get("status") == "ACTIVE"
            and o["quantity"] > 0
        ]

        my_active.sort(key=lambda o: o.get("created_at", 0), reverse=True)

        orders_payload = []
        for o in my_active:
            orders_payload.append({
                "order_id": o["order_id"],
                "side": o["side"],
                "price": o["price"],
                "quantity": o["quantity"],
                "delivery_start": o["delivery_start"],
                "delivery_end": o["delivery_end"],
                "timestamp": o.get("created_at", 0),
            })

        self._send_gbuf(200, {"orders": orders_payload})

    def handle_my_trades(self, parsed):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

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

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        my_trades = []
        for t in TRADES:
            if t.get("delivery_start") != delivery_start or t.get("delivery_end") != delivery_end:
                continue
            buyer = t["buyer_id"]
            seller = t["seller_id"]
            if buyer != username and seller != username:
                continue

            if buyer == username:
                side = "buy"
                counterparty = seller
            else:
                side = "sell"
                counterparty = buyer

            my_trades.append({
                "trade_id": t["trade_id"],
                "side": side,
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "counterparty": counterparty,
                "delivery_start": int(t["delivery_start"]),
                "delivery_end": int(t["delivery_end"]),
                "timestamp": int(t["timestamp"]),
            })

        my_trades.sort(key=lambda tr: tr["timestamp"], reverse=True)

        self._send_gbuf(200, {"trades": my_trades})

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

    def handle_v2_trades(self, parsed):
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

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        v2_trades = [
            t for t in TRADES
            if t.get("source") == "v2"
            and t.get("delivery_start") == delivery_start
            and t.get("delivery_end") == delivery_end
        ]

        v2_trades.sort(key=lambda t: int(t["timestamp"]), reverse=True)

        trades_payload = []
        for t in v2_trades:
            trades_payload.append({
                "trade_id": str(t["trade_id"]),
                "buyer_id": str(t["buyer_id"]),
                "seller_id": str(t["seller_id"]),
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "delivery_start": int(t["delivery_start"]),
                "delivery_end": int(t["delivery_end"]),
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

        trade_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        trade = {
            "trade_id": trade_id,
            "buyer_id": username,
            "seller_id": order["seller_id"],
            "price": int(order["price"]),
            "quantity": int(order["quantity"]),
            "timestamp": now_ms,
            "delivery_start": int(order["delivery_start"]),
            "delivery_end": int(order["delivery_end"]),
            "source": "v1",
        }
        TRADES.append(trade)

        self._apply_trade_balances(username, order["seller_id"], int(order["price"]), int(order["quantity"]))

        self._send_gbuf(200, {"trade_id": trade_id})

    def handle_set_collateral(self, username: str):
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            self._send_no_content(401)
            return
        token = auth[7:].strip()
        if token != "password123":
            self._send_no_content(401)
            return

        if username not in USERS:
            self._send_no_content(404)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        if "collateral" not in data:
            self._send_no_content(400)
            return

        try:
            collateral_value = int(data.get("collateral"))
        except Exception:
            self._send_no_content(400)
            return

        COLLATERAL[username] = collateral_value
        self._send_no_content(204)

    def handle_get_balance(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        if username not in USERS:
            self._send_no_content(404)
            return

        balance = BALANCES.get(username, 0)
        potential = self._compute_potential_balance(username)
        collateral = COLLATERAL.get(username)
        if collateral is None:
            collateral = 9223372036854775807

        self._send_gbuf(200, {
            "balance": int(balance),
            "potential_balance": int(potential),
            "collateral": int(collateral),
        })


def run():
    server = ThreadingHTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
