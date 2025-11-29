from http.server import BaseHTTPRequestHandler, HTTPServer
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

# New: balances + collateral
BALANCES = {}     # username -> int
COLLATERAL = {}   # username -> collateral limit (None = unlimited)

# New: DNA samples
# username -> list of registered DNA strings (normalized as str)
DNA_SAMPLES = {}

# WebSocket trade stream clients (raw sockets)
TRADE_STREAM_CLIENTS = []


class Handler(BaseHTTPRequestHandler):
    # ---------- helpers ----------

    def _check_trading_window(self, delivery_start: int):
        """
        Returns:
            True  - inside trading window
            False - already responded with 425/451
        """

        now_ms = int(time.time() * 1000)

        OPEN_MS = 15 * 24 * 60 * 60 * 1000     # 15 days
        CLOSE_MS = 60 * 1000                   # 1 min

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

    # ----- balances / collateral helpers -----

    def _apply_trade_balances(self, buyer_id: str, seller_id: str, price: int, quantity: int):
        """
        Buyer pays price * quantity, seller receives price * quantity.
        Negative prices will naturally invert the effect.
        """
        amount = int(price) * int(quantity)
        BALANCES[buyer_id] = BALANCES.get(buyer_id, 0) - amount
        BALANCES[seller_id] = BALANCES.get(seller_id, 0) + amount

    def _compute_potential_balance(self, username: str) -> int:
        """
        Potential balance = current balance + effect if all ACTIVE orders fill.

        Includes:
          - V1 ORDERS (always sells at positive price)
          - V2_ORDERS (buys/sells with side)
        """
        balance = BALANCES.get(username, 0)

        # --- V1 orders: always sells made by seller_id ---
        for o in ORDERS:
            if not o.get("active", True):
                continue
            if o.get("seller_id") != username:
                continue
            try:
                qty = int(o.get("quantity", 0))
                price = int(o.get("price", 0))
            except Exception:
                continue
            if qty <= 0:
                continue
            # V1 is always sell at positive price -> user receives price * qty
            balance += price * qty

        # --- V2 orders: buys and sells ---
        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            try:
                qty = int(o.get("quantity", 0))
                price = int(o.get("price", 0))
            except Exception:
                continue
            if qty <= 0:
                continue

            side = o.get("side")
            if side == "buy":
                # Buy: user pays price * qty
                balance -= price * qty
            elif side == "sell":
                # Sell: user receives price * qty
                balance += price * qty

        return balance

    # ---------- DNA helpers ----------

    def _validate_dna_sample(self, dna) -> bool:
        """
        Accepts str or bytes.
        Only C/G/A/T, length must be multiple of 3.
        """
        if not dna:
            return False

        if isinstance(dna, bytes):
            n = len(dna)
            if n % 3 != 0:
                return False
            allowed = {ord("C"), ord("G"), ord("A"), ord("T")}
            for b in dna:
                if b not in allowed:
                    return False
            return True

        if isinstance(dna, str):
            if len(dna) % 3 != 0:
                return False
            for ch in dna:
                if ch not in ("C", "G", "A", "T"):
                    return False
            return True

        return False

    def _split_codons(self, dna_str: str):
        # dna_str is guaranteed to be a str, len multiple of 3
        return [dna_str[i:i+3] for i in range(0, len(dna_str), 3)]

    def _codon_edit_distance_bounded(self, ref_codons, sample_codons, max_diff: int) -> int:
        """
        Levenshtein distance on codons with a hard cap max_diff.
        Returns a value > max_diff if distance exceeds max_diff.
        Uses a banded DP of width ~2*max_diff+1.
        """
        n = len(ref_codons)
        m = len(sample_codons)

        if max_diff < 0:
            return max_diff + 1

        # At least this many insert/delete ops
        if abs(n - m) > max_diff:
            return max_diff + 1

        if n == 0:
            return m
        if m == 0:
            return n

        prev = {}
        for j in range(0, min(m, max_diff) + 1):
            prev[j] = j

        for i in range(1, n + 1):
            j_min = max(0, i - max_diff)
            j_max = min(m, i + max_diff)
            curr = {}

            for j in range(j_min, j_max + 1):
                if j > j_min:
                    ins = curr[j - 1] + 1
                else:
                    ins = max_diff + 1

                dele = prev.get(j, max_diff + 1) + 1

                if j - 1 in prev:
                    sub_cost = 0 if ref_codons[i - 1] == sample_codons[j - 1] else 1
                    sub = prev[j - 1] + sub_cost
                else:
                    sub = max_diff + 1

                curr[j] = min(ins, dele, sub)

            if min(curr.values()) > max_diff:
                return max_diff + 1

            prev = curr

        dist = prev.get(m, max_diff + 1)
        return dist

    def _dna_matches(self, reference, submitted) -> bool:
        """
        reference: stored as str (from DNA_SAMPLES)
        submitted: str or bytes (from login)
        """
        # Normalize to str
        if isinstance(reference, bytes):
            try:
                reference = reference.decode("ascii")
            except Exception:
                return False

        if isinstance(submitted, bytes):
            try:
                submitted = submitted.decode("ascii")
            except Exception:
                return False

        if not isinstance(reference, str) or not isinstance(submitted, str):
            return False

        # Basic structural checks
        if not reference or not submitted:
            return False
        if len(reference) % 3 != 0 or len(submitted) % 3 != 0:
            return False

        ref_codons_count = len(reference) // 3
        allowed_diff = ref_codons_count // 100000  # floor(Ca/100000)

        # Fast path: allowed_diff == 0 -> exact match only
        if allowed_diff == 0:
            return reference == submitted

        # For non-zero allowed_diff, use banded DP on codons
        ref_codons = self._split_codons(reference)
        sub_codons = self._split_codons(submitted)

        max_diff = allowed_diff
        dist = self._codon_edit_distance_bounded(ref_codons, sub_codons, max_diff)
        return dist <= allowed_diff

    # ---------- WebSocket helpers ----------

    def _ws_build_binary_frame(self, payload: bytes) -> bytes:
        # Server-to-client frames are not masked.
        fin_opcode = 0x82  # FIN=1, opcode=2 (binary)
        length = len(payload)
        if length < 126:
            header = bytes([fin_opcode, length])
        elif length < (1 << 16):
            header = bytes([fin_opcode, 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([fin_opcode, 127]) + length.to_bytes(8, "big")
        return header + payload

    def _broadcast_trade(self, trade: dict):
        # Only V2 trades should be broadcast; caller ensures this
        if not TRADE_STREAM_CLIENTS:
            return
        payload = encode_message({
            "trade_id": str(trade["trade_id"]),
            "buyer_id": str(trade["buyer_id"]),
            "seller_id": str(trade["seller_id"]),
            "price": int(trade["price"]),
            "quantity": int(trade["quantity"]),
            "delivery_start": int(trade["delivery_start"]),
            "delivery_end": int(trade["delivery_end"]),
            "timestamp": int(trade["timestamp"]),
        })
        frame = self._ws_build_binary_frame(payload)

        # Send to all connected clients, drop broken ones
        for sock in list(TRADE_STREAM_CLIENTS):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    TRADE_STREAM_CLIENTS.remove(sock)
                except ValueError:
                    pass

    # ---------- bulk ops simulation helpers (unchanged from your version) ----------

    def _bulk_sim_create(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        """Simulate create operation, return validation result with trades if any"""
        try:
            side = (op.get("side") or "").strip()
            price = int(op.get("price"))
            quantity = int(op.get("quantity"))
            execution_type = (op.get("execution_type") or "GTC").strip() or "GTC"
        except:
            return {"ok": False, "status": 400}

        if side not in ("buy", "sell"):
            return {"ok": False, "status": 400}
        if quantity <= 0:
            return {"ok": False, "status": 400}
        if execution_type not in ("GTC", "IOC", "FOK"):
            return {"ok": False, "status": 400}

        # Check collateral in simulated state
        if not self._check_collateral_in_sim_state(username, side, price, quantity, staged_ops):
            return {"ok": False, "status": 402}

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        # Build hypothetical order book including staged creates/modifies
        sim_book = self._build_sim_order_book(ds, de, staged_ops)

        # Find matching orders
        if side == "buy":
            candidates = [
                o for o in sim_book
                if o["side"] == "sell"
                and o["price"] <= price
            ]
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in sim_book
                if o["side"] == "buy"
                and o["price"] >= price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        # Self-match check
        for c in candidates:
            if c.get("owner") == username:
                return {"ok": False, "status": 412}

        # Simulate matching
        remaining = quantity
        filled_quantity = 0
        trades = []

        for resting in candidates:
            if remaining <= 0:
                break
            if resting["quantity"] <= 0:
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

            trade = {
                "trade_id": uuid.uuid4().hex,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": resting["price"],
                "quantity": trade_qty,
                "timestamp": int(time.time() * 1000),
                "delivery_start": ds,
                "delivery_end": de,
                "source": "v2",
            }
            trades.append(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty
            resting["quantity"] -= trade_qty

        # FOK check
        if execution_type == "FOK" and remaining > 0:
            return {
                "ok": True,
                "action": "create",
                "order_id": order_id,
                "status": "CANCELLED",
                "order": None,
                "trades": [],
            }

        # Determine final status
        if execution_type == "GTC":
            if remaining > 0:
                status = "ACTIVE"
                order_data = {
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": remaining,
                    "delivery_start": ds,
                    "delivery_end": de,
                    "status": "ACTIVE",
                    "created_at": now_ms,
                }
            else:
                status = "FILLED"
                order_data = None
        elif execution_type == "IOC":
            status = "FILLED" if remaining <= 0 else "CANCELLED"
            order_data = None
        else:  # FOK
            status = "FILLED"
            order_data = None

        return {
            "ok": True,
            "action": "create",
            "order_id": order_id,
            "status": status,
            "order": order_data,
            "trades": trades,
        }

    def _bulk_sim_modify(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        """Simulate modify operation, return validation result"""
        try:
            order_id = op.get("order_id", "").strip()
            new_price = int(op.get("price"))
            new_quantity = int(op.get("quantity"))
        except:
            return {"ok": False, "status": 400}

        if not order_id:
            return {"ok": False, "status": 400}
        if new_quantity <= 0:
            return {"ok": False, "status": 400}

        # Find order in real book or staged creates
        order = self._find_order_in_sim(order_id, ds, de, staged_ops)

        if not order:
            return {"ok": False, "status": 404}

        if order.get("owner") != username:
            return {"ok": False, "status": 403}

        # Check if already cancelled in staged ops
        for sop in staged_ops:
            if sop.get("action") == "cancel" and sop.get("order_id") == order_id:
                return {"ok": False, "status": 404}

        side = order["side"]

        # Build sim book
        sim_book = self._build_sim_order_book(ds, de, staged_ops, exclude_order_id=order_id)

        # Find matching orders with new price
        if side == "buy":
            candidates = [
                o for o in sim_book
                if o["side"] == "sell"
                and o["price"] <= new_price
            ]
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in sim_book
                if o["side"] == "buy"
                and o["price"] >= new_price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        # Self-match check
        for c in candidates:
            if c.get("owner") == username:
                return {"ok": False, "status": 412}

        # Collateral check with new values
        if not self._check_collateral_modify_in_sim(username, order_id, new_price, new_quantity, staged_ops):
            return {"ok": False, "status": 402}

        # Simulate matching
        remaining = new_quantity
        filled_quantity = 0
        trades = []
        now_ms = int(time.time() * 1000)

        old_price = order["price"]
        old_quantity = order["quantity"]

        for resting in candidates:
            if remaining <= 0:
                break
            if resting["quantity"] <= 0:
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

            trade = {
                "trade_id": uuid.uuid4().hex,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": resting["price"],
                "quantity": trade_qty,
                "timestamp": int(time.time() * 1000),
                "delivery_start": ds,
                "delivery_end": de,
                "source": "v2",
            }
            trades.append(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty
            resting["quantity"] -= trade_qty

        status = "FILLED" if remaining <= 0 else "ACTIVE"

        result = {
            "ok": True,
            "action": "modify",
            "order_id": order_id,
            "new_price": new_price,
            "new_quantity": remaining,
            "status": status,
            "trades": trades,
        }

        if new_price != old_price or new_quantity > old_quantity:
            result["created_at"] = now_ms

        return result

    def _bulk_sim_cancel(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        """Simulate cancel operation, return validation result"""
        try:
            order_id = op.get("order_id", "").strip()
        except:
            return {"ok": False, "status": 400}

        if not order_id:
            return {"ok": False, "status": 400}

        # Find order
        order = self._find_order_in_sim(order_id, ds, de, staged_ops)

        if not order:
            return {"ok": False, "status": 404}

        if order.get("owner") != username:
            return {"ok": False, "status": 403}

        # Check if already cancelled in staged ops
        for sop in staged_ops:
            if sop.get("action") == "cancel" and sop.get("order_id") == order_id:
                return {"ok": False, "status": 404}

        return {
            "ok": True,
            "action": "cancel",
            "order_id": order_id,
        }

    def _build_sim_order_book(self, ds: int, de: int, staged_ops: list, exclude_order_id: str = None):
        """Build hypothetical order book including real orders + staged creates/modifies"""
        sim_book = []

        # Start with real active orders
        for o in V2_ORDERS:
            if o.get("status") != "ACTIVE":
                continue
            if o["quantity"] <= 0:
                continue
            if o["delivery_start"] != ds or o["delivery_end"] != de:
                continue
            if exclude_order_id and o["order_id"] == exclude_order_id:
                continue

            # Check if this order was modified or cancelled in staged ops
            was_modified = False
            was_cancelled = False
            modified_data = None

            for sop in staged_ops:
                if sop.get("order_id") == o["order_id"]:
                    if sop["action"] == "cancel":
                        was_cancelled = True
                        break
                    elif sop["action"] == "modify":
                        was_modified = True
                        modified_data = sop
                        break

            if was_cancelled:
                continue

            if was_modified:
                sim_book.append({
                    "order_id": o["order_id"],
                    "side": o["side"],
                    "owner": o["owner"],
                    "price": modified_data["new_price"],
                    "quantity": modified_data["new_quantity"],
                    "created_at": modified_data.get("created_at", o.get("created_at", 0)),
                })
            else:
                sim_book.append({
                    "order_id": o["order_id"],
                    "side": o["side"],
                    "owner": o["owner"],
                    "price": o["price"],
                    "quantity": o["quantity"],
                    "created_at": o.get("created_at", 0),
                })

        # Add staged creates that are ACTIVE
        for sop in staged_ops:
            if sop["action"] == "create" and sop.get("order"):
                order_data = sop["order"]
                sim_book.append({
                    "order_id": order_data["order_id"],
                    "side": order_data["side"],
                    "owner": order_data["owner"],
                    "price": order_data["price"],
                    "quantity": order_data["quantity"],
                    "created_at": order_data.get("created_at", 0),
                })

        return sim_book

    def _find_order_in_sim(self, order_id: str, ds: int, de: int, staged_ops: list):
        """Find order in real book or staged creates"""
        # Check staged creates first
        for sop in staged_ops:
            if sop["action"] == "create" and sop.get("order"):
                if sop["order"]["order_id"] == order_id:
                    return sop["order"]

        # Check real orders
        for o in V2_ORDERS:
            if o["order_id"] == order_id:
                if o.get("status") != "ACTIVE" or o["quantity"] <= 0:
                    return None
                if o["delivery_start"] != ds or o["delivery_end"] != de:
                    return None
                return o

        return None

    def _check_collateral_in_sim_state(self, username: str, side: str, price: int, quantity: int, staged_ops: list):
        """Check collateral considering staged operations"""
        coll = COLLATERAL.get(username)
        if coll is None:
            return True

        if not ((side == "buy" and price > 0) or (side == "sell" and price < 0)):
            return True

        # Compute balance including staged trades
        balance = BALANCES.get(username, 0)

        for sop in staged_ops:
            for trade in sop.get("trades", []):
                buyer = trade["buyer_id"]
                seller = trade["seller_id"]
                amount = trade["price"] * trade["quantity"]
                if buyer == username:
                    balance -= amount
                elif seller == username:
                    balance += amount

        # Include existing active orders
        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            qty = int(o.get("quantity", 0))
            if qty <= 0:
                continue

            # Check if this order was modified/cancelled in staged ops
            skip = False
            for sop in staged_ops:
                if sop.get("order_id") == o["order_id"]:
                    if sop["action"] in ("modify", "cancel"):
                        skip = True
                        break

            if skip:
                continue

            p = int(o["price"])
            s = o["side"]
            if s == "buy":
                balance -= p * qty
            else:
                balance += p * qty

        # Include staged creates/modifies
        for sop in staged_ops:
            if sop["action"] == "create" and sop.get("order"):
                od = sop["order"]
                if od["owner"] == username:
                    qty = od["quantity"]
                    p = od["price"]
                    s = od["side"]
                    if s == "buy":
                        balance -= p * qty
                    else:
                        balance += p * qty
            elif sop["action"] == "modify":
                # Find original order
                for o in V2_ORDERS:
                    if o["order_id"] == sop["order_id"] and o["owner"] == username:
                        qty = sop["new_quantity"]
                        p = sop["new_price"]
                        s = o["side"]
                        if s == "buy":
                            balance -= p * qty
                        else:
                            balance += p * qty
                        break

        # Now add this new order's effect
        if side == "buy":
            balance -= price * quantity
        else:
            balance += price * quantity

        return balance >= -coll

    def _check_collateral_modify_in_sim(self, username: str, order_id: str, new_price: int, new_quantity: int, staged_ops: list):
        """Check collateral for modify in sim state"""
        coll = COLLATERAL.get(username)
        if coll is None:
            return True

        # Find the order being modified
        target_order = None
        for o in V2_ORDERS:
            if o["order_id"] == order_id and o["owner"] == username:
                target_order = o
                break

        if not target_order:
            # Maybe it's a staged create
            for sop in staged_ops:
                if sop["action"] == "create" and sop.get("order"):
                    if sop["order"]["order_id"] == order_id:
                        target_order = sop["order"]
                        break

        if not target_order:
            return True

        side = target_order["side"]
        if not ((side == "buy" and new_price > 0) or (side == "sell" and new_price < 0)):
            return True

        # Compute balance with all staged effects except this modify
        balance = BALANCES.get(username, 0)

        for sop in staged_ops:
            for trade in sop.get("trades", []):
                buyer = trade["buyer_id"]
                seller = trade["seller_id"]
                amount = trade["price"] * trade["quantity"]
                if buyer == username:
                    balance -= amount
                elif seller == username:
                    balance += amount

        # Include existing orders
        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            qty = int(o.get("quantity", 0))
            if qty <= 0:
                continue

            if o["order_id"] == order_id:
                qty = new_quantity
                p = new_price
            else:
                # Check if modified/cancelled
                skip = False
                for sop in staged_ops:
                    if sop.get("order_id") == o["order_id"]:
                        if sop["action"] in ("modify", "cancel"):
                            skip = True
                            break
                if skip:
                    continue
                p = int(o["price"])

            s = o["side"]
            if s == "buy":
                balance -= p * qty
            else:
                balance += p * qty

        # Include staged creates (excluding this one)
        for sop in staged_ops:
            if sop["action"] == "create" and sop.get("order"):
                od = sop["order"]
                if od["owner"] == username and od["order_id"] != order_id:
                    qty = od["quantity"]
                    p = od["price"]
                    s = od["side"]
                    if s == "buy":
                        balance -= p * qty
                    else:
                        balance += p * qty

        return balance >= -coll

    # ---------- HTTP methods ----------

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

        elif parsed.path == "/v2/orders":
            self.handle_v2_order_book(parsed)

        elif parsed.path == "/v2/my-orders":
            self.handle_my_orders()

        elif parsed.path == "/v2/my-trades":
            self.handle_my_trades(parsed)

        elif parsed.path == "/balance":
            self.handle_get_balance()

        elif parsed.path == "/v2/trades":
            self.handle_v2_trades(parsed)

        elif parsed.path == "/v2/stream/trades":
            self.handle_trades_stream()

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
        elif self.path == "/v2/bulk-operations":
            self.handle_bulk_operations()
        elif self.path == "/dna-submit":
            self.handle_dna_submit()
        elif self.path == "/dna-login":
            self.handle_dna_login()
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path == "/user/password":
            self.handle_change_password()
        elif parsed.path.startswith("/v2/orders/"):
            order_id = parsed.path.split("/")[-1]
            self.handle_modify_order(order_id)
        elif parsed.path.startswith("/collateral/"):
            username = parsed.path.split("/")[-1]
            self.handle_set_collateral(username)
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/v2/orders/"):
            order_id = parsed.path.split("/")[-1]
            self.handle_cancel_order(order_id)
        else:
            self.send_response(404)
            self.end_headers()

    # Override finish so WebSocket connections stay open
    def finish(self):
        try:
            if not self.wfile.closed:
                self.wfile.flush()
        except Exception:
            pass
        if getattr(self, "_is_websocket", False):
            return
        try:
            self.wfile.close()
        except Exception:
            pass
        try:
            self.rfile.close()
        except Exception:
            pass

    # ---------- auth & users ----------

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
            tokens_to_delete = [t for t, u in list(TOKENS.items()) if u == username]
            for t in tokens_to_delete:
                del TOKENS[t]
        except Exception:
            self._send_no_content(500)
            return

        self._send_no_content(204)

    # ---------- bulk operations endpoint ----------

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
            except:
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

        # ---------- commit ----------
        for result in staged_operations:
            if result["action"] == "create":
                order_data = result["order"]
                if order_data:
                    V2_ORDERS.append(order_data)

                for trade in result.get("trades", []):
                    TRADES.append(trade)
                    self._apply_trade_balances(
                        trade["buyer_id"],
                        trade["seller_id"],
                        trade["price"],
                        trade["quantity"],
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

                for trade in result.get("trades", []):
                    TRADES.append(trade)
                    self._apply_trade_balances(
                        trade["buyer_id"],
                        trade["seller_id"],
                        trade["price"],
                        trade["quantity"],
                    )
                    self._broadcast_trade(trade)

            elif result["action"] == "cancel":
                order_id = result["order_id"]
                target = next(o for o in V2_ORDERS if o["order_id"] == order_id)
                target["status"] = "CANCELLED"
                target["quantity"] = 0

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

    # ---------- DNA endpoints ----------

    def handle_dna_submit(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()
        dna_raw = data.get("dna_sample", None)

        if not username or not password or dna_raw is None:
            self._send_no_content(400)
            return

        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        # dna_raw may be bytes (v2) or str (v1)
        if isinstance(dna_raw, bytes):
            dna_sample = dna_raw  # keep bytes for validation
        elif isinstance(dna_raw, str):
            dna_sample = dna_raw.strip()
        else:
            self._send_no_content(400)
            return

        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        # Normalize to str for storage
        if isinstance(dna_sample, bytes):
            try:
                dna_store = dna_sample.decode("ascii")
            except Exception:
                self._send_no_content(400)
                return
        else:
            dna_store = dna_sample

        samples = DNA_SAMPLES.setdefault(username, [])
        if dna_store not in samples:
            samples.append(dna_store)

        self._send_no_content(204)

    def handle_dna_login(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        dna_raw = data.get("dna_sample", None)

        if not username or dna_raw is None:
            self._send_no_content(400)
            return

        if username not in USERS:
            self._send_no_content(401)
            return

        if username not in DNA_SAMPLES or not DNA_SAMPLES[username]:
            self._send_no_content(401)
            return

        if isinstance(dna_raw, bytes):
            dna_sample = dna_raw
        elif isinstance(dna_raw, str):
            dna_sample = dna_raw.strip()
        else:
            self._send_no_content(400)
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

    # ---------- V1 orders & trades ----------

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

    # ---------- collateral checks ----------

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

    # ---------- V2 submit (matching engine + IOC/FOK) ----------

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

        if execution_type == "GTC":
            if remaining > 0:
                status = "ACTIVE"
                V2_ORDERS.append({
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": remaining,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "ACTIVE",
                    "created_at": now_ms,
                })
            else:
                status = "FILLED"
        elif execution_type == "IOC":
            status = "FILLED" if remaining <= 0 else "CANCELLED"
        else:
            status = "FILLED"

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        })

    # ---------- V2 modify / cancel ----------

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

        order["quantity"] = remaining
        if remaining <= 0:
            order["quantity"] = 0
            order["status"] = "FILLED"

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
        order["quantity"] = 0

        self._send_no_content(204)

    # ---------- V2 order book / my-orders / my-trades ----------

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

    # ---------- public trades ----------

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

    # ---------- public V2-only trades for a contract ----------

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

    # ---------- V1 take order ----------

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

    # ---------- collateral endpoints ----------

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

    # ---------- WebSocket trade stream endpoint ----------

    def handle_trades_stream(self):
        upgrade = (self.headers.get("Upgrade") or "").lower()
        connection = (self.headers.get("Connection") or "").lower()
        key = self.headers.get("Sec-WebSocket-Key")

        if upgrade != "websocket" or "upgrade" not in connection or not key:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        accept_src = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
        accept = base64.b64encode(hashlib.sha1(accept_src).digest()).decode("ascii")

        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

        self._is_websocket = True
        TRADE_STREAM_CLIENTS.append(self.request)
        # We don't read frames; stream is server -> client only.


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
