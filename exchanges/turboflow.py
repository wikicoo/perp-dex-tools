"""
TurboFlow exchange client implementation.

TurboFlow currently exposes order/account streams and latest-price ticker data,
but no public order book / BBO feed was available during integration. To keep
TradingBot compatible with the existing exchange interface, this client derives
"pseudo BBO" values from the latest known price and the pair tick size.
"""

import asyncio
import hashlib
import hmac
import json
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
import nacl.signing
import websockets

from .base import BaseExchangeClient, OrderInfo, OrderResult, query_retry
from helpers.logger import TradingLogger


class TurboFlowClient(BaseExchangeClient):
    """TurboFlow exchange client implementation."""

    ORDER_STATUS_MAP = {
        "Pending": "OPEN",
        "Finished": "FILLED",
        "Filled": "FILLED",
        "Cancelled": "CANCELED",
        "Rejected": "CANCELED",
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        self.api_key = os.getenv("TURBOFLOW_API_KEY")
        self.api_secret = os.getenv("TURBOFLOW_API_SECRET")
        self.base_url = os.getenv("TURBOFLOW_BASE_URL", "https://api.turboflow.xyz").rstrip("/")
        self.ws_url = os.getenv("TURBOFLOW_WS_URL", "wss://api.turboflow.xyz/realtime")
        self.lang = os.getenv("TURBOFLOW_LANG", "en")

        self.logger = TradingLogger(exchange="turboflow", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        self.session: Optional[aiohttp.ClientSession] = None
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop = asyncio.Event()

        self.latest_price: Decimal = Decimal("0")
        self.pair_id: Optional[str] = None
        self.pool_id: Optional[int] = None
        self.coin_code: Optional[str] = None
        self.price_decimals: Optional[int] = None
        self._position_cache: Dict[str, Dict[str, Any]] = {}
        self.open_vol_buffer = Decimal(os.getenv("TURBOFLOW_OPEN_VOL_BUFFER", "0.0006"))

    def _validate_config(self) -> None:
        required_env_vars = ["TURBOFLOW_API_KEY", "TURBOFLOW_API_SECRET"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    def get_exchange_name(self) -> str:
        return "turboflow"

    def setup_order_update_handler(self, handler) -> None:
        self._order_update_handler = handler

    async def connect(self) -> None:
        self._ws_stop.clear()
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_ws())

        await asyncio.sleep(1)

    async def disconnect(self) -> None:
        self._ws_stop.set()

        if self._ws_task and not self._ws_task.done():
            try:
                await self._ws_task
            except Exception as exc:
                self.logger.log(f"Error stopping TurboFlow websocket: {exc}", "ERROR")

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None

    async def _run_ws(self) -> None:
        backoff = 1
        while not self._ws_stop.is_set():
            try:
                timestamp = int(time.time())
                signature = self._sign_get("/realtime", timestamp)
                ws_url = (
                    f"{self.ws_url}?API-KEY={self.api_key}&SIGN={signature}"
                    f"&TIMESTAMP={timestamp}&PLATFORM=web"
                )

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    self._ws = ws
                    backoff = 1
                    await self._subscribe_topics(ws)
                    self.logger.log("TurboFlow websocket connected", "INFO")

                    async for raw in ws:
                        if self._ws_stop.is_set():
                            break
                        await self._handle_ws_message(raw)
            except Exception as exc:
                if not self._ws_stop.is_set():
                    self.logger.log(f"TurboFlow websocket error: {exc}", "ERROR")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
            finally:
                self._ws = None

    async def _subscribe_topics(self, ws) -> None:
        topics = ["dex_trade", "dex_trade_fill", "dex_position"]
        if self.config.contract_id:
            topics.append(f"dex_ticker.{self.config.contract_id}")

        for topic in topics:
            await ws.send(json.dumps({"action": "subscribe", "args": [topic]}))

    async def _handle_ws_message(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return

        group = message.get("group")
        data = message.get("data")
        if not group or data is None:
            return

        if group.startswith("dex_ticker"):
            price = data.get("p1")
            if price is not None:
                self.latest_price = Decimal(str(price))
            return

        if group == "dex_position":
            if str(data.get("pair_id")) == str(self.config.contract_id):
                position_id = str(data.get("id", ""))
                if position_id:
                    self._position_cache[position_id] = data
            return

        if group == "dex_trade_fill":
            if str(data.get("pair_id")) == str(self.config.contract_id):
                position_id = str(data.get("position_id", ""))
                if position_id:
                    self._position_cache.setdefault(position_id, {})["id"] = position_id
            return

        if group != "dex_trade":
            return

        if str(data.get("pair_id")) != str(self.config.contract_id):
            return

        status = self._map_order_status(data.get("order_status"), data.get("done_size"))
        side = self._map_order_side(data.get("order_way"))

        if status and self._order_update_handler:
            self._order_update_handler({
                "order_id": str(data.get("id")),
                "side": side,
                "order_type": "OPEN" if str(data.get("position_id", "0")) in ("", "0") else (
                    "CLOSE" if side == self.config.close_order_side else "OPEN"
                ),
                "status": status,
                "size": data.get("size", "0"),
                "price": data.get("price", "0"),
                "contract_id": str(data.get("pair_id")),
                "filled_size": data.get("done_size", "0"),
            })

    def _map_order_status(self, raw_status: Any, done_size: Any = None) -> str:
        status = self.ORDER_STATUS_MAP.get(str(raw_status), str(raw_status or "").upper())
        if status == "OPEN":
            try:
                if Decimal(str(done_size or "0")) > 0:
                    return "PARTIALLY_FILLED"
            except Exception:
                pass
        return status

    def _map_order_side(self, order_way: Any) -> str:
        order_way = int(order_way)
        if order_way in (1, 2):
            return "buy"
        if order_way in (3, 4):
            return "sell"
        raise ValueError(f"Unknown TurboFlow order_way: {order_way}")

    def _build_headers(self, path: str, method: str, body: Optional[str] = None) -> Dict[str, str]:
        timestamp = int(time.time())
        if method.upper() == "POST":
            signature = self._sign_post(path, timestamp, body or "")
        else:
            signature = self._sign_get(path, timestamp)

        return {
            "API-KEY": self.api_key,
            "SIGN": signature,
            "TIMESTAMP": str(timestamp),
            "LANG": self.lang,
        }

    def _sign_get(self, path: str, timestamp: int) -> str:
        payload = f"method=GET&path={path}&timestamp={timestamp}&access-key={self.api_key}"
        return self._sign_payload(payload)

    def _sign_post(self, path: str, timestamp: int, body: str) -> str:
        payload = (
            f"method=POST&path={path}&timestamp={timestamp}&access-key={self.api_key}&body={body}"
        )
        return self._sign_payload(payload)

    def _sign_payload(self, payload: str) -> str:
        pub_key_bytes = bytes.fromhex(self.api_key)
        mac = hmac.new(pub_key_bytes, payload.encode("utf-8"), hashlib.sha256)
        digest = mac.digest()

        signing_key = nacl.signing.SigningKey(bytes.fromhex(self.api_secret))
        signature = signing_key.sign(digest).signature
        return signature.hex()

    async def _ensure_session(self) -> None:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def _make_public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await self._ensure_session()
        query = f"?{urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        await self._ensure_session()

        query = f"?{urlencode(params)}" if params else ""
        full_path = f"{path}{query}"
        url = f"{self.base_url}{full_path}"

        body = None
        headers = None
        if method.upper() == "POST":
            body = json.dumps(data or {}, separators=(",", ":"), ensure_ascii=False)
            headers = self._build_headers(path, "POST", body)
            headers["Content-Type"] = "application/json"
            request_kwargs = {"data": body, "headers": headers}
        else:
            headers = self._build_headers(full_path, "GET")
            request_kwargs = {"headers": headers}

        async with self.session.request(method.upper(), url, **request_kwargs) as response:
            response.raise_for_status()
            result = await response.json()
            if result.get("errno") != "200":
                raise ValueError(result.get("msg", "Unknown TurboFlow error"))
            return result

    @query_retry(default_return=(Decimal("0"), Decimal("0")))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """
        TurboFlow fallback BBO derived from latest price and tick size.

        The exchange did not expose a usable best-bid/best-ask feed during
        integration, so we approximate both sides around the latest price.
        """
        latest_price = await self._get_latest_price(contract_id)
        if latest_price <= 0 or self.config.tick_size <= 0:
            return Decimal("0"), Decimal("0")

        best_bid = self.round_to_tick(max(latest_price - self.config.tick_size, self.config.tick_size))
        best_ask = self.round_to_tick(latest_price + self.config.tick_size)
        if best_bid >= best_ask:
            best_ask = best_bid + self.config.tick_size
        return best_bid, best_ask

    async def _get_latest_price(self, contract_id: str) -> Decimal:
        if self.latest_price > 0:
            return self.latest_price

        result = await self._make_request("GET", "/market/pair/price", params={"spot_token_key": contract_id})
        price = Decimal(str(result["data"]["price"]))
        self.latest_price = price
        return price

    async def get_order_price(self, direction: str) -> Decimal:
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Invalid pseudo BBO prices")

        if direction == "buy":
            return self.round_to_tick(best_ask - self.config.tick_size)
        if direction == "sell":
            return self.round_to_tick(best_bid + self.config.tick_size)
        raise ValueError(f"Invalid direction: {direction}")

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        try:
            order_price = await self.get_order_price(direction)
            order_way = 1 if direction == "buy" else 3
            open_vol = self._build_open_vol(quantity)

            payload = {
                "request_id": int(time.time() * 1000),
                "pair_id": str(self.pair_id or contract_id),
                "pool_id": int(self.pool_id),
                "coin_code": str(self.coin_code),
                "order_type": "limit",
                "order_way": order_way,
                "margin_type": 1,
                "leverage": 1,
                "vol": float(open_vol),
                "position_mode": 1,
                "time_in_force": "GTC",
                "fee_mode": 1,
                "order_mode": 1,
                "price": str(self.round_to_tick(order_price)),
            }

            result = await self._make_request("POST", "/account/order/submit", data=payload)
            order = result["data"]["order"]
            return OrderResult(
                success=True,
                order_id=str(order["id"]),
                side=direction,
                size=Decimal(str(order.get("size", quantity))),
                price=Decimal(str(order.get("price", order_price))),
                status=self._map_order_status(order.get("order_status"), order.get("done_size")),
            )
        except Exception as exc:
            message = str(exc)
            if "Rate limit exceeded" in message:
                await asyncio.sleep(2)
            return OrderResult(success=False, error_message=message)

    async def place_close_order(
        self,
        contract_id: str,
        quantity: Decimal,
        price: Decimal,
        side: str,
    ) -> OrderResult:
        try:
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
            adjusted_price = price
            if side == "sell" and price <= best_bid:
                adjusted_price = best_bid + self.config.tick_size
            elif side == "buy" and price >= best_ask:
                adjusted_price = best_ask - self.config.tick_size

            adjusted_price = self.round_to_tick(adjusted_price)
            order_way = 4 if side == "sell" else 2

            payload = {
                "request_id": int(time.time() * 1000),
                "pair_id": str(self.pair_id or contract_id),
                "pool_id": int(self.pool_id),
                "coin_code": str(self.coin_code),
                "order_type": "limit",
                "margin_type": 1,
                "order_way": order_way,
                "size": str(quantity),
                "position_mode": 1,
                "price": str(adjusted_price),
            }

            position_id = await self._get_position_id_for_side(side)
            if position_id:
                payload["position_id"] = position_id

            result = await self._make_request("POST", "/account/order/submit", data=payload)
            order = result["data"]["order"]
            return OrderResult(
                success=True,
                order_id=str(order["id"]),
                side=side,
                size=Decimal(str(order.get("size", quantity))),
                price=Decimal(str(order.get("price", adjusted_price))),
                status=self._map_order_status(order.get("order_status"), order.get("done_size")),
            )
        except Exception as exc:
            message = str(exc)
            if "Rate limit exceeded" in message:
                await asyncio.sleep(2)
            return OrderResult(success=False, error_message=message)

    def _build_open_vol(self, quantity: Decimal) -> Decimal:
        return (Decimal(str(quantity)) * (Decimal("1") + self.open_vol_buffer)).quantize(Decimal("0.000001"))

    async def _get_position_id_for_side(self, side: str) -> Optional[str]:
        positions = await self._list_positions(status="Holding")
        expected_side = 1 if side == "sell" else 2
        for position in positions:
            if str(position.get("pair_id")) != str(self.config.contract_id):
                continue
            if int(position.get("side", 0)) == expected_side and Decimal(str(position.get("hold_size", "0"))) > 0:
                position_id = str(position.get("id"))
                self._position_cache[position_id] = position
                return position_id
        return None

    async def cancel_order(self, order_id: str) -> OrderResult:
        payload = {
            "pair_id": str(self.pair_id or self.config.contract_id),
            "order_id": str(order_id),
            "pool_id": int(self.pool_id),
        }
        await self._make_request("POST", "/account/order/cancel", data=payload)
        return OrderResult(success=True)

    @query_retry(default_return=None)
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        orders = await self._list_orders(status="All")
        for order in orders:
            if str(order.get("id")) != str(order_id):
                continue

            filled_size = Decimal(str(order.get("done_size", "0")))
            size = Decimal(str(order.get("size", "0")))
            return OrderInfo(
                order_id=str(order["id"]),
                side=self._map_order_side(order.get("order_way")),
                size=size,
                price=Decimal(str(order.get("price", "0"))),
                status=self._map_order_status(order.get("order_status"), order.get("done_size")),
                filled_size=filled_size,
                remaining_size=max(size - filled_size, Decimal("0")),
                cancel_reason=str(order.get("err_msg", "")),
            )
        return None

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        orders = await self._list_orders(status="Pending")
        active_orders = []
        for order in orders:
            if str(order.get("pair_id")) != str(contract_id):
                continue

            filled_size = Decimal(str(order.get("done_size", "0")))
            size = Decimal(str(order.get("size", "0")))
            active_orders.append(OrderInfo(
                order_id=str(order["id"]),
                side=self._map_order_side(order.get("order_way")),
                size=size,
                price=Decimal(str(order.get("price", "0"))),
                status=self._map_order_status(order.get("order_status"), order.get("done_size")),
                filled_size=filled_size,
                remaining_size=max(size - filled_size, Decimal("0")),
                cancel_reason=str(order.get("err_msg", "")),
            ))
        return active_orders

    @query_retry(default_return=Decimal("0"))
    async def get_account_positions(self) -> Decimal:
        positions = await self._list_positions(status="Holding")
        total = Decimal("0")
        for position in positions:
            if str(position.get("pair_id")) != str(self.config.contract_id):
                continue
            if int(position.get("status", 0)) != 1:
                continue
            total += abs(Decimal(str(position.get("hold_size", "0"))))
        return total

    async def _list_orders(self, status: str = "All") -> List[Dict[str, Any]]:
        result = await self._make_request(
            "GET",
            "/account/order/list",
            params={"page_num": 1, "page_size": 100, "status": status},
        )
        data = result.get("data")
        if data is None:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            orders = data.get("data")
            if orders is None:
                return []
            if isinstance(orders, list):
                return orders
        return []

    async def _list_positions(self, status: str = "Holding") -> List[Dict[str, Any]]:
        result = await self._make_request(
            "GET",
            "/account/position/list",
            params={"status": status, "page_num": 1, "page_size": 100},
        )
        data = result.get("data")
        if data is None:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            positions = data.get("data")
            if positions is None:
                return []
            if isinstance(positions, list):
                return positions
        return []

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        ticker = self.config.ticker.upper()
        pair_result = await self._make_request("GET", "/pool/pair/list")
        pairs = pair_result.get("data", [])

        pair_info = None
        for pair in pairs:
            if str(pair.get("base_token", "")).upper() == ticker:
                pair_info = pair
                break

        if pair_info is None:
            raise ValueError(f"TurboFlow pair not found for ticker: {ticker}")

        self.pair_id = str(pair_info["pair_id"])
        self.config.contract_id = self.pair_id
        quote_token = str(pair_info.get("quote_token", "USDC")).upper()
        self.pool_id, self.coin_code = await self._resolve_pair_trading_config(pair_info, quote_token)

        decimal_result = await self._make_request("GET", "/market/pair/decimal/config")
        decimal_configs = decimal_result.get("data", [])
        decimal_config = next(
            (cfg for cfg in decimal_configs if str(cfg.get("coin_code")) == str(self.coin_code)),
            None,
        )

        if decimal_config is None:
            self.logger.log(
                f"TurboFlow decimal config not found for coin_code={self.coin_code}, using 4 dp fallback",
                "WARNING",
            )
            self.price_decimals = 4
        else:
            self.price_decimals = int(decimal_config.get("price_decimal", 4))

        self.config.tick_size = Decimal("1") / (Decimal("10") ** self.price_decimals)
        return self.config.contract_id, self.config.tick_size

    async def _resolve_pair_trading_config(
        self,
        pair_info: Dict[str, Any],
        quote_token: str,
    ) -> Tuple[int, str]:
        pool_id = pair_info.get("pool_id")
        coin_code = pair_info.get("coin_code")

        if pool_id is not None and coin_code is not None:
            return int(pool_id), str(coin_code)

        pair_id = str(pair_info["pair_id"])
        try:
            result = await self._make_request(
                "GET",
                "/pool/collateral/list",
                params={"pair_id": pair_id},
            )
            collateral_list = result.get("data", [])

            preferred = None
            for collateral in collateral_list:
                if str(collateral.get("coin_name", "")).upper() == quote_token:
                    preferred = collateral
                    break

            target = preferred or (collateral_list[0] if collateral_list else None)
            if target is not None:
                resolved_pool_id = target.get("pool_id", pool_id)
                resolved_coin_code = target.get("coin_code", coin_code)
                if resolved_pool_id is not None and resolved_coin_code is not None:
                    return int(resolved_pool_id), str(resolved_coin_code)
        except Exception as exc:
            self.logger.log(
                f"Unable to resolve TurboFlow collateral config for pair_id={pair_id}: {exc}",
                "WARNING",
            )

        env_pool_id = os.getenv("TURBOFLOW_POOL_ID")
        if env_pool_id:
            fallback_coin_code = str(coin_code) if coin_code is not None else await self._get_coin_code_for_symbol(quote_token)
            self.logger.log(
                f"Using fallback TurboFlow pool_id={env_pool_id} for pair_id={pair_id}",
                "WARNING",
            )
            return int(env_pool_id), fallback_coin_code

        raise ValueError(
            f"TurboFlow pool_id/coin_code not found for pair_id={pair_id}. "
            "Provide TURBOFLOW_POOL_ID or share /pool/collateral/list response."
        )

    async def _get_coin_code_for_symbol(self, symbol: str) -> str:
        try:
            result = await self._make_public_get("/public/coin/list", params={"action": 1, "chain_id": 0})
            for coin in result.get("data", []):
                names = {
                    str(coin.get("token_name", "")).upper(),
                    str(coin.get("token_slug", "")).upper(),
                }
                if symbol.upper() in names:
                    return str(coin["coin_code"])
        except Exception as exc:
            self.logger.log(f"Unable to resolve TurboFlow coin_code from public API: {exc}", "WARNING")

        fallback = os.getenv("TURBOFLOW_COIN_CODE", "2")
        self.logger.log(f"Using fallback TurboFlow coin_code={fallback} for {symbol}", "WARNING")
        return fallback
