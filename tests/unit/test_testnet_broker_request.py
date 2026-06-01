from __future__ import annotations

import hashlib
import hmac
from typing import Any, List, Optional
from urllib.parse import urlencode

import pytest

from forward import testnet_broker
from forward.testnet_broker import (
    BinanceFuturesTestnetBroker,
    TestnetAPIError as BrokerTestnetAPIError,
    TestnetConfig as BrokerTestnetConfig,
    _sign,
)


class _Resp:
    def __init__(
        self,
        status_code: int,
        payload: Optional[dict] = None,
        headers: Optional[dict] = None,
        text: str = "",
    ) -> None:
        self.status_code = int(status_code)
        self._payload = {} if payload is None else payload
        self.headers = headers or {}
        self.text = text

    def json(self) -> Any:
        return self._payload


class _SeqSession:
    """Returns queued responses in order and records the params each call was given."""

    def __init__(self, responses: List[_Resp]) -> None:
        self._responses = list(responses)
        self.calls: List[dict] = []
        self.n = 0

    def request(self, method: str, url: str, params: Any = None, timeout: Any = None) -> _Resp:
        self.calls.append(
            {"method": method, "url": url, "params": list(params or []), "timeout": timeout}
        )
        resp = self._responses[min(self.n, len(self._responses) - 1)]
        self.n += 1
        return resp


def _bare_broker(responses: List[_Resp]) -> BinanceFuturesTestnetBroker:
    broker = object.__new__(BinanceFuturesTestnetBroker)
    broker.cfg = BrokerTestnetConfig()
    broker.api_key = "test-key"
    broker.api_secret = "test-secret"
    broker.session = _SeqSession(responses)
    return broker


def test_sign_is_deterministic_known_vector() -> None:
    query = "symbol=BTCUSDT&side=BUY&recvWindow=5000&timestamp=1700000000000"
    expected = hmac.new(b"topsecret", query.encode("utf-8"), hashlib.sha256).hexdigest()
    s1 = _sign("topsecret", query)
    s2 = _sign("topsecret", query)
    assert s1 == s2 == expected
    assert len(s1) == 64


def test_signed_request_appends_expected_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(testnet_broker, "_now_ms", lambda: 1_700_000_000_000)
    broker = _bare_broker([_Resp(200, {"ok": True})])

    out = broker._request("GET", "/fapi/v1/order", params={"symbol": "BTCUSDT", "side": "BUY"}, signed=True)
    assert out == {"ok": True}

    pairs = broker.session.calls[0]["params"]
    assert pairs[-1][0] == "signature"
    keys = [k for k, _ in pairs]
    assert "timestamp" in keys and "recvWindow" in keys
    # The signature is HMAC-SHA256 over the urlencoded query of every prior pair.
    body = urlencode(pairs[:-1], doseq=True)
    assert pairs[-1][1] == hmac.new(b"test-secret", body.encode("utf-8"), hashlib.sha256).hexdigest()


def test_request_honors_retry_after_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: List[float] = []
    monkeypatch.setattr(testnet_broker.time, "sleep", lambda s: sleeps.append(float(s)))
    broker = _bare_broker(
        [
            _Resp(429, {"code": -1003, "msg": "Too many requests"}, headers={"Retry-After": "7"}),
            _Resp(200, {"ok": True}),
        ]
    )

    out = broker._request("GET", "/fapi/v1/ping", signed=False)
    assert out == {"ok": True}
    assert broker.session.n == 2  # retried exactly once
    # Slept for the Retry-After value (7s) plus small jitter, not the 0.5s base backoff.
    assert sleeps
    assert 7.0 <= sleeps[0] < 7.2


def test_request_raises_api_error_on_400(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(testnet_broker, "_now_ms", lambda: 1_700_000_000_000)
    broker = _bare_broker([_Resp(400, {"code": -2010, "msg": "NEW_ORDER_REJECTED"})])

    with pytest.raises(BrokerTestnetAPIError) as excinfo:
        broker._request("POST", "/fapi/v1/order", params={"symbol": "BTCUSDT"}, signed=True)

    err = excinfo.value
    assert err.status_code == 400
    assert err.payload.get("code") == -2010
    assert broker.session.n == 1  # 400s are not retried


def test_position_risk_falls_back_v3_to_v2() -> None:
    broker = object.__new__(BinanceFuturesTestnetBroker)
    calls: List[str] = []

    def _fake_request(method: str, path: str, *, params: Any = None, signed: bool = False, timeout: float = 10.0):
        calls.append(path)
        if path == "/fapi/v3/positionRisk":
            raise BrokerTestnetAPIError("v3 unavailable", status_code=404, payload={})
        return [{"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "100"}]

    broker._request = _fake_request  # type: ignore[method-assign]

    row = broker.position_risk(symbol="BTCUSDT")
    assert row["symbol"] == "BTCUSDT"
    assert row["positionAmt"] == "0.5"
    assert calls == ["/fapi/v3/positionRisk", "/fapi/v2/positionRisk"]
