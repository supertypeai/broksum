"""Minimal SocketCluster v17+ client over curl_cffi WS for IPOT scraping.

Implements only what IPOT's broker-summary path needs:
  - Open WSS with Chrome TLS impersonation
  - Send #handshake, wait for handshake reply
  - Send `event:"login"` with `cmd:"autologin"`, persist rotated token
  - Send `event:"cmd"` RPCs and collect all `record` events for that cmdid
  - Close cleanly

This is purposely *not* a generic SocketCluster client. It encodes IPOT's
specific framing conventions (cid sequencing, HOLD/'reply as record' patterns,
JWT-extracted autologintoken). When IPOT changes their protocol we'll need
to update this file to match.
"""

import base64
import json
import logging
import time
from typing import Any

from curl_cffi.requests import Session

log = logging.getLogger(__name__)

WSS_URL = "wss://ipotapp.ipot.id/socketcluster/?appsession={appsession}"
HOMEPAGE = "https://www.indopremier.com/"
APPSESSION_URL = "https://www.indopremier.com/ipc/appsession.js"
CHROME_IMPERSONATE = "chrome146"

# Asset URLs that IPOT's SPA loads on boot. Pre-fetching them with the right
# Referer/Sec-Fetch-* headers gets us a non-empty appsession token from
# /ipc/appsession.js (the server returns empty for "naked" requests).
WARM_ASSETS = (
    "https://www.indopremier.com/app/css/app.css?v=20260226",
    "https://www.indopremier.com/app/js/socketcluster.js?v=20251008",
    "https://www.indopremier.com/app/js/index.js?v=20251205",
    "https://www.indopremier.com/app/lib/socketcluster-client/socketcluster.min.js",
    "https://www.indopremier.com/app/lib/md5/dist/md5.min.js",
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

DEFAULT_DEVICE = {
    "desktop": True,
    "windows": True,
    "os": "windows",
    "pixelRatio": 1,
}

HANDSHAKE_TIMEOUT_S = 15
AUTH_TIMEOUT_S = 20
RPC_TIMEOUT_S = 30


class IPOTAuthError(RuntimeError):
    """Raised when IPOT auth fails (expired/invalid token, NEEDLOGIN, TRYAGAINN, etc.)."""


def _decode_jwt_payload(jwt_str: str) -> dict | None:
    """Decode JWT without verifying signature. Returns payload dict or None."""
    parts = jwt_str.split(".")
    if len(parts) < 2:
        return None
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    except Exception:
        return None


def _describe_frame(value, depth: int = 0):
    """Frame summary for debug logs: secrets and long strings are hidden."""
    if depth > 6:
        return "..."

    if isinstance(value, dict):
        return {
            key: (
                "<redacted>"
                if key in ("token", "lasttoken", "session", "password", "qrcode", "authToken")
                else _describe_frame(nested_value, depth + 1)
            )
            for key, nested_value in value.items()
        }

    if isinstance(value, list):
        return [_describe_frame(list_item, depth + 1) for list_item in value[:5]]

    if isinstance(value, str) and len(value) > 60:
        return f"<str len={len(value)}>"

    return value


class IPOTConnection:
    """Single SocketCluster connection: connect, authenticate, send RPCs.

    Use as a context manager so close() always runs.
    """

    def __init__(self, autologintoken: str, appsession: str | None = None,
                 cookies: list[dict] | None = None):
        self.autologintoken = autologintoken
        self.appsession = appsession
        self.cookies = cookies or []
        self.sess: Session | None = None
        self.ws = None
        self._cid = 0
        self._authenticated = False
        # Overrides the per-request timeouts while warming up (None = defaults)
        self.request_timeout_seconds: float | None = None
        # Buffer of frames received during one logical exchange but not for
        # the cid we were waiting for. We re-process them on the next call.
        self._spillover: list[dict] = []

    # ---- low-level frame I/O ----

    def _next_cid(self) -> int:
        self._cid += 1
        return self._cid

    def _send_obj(self, obj: dict) -> None:
        self.ws.send(json.dumps(obj).encode("utf-8"))

    def _recv_obj(self) -> dict | None:
        f = self.ws.recv()
        data = f[0] if isinstance(f, tuple) else f
        if isinstance(data, bytes):
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                return None
        else:
            text = str(data)
        if text == "#1":
            # SocketCluster server ping; answer it or the server drops us
            # after pingTimeout (matters for long waits like a QR scan)
            self.ws.send(b"#2")
            return None
        if not text or text.startswith("#"):
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            log.debug(f"Non-JSON WS frame: {text[:100]}")
            return None

    # ---- session warmup + WS connect ----

    def _warm_session(self) -> str:
        """Pre-fetch SPA assets, return a fresh appsession token.

        Must be called before opening the WS so /ipc/appsession.js returns
        a non-empty token.
        """
        assert self.sess is not None
        self.sess.get(HOMEPAGE, timeout=self.request_timeout_seconds or 20)
        asset_headers = {"Referer": HOMEPAGE, "Sec-Fetch-Site": "same-origin"}
        for url in WARM_ASSETS:
            try:
                self.sess.get(url, headers=asset_headers, timeout=self.request_timeout_seconds or 15)
            except Exception:
                pass
        r = self.sess.get(
            APPSESSION_URL,
            headers={
                "Referer": HOMEPAGE,
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-origin",
                "Accept": "*/*",
            },
            timeout=self.request_timeout_seconds or 20,
        )
        text = r.text
        if "appsession='" in text:
            return text.split("appsession='", 1)[1].split("'", 1)[0]
        return ""

    def connect(self) -> None:
        """Open the WS, run SocketCluster handshake. Does not authenticate."""
        self.sess = Session(impersonate=CHROME_IMPERSONATE)
        for c in self.cookies:
            try:
                self.sess.cookies.set(
                    c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/")
                )
            except Exception:
                pass

        appsession = self._warm_session()
        if not appsession:
            raise IPOTAuthError("Could not fetch appsession token")
        # If we had a stored appsession (from saved creds), prefer the freshly
        # fetched one — it's tied to this curl_cffi session's IP/cookies.
        self.appsession = appsession

        ws_headers = {
            "Origin": "https://www.indopremier.com",
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        }
        self.ws = self.sess.ws_connect(
            WSS_URL.format(appsession=appsession), headers=ws_headers
        )

        # Send #handshake immediately. Server kicks with code 4005 if we
        # don't initiate within ~5s.
        self._send_obj({"event": "#handshake", "data": {"authToken": None}, "cid": self._next_cid()})

        deadline = time.time() + HANDSHAKE_TIMEOUT_S
        while time.time() < deadline:
            obj = self._recv_obj()
            if obj is None:
                continue
            if obj.get("rid") == 1 and isinstance(obj.get("data"), dict) and "id" in obj["data"]:
                log.info(f"IPOT WS handshake OK (sid={obj['data']['id']})")
                return
        raise IPOTAuthError("WS handshake timed out")

    # ---- authentication ----

    def authenticate(self, lazy: bool = True) -> None:
        """Run autologin with stored token. On success, persist rotated token."""
        cid = self._next_cid()
        payload = {
            "event": "login",
            "data": {
                "cmdid": cid,  # arbitrary; mirror cid for tracking
                "param": {
                    "cmd": "autologin",
                    "lasttoken": self.autologintoken,
                    "lazy": lazy,
                    "session": self.appsession,
                    "device": DEFAULT_DEVICE,
                },
            },
            "cid": cid,
        }
        self._send_obj(payload)

        # Look for #setAuthToken with new JWT, then record event with cmdid==cid
        new_token = None
        success = False
        deadline = time.time() + AUTH_TIMEOUT_S
        while time.time() < deadline:
            obj = self._recv_obj()
            if obj is None:
                continue
            ev = obj.get("event")
            if ev == "#setAuthToken":
                jwt = (obj.get("data") or {}).get("token")
                payload_decoded = _decode_jwt_payload(jwt) if jwt else None
                if payload_decoded:
                    new_token = payload_decoded.get("token")
            elif ev == "record":
                d = obj.get("data") or {}
                if d.get("cmdid") == cid:
                    if "errmsg" in d:
                        raise IPOTAuthError(f"autologin rejected: {d['errmsg']}")
                    success = True
                    break
            elif obj.get("rid") == cid:
                # lazy=false: IPOT answers in the direct reply instead of a record
                log.debug(f"autologin direct reply: {_describe_frame(obj)}")

                if obj.get("error"):
                    raise IPOTAuthError(f"autologin rejected: {obj['error']}")

                reply_data = obj.get("data")

                if not isinstance(reply_data, dict) or reply_data.get("status") == "HOLD":
                    continue

                if "errmsg" in reply_data:
                    raise IPOTAuthError(f"autologin rejected: {reply_data['errmsg']}")

                reply_result = reply_data.get("result") or {}

                if reply_result.get("status") not in (None, "OK"):
                    raise IPOTAuthError(f"autologin rejected: {_describe_frame(reply_result)}")

                success = True
                break
            else:
                # Buffer non-auth records (notifs, other channels) — we don't need them
                pass

        if not success:
            raise IPOTAuthError("autologin timed out before record reply")

        # The rotated token can arrive just after the reply; give it a moment
        token_deadline = time.time() + 3

        while new_token is None and time.time() < token_deadline:
            obj = self._recv_obj()

            if obj and obj.get("event") == "#setAuthToken":
                jwt = (obj.get("data") or {}).get("token")
                payload_decoded = _decode_jwt_payload(jwt) if jwt else None

                if payload_decoded:
                    new_token = payload_decoded.get("token")

        if new_token:
            self.autologintoken = new_token
        self._authenticated = True
        log.info("IPOT autologin OK; token rotated")

    def qr_login(self, show_qrcode, max_qr_attempts: int = 3, lazy: bool = True) -> None:
        """
        Log this connection in by QR, the same way the desktop web app does.

        Sends `login` with `cmd:"getqr"` IPOT replies with a QR string valid
        for `span` seconds. `show_qrcode(qrcode_text, span_seconds)` must
        display it so a human can scan it with the IPOT mobile app. After the
        scan, IPOT pushes `#setAuthToken` (and a record for our cmdid) on this
        same connection, so no token has to be copied anywhere.
        """
        for qr_attempt in range(1, max_qr_attempts + 1):
            command_id = self._next_cid()

            self._send_obj({
                "event": "login",
                "data": {
                    "cmdid": command_id,
                    "param": {
                        "cmd": "getqr",
                        "lazy": lazy,
                        "session": self.appsession,
                        "device": DEFAULT_DEVICE,
                    },
                },
                "cid": command_id,
            })

            received_auth_token = False
            deadline = time.time() + HANDSHAKE_TIMEOUT_S

            while time.time() < deadline:
                message = self._recv_obj()

                if message is None:
                    continue

                event_name = message.get("event")

                if event_name == "notif":
                    continue

                log.debug(f"QR login frame: {_describe_frame(message)}")

                if event_name == "#setAuthToken":
                    jwt_token = (message.get("data") or {}).get("token")
                    jwt_payload = _decode_jwt_payload(jwt_token) if jwt_token else None

                    if jwt_payload and jwt_payload.get("token"):
                        self.autologintoken = jwt_payload["token"]

                    received_auth_token = True
                    # The login record usually follows right away
                    deadline = min(deadline, time.time() + 5)
                    continue

                # With lazy=true IPOT answers HOLD and sends the QR as a
                # `record` with lazy=false it puts the QR in the direct
                # reply (rid == our cid). Accept both shapes
                is_direct_reply = message.get("rid") == command_id

                if event_name != "record" and not is_direct_reply:
                    continue

                record_data = message.get("data") or {}

                if not isinstance(record_data, dict) or record_data.get("cmdid") != command_id:
                    continue

                if is_direct_reply and record_data.get("status") == "HOLD":
                    continue

                if "errmsg" in record_data:
                    raise IPOTAuthError(f"QR login rejected: {record_data['errmsg']}")

                # The reply echoes our request under record.data.data,
                # with the outcome at record.data.data.result
                inner_data = record_data.get("data") or {}
                login_result = inner_data.get("result") or record_data.get("result") or {}
                result_info = login_result.get("info") or {}

                if result_info.get("qrcode"):
                    span_seconds = int(result_info.get("span") or 60)
                    show_qrcode(result_info["qrcode"], span_seconds)
                    deadline = time.time() + span_seconds + 5
                    continue

                # Any other record for our cmdid after the QR is the login outcome
                if login_result.get("status") == "OK" or received_auth_token:
                    self._authenticated = True
                    log.info("IPOT QR login OK")
                    return

                raise IPOTAuthError(f"QR login failed: {_describe_frame(record_data)}")

            if received_auth_token:
                self._authenticated = True
                log.info("IPOT QR login OK (auth token received)")
                return

            log.warning(f"QR code expired without a scan (attempt {qr_attempt}/{max_qr_attempts})")

        raise IPOTAuthError(f"QR login not completed after {max_qr_attempts} QR code(s)")

    # ---- RPC ----

    def send_request(self, service: str, cmd: str, param: dict) -> list[Any]:
        """Send a SocketCluster RPC and collect all data records for it.

        Returns the list of `record.data.data` payloads streamed back.
        Stops collecting when server sends the sentinel `recno: -1`.
        """
        if not self._authenticated:
            raise IPOTAuthError("send_request before authenticate()")

        cid = self._next_cid()
        cmdid = cid
        request = {
            "event": "cmd",
            "data": {
                "cmdid": cmdid,
                "param": {"service": service, "cmd": cmd, "param": param},
            },
            "cid": cid,
        }
        self._send_obj(request)

        # Server replies with rid:cid and either:
        #   - data list directly (small response), OR
        #   - {status:OK, msg:"reply as record"} — actual rows arrive as
        #     `record` events with cmdid==cmdid, terminated by recno:-1.
        rows: list[Any] = []
        ack_seen = False
        terminated = False
        deadline = time.time() + RPC_TIMEOUT_S

        while time.time() < deadline and not terminated:
            obj = self._recv_obj()
            if obj is None:
                continue

            if obj.get("rid") == cid and not ack_seen:
                rdata = obj.get("data")
                if isinstance(rdata, dict) and (
                    rdata.get("status") in ("HOLD", "OK")
                    and rdata.get("msg") in ("Wait a second", "reply as record")
                ):
                    ack_seen = True
                    continue
                if isinstance(rdata, list):
                    rows.extend(rdata)
                    terminated = True
                    break
                if isinstance(rdata, dict) and "errmsg" in rdata:
                    raise RuntimeError(f"RPC failed: {rdata['errmsg']}")

            if obj.get("event") == "record":
                d = obj.get("data") or {}
                if d.get("cmdid") != cmdid:
                    continue
                if "errmsg" in d:
                    raise RuntimeError(f"RPC failed: {d['errmsg']}")
                inner = d.get("data")
                if d.get("recno") == -1:
                    terminated = True
                    break
                if isinstance(inner, list):
                    rows.extend(inner)
                elif inner is not None:
                    rows.append(inner)

        if not terminated:
            log.warning(f"RPC {service}/{cmd} did not terminate cleanly within {RPC_TIMEOUT_S}s")
        return rows

    # ---- cleanup ----

    def close(self) -> None:
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
        if self.sess is not None:
            try:
                self.sess.close()
            except Exception:
                pass
            self.sess = None
        self._authenticated = False

    def __enter__(self):
        self.connect()
        self.authenticate()
        return self

    def __exit__(self, *exc):
        self.close()
