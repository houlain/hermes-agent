"""
QQ Bot platform adapter using the Official QQ Bot API (v2).

Connects to the QQ Bot WebSocket Gateway for inbound events and uses the
REST API (``api.sgroup.qq.com``) for outbound messages and media uploads.

Configuration in config.yaml:
    platforms:
      qq:
        enabled: true
        extra:
          app_id: "your-app-id"            # or QQ_APP_ID env var
          client_secret: "your-secret"     # or QQ_CLIENT_SECRET env var
          markdown_support: true           # enable QQ markdown (msg_type 2)
          dm_policy: "pairing"             # open | allowlist | disabled | pairing
          allow_from: ["openid_1"]
          group_policy: "pairing"          # open | allowlist | disabled | pairing
          group_allow_from: ["group_openid_1"]
          stt:                             # Voice-to-text config (optional)
            provider: "zai"                # zai (GLM-ASR), openai (Whisper), etc.
            baseUrl: "https://open.bigmodel.cn/api/coding/paas/v4"
            apiKey: "your-...key"     # or set QQ_STT_API_KEY env var
            model: "glm-asr"               # glm-asr, whisper-1, etc.

    Voice transcription priority:
      1. QQ's built-in ``asr_refer_text`` (Tencent ASR — free, always tried first)
      2. Configured STT provider via ``stt`` config or ``QQ_STT_*`` env vars

Reference: https://bot.q.qq.com/wiki/develop/api-v2/
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import mimetypes
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    _ssrf_redirect_guard,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.platforms.helpers import strip_markdown

logger = logging.getLogger(__name__)


class QQCloseError(Exception):
    """Raised when QQ WebSocket closes with a specific code.

    Carries the close code and reason for proper handling in the reconnect loop.
    """

    def __init__(self, code, reason=""):
        self.code = int(code) if code else None
        self.reason = str(reason) if reason else ""
        super().__init__(f"WebSocket closed (code={self.code}, reason={self.reason})")


# ---------------------------------------------------------------------------
# Constants — imported from the shared constants module.
# ---------------------------------------------------------------------------

from gateway.platforms.qqbot.constants import (
    API_BASE,
    TOKEN_URL,
    GATEWAY_URL_PATH,
    DEFAULT_API_TIMEOUT,
    FILE_UPLOAD_TIMEOUT,
    CONNECT_TIMEOUT_SECONDS,
    RECONNECT_BACKOFF,
    MAX_RECONNECT_ATTEMPTS,
    RATE_LIMIT_DELAY,
    QUICK_DISCONNECT_THRESHOLD,
    MAX_QUICK_DISCONNECT_COUNT,
    MAX_MESSAGE_LENGTH,
    DEDUP_WINDOW_SECONDS,
    DEDUP_MAX_SIZE,
    MSG_TYPE_TEXT,
    MSG_TYPE_MARKDOWN,
    MSG_TYPE_MEDIA,
    MSG_TYPE_INPUT_NOTIFY,
    MEDIA_TYPE_IMAGE,
    MEDIA_TYPE_VIDEO,
    MEDIA_TYPE_VOICE,
    MEDIA_TYPE_FILE,
)
from gateway.platforms.qqbot.utils import (
    coerce_list as _coerce_list_impl,
    build_user_agent,
)
from gateway.platforms.qqbot.chunked_upload import (
    ChunkedUploader,
    UploadDailyLimitExceededError,
    UploadFileTooLargeError,
)
from gateway.platforms.qqbot.keyboards import (
    ApprovalRequest,
    InlineKeyboard,
    InteractionEvent,
    build_approval_keyboard,
    build_update_prompt_keyboard,
    parse_approval_button_data,
    parse_interaction_event,
    parse_update_prompt_button_data,
)


def check_qq_requirements() -> bool:
    """Check if QQ runtime dependencies are available."""
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values into a trimmed string list."""
    return _coerce_list_impl(value)


# ---------------------------------------------------------------------------
# QQAdapter
# ---------------------------------------------------------------------------


class QQAdapter(BasePlatformAdapter):
    """QQ Bot adapter backed by the official QQ Bot WebSocket Gateway + REST API."""

    # QQ Bot API does not support editing sent messages.
    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    _TYPING_INPUT_SECONDS = 60  # input_notify duration reported to QQ
    _TYPING_DEBOUNCE_SECONDS = 50  # refresh before it expires

    @property
    def _log_tag(self) -> str:
        """Log prefix including app_id for multi-instance disambiguation."""
        app_id = getattr(self, "_app_id", None)
        if app_id:
            return f"QQBot:{app_id}"
        return "QQBot"

    def _fail_pending(self, reason: str) -> None:
        """Fail all pending response futures."""
        for fut in self._pending_responses.values():
            if not fut.done():
                fut.set_exception(RuntimeError(reason))
        self._pending_responses.clear()

    def _mark_transport_disconnected(self) -> None:
        """Mark QQ WS down without stopping the reconnect loop.

        BasePlatformAdapter uses _running for both process lifecycle and
        connection status. QQBot needs to keep the listener task alive across
        transient transport drops so it can continue reconnect attempts after a
        short-lived gateway or network failure.
        """
        if self.has_fatal_error:
            return
        self._write_runtime_status_safe(
            "disconnected",
            platform_state="disconnected",
            error_code=None,
            error_message=None,
        )

    @property
    def is_connected(self) -> bool:
        """Return True only when the QQ WebSocket transport is usable."""
        return bool(self._running and self._ws and not self._ws.closed)

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.QQBOT)

        extra = config.extra or {}
        self._app_id = str(extra.get("app_id") or os.getenv("QQ_APP_ID", "")).strip()
        self._client_secret = str(
            extra.get("client_secret") or os.getenv("QQ_CLIENT_SECRET", "")
        ).strip()
        self._markdown_support = bool(extra.get("markdown_support", True))

        # Auth/ACL policies
        self._dm_policy = str(extra.get("dm_policy", "pairing")).strip().lower()
        self._allow_from = _coerce_list(
            extra.get("allow_from") or extra.get("allowFrom")
        )
        self._group_policy = str(extra.get("group_policy", "pairing")).strip().lower()
        self._group_allow_from = _coerce_list(
            extra.get("group_allow_from") or extra.get("groupAllowFrom")
        )

        # Connection state
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_interval: float = 30.0  # seconds, updated by Hello
        self._session_id: Optional[str] = None
        self._last_seq: Optional[int] = None
        self._chat_type_map: Dict[str, str] = {}  # chat_id → "c2c"|"group"|"guild"|"dm"

        # Request/response correlation
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._seen_messages: Dict[str, float] = {}

        # Last inbound message ID per chat — used by send_typing
        self._last_msg_id: Dict[str, str] = {}
        # Typing debounce: chat_id → last send_typing timestamp
        self._typing_sent_at: Dict[str, float] = {}

        # Token cache
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_lock = asyncio.Lock()

        # Upload cache: content_hash -> {file_info, file_uuid, expires_at}
        self._upload_cache: Dict[str, Dict[str, Any]] = {}

        # Inline-keyboard interaction routing. The callback (if set) is invoked
        # for every INTERACTION_CREATE event after the adapter has already
        # ACKed it. Callers (gateway wiring for approvals / update prompts)
        # register via set_interaction_callback().
        self._interaction_callback: Optional[
            Callable[[InteractionEvent], Awaitable[None]]
        ] = None

        # Default interaction dispatcher: routes approval-button clicks to
        # tools.approval.resolve_gateway_approval() and update-prompt clicks
        # to ~/.hermes/.update_response. Set here so the cross-adapter gateway
        # contract (send_exec_approval / send_update_prompt) works out of the
        # box; callers can override with set_interaction_callback(None) or
        # register a custom handler.
        self._interaction_callback = self._default_interaction_dispatch

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "QQBot"

    @property
    def enforces_own_access_policy(self) -> bool:
        """QQBot gates DM/group access at intake via dm_policy/group_policy."""
        return True

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Authenticate, obtain gateway URL, and open the WebSocket.
        
        Args:
            is_reconnect: Whether this is a reconnection attempt. Forwarded
                from the gateway reconnect watcher. Not currently used by
                QQ adapter, but accepted to satisfy the BasePlatformAdapter
                contract.
        """
        if not AIOHTTP_AVAILABLE:
            message = "QQ startup failed: aiohttp not installed"
            self._set_fatal_error("qq_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install aiohttp", self._log_tag, message)
            return False
        if not HTTPX_AVAILABLE:
            message = "QQ startup failed: httpx not installed"
            self._set_fatal_error("qq_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install httpx", self._log_tag, message)
            return False
        if not self._app_id or not self._client_secret:
            message = "QQ startup failed: QQ_APP_ID and QQ_CLIENT_SECRET are required"
            self._set_fatal_error("qq_missing_credentials", message, retryable=True)
            logger.warning("[%s] %s", self._log_tag, message)
            return False

        # Prevent duplicate connections with the same credentials
        if not self._acquire_platform_lock("qqbot-appid", self._app_id, "QQBot app ID"):
            return False

        try:
            # Tighter keepalive pool so idle CLOSE_WAIT sockets drain
            # faster behind proxies like Cloudflare Warp (#18451).
            from gateway.platforms._http_client_limits import platform_httpx_limits
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                follow_redirects=True,
                event_hooks={"response": [_ssrf_redirect_guard]},
                limits=platform_httpx_limits(),
            )

            # 1. Get access token
            await self._ensure_token()

            # 2. Get WebSocket gateway URL
            gateway_url = await self._get_gateway_url()
            logger.info("[%s] Gateway URL: %s", self._log_tag, gateway_url)

            # 3. Open WebSocket
            await self._open_ws(gateway_url)

            # 4. Start listeners
            self._listen_task = asyncio.create_task(self._listen_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._mark_connected()
            logger.info("[%s] Connected", self._log_tag)
            return True
        except Exception as exc:
            message = f"QQ startup failed: {exc}"
            self._set_fatal_error("qq_connect_error", message, retryable=True)
            logger.error("[%s] %s", self._log_tag, message, exc_info=True)
            await self._cleanup()
            self._release_platform_lock()
            return False

    async def disconnect(self) -> None:
        """Close all connections and stop listeners."""
        self._running = False
        self._mark_disconnected()

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        await self._cleanup()
        self._release_platform_lock()
        logger.info("[%s] Disconnected", self._log_tag)

    async def _cleanup(self) -> None:
        """Close WebSocket, HTTP session, and client."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        # Fail pending
        for fut in self._pending_responses.values():
            if not fut.done():
                fut.set_exception(RuntimeError("Disconnected"))
        self._pending_responses.clear()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    async def _ensure_token(self) -> str:
        """Return a valid access token, refreshing if needed (with singleflight)."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token

        async with self._token_lock:
            # Double-check after acquiring lock
            if self._access_token and time.time() < self._token_expires_at - 60:
                return self._access_token

            try:
                resp = await self._http_client.post(
                    TOKEN_URL,
                    json={"appId": self._app_id, "clientSecret": self._client_secret},
                    timeout=DEFAULT_API_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                raise RuntimeError(f"Failed to get QQ Bot access token: {exc}") from exc

            token = data.get("access_token")
            if not token:
                raise RuntimeError(
                    f"QQ Bot token response missing access_token: {data}"
                )

            expires_in = int(data.get("expires_in", 7200))
            self._access_token = token
            self._token_expires_at = time.time() + expires_in
            logger.info(
                "[%s] Access token refreshed, expires in %ds", self._log_tag, expires_in
            )
            return self._access_token

    async def _get_gateway_url(self) -> str:
        """Fetch the WebSocket gateway URL from the REST API."""
        token = await self._ensure_token()
        try:
            resp = await self._http_client.get(
                f"{API_BASE}{GATEWAY_URL_PATH}",
                headers={
                    "Authorization": f"QQBot {token}",
                    "User-Agent": build_user_agent(),
                },
                timeout=DEFAULT_API_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            raise RuntimeError(f"Failed to get QQ Bot gateway URL: {exc}") from exc

        url = data.get("url")
        if not url:
            raise RuntimeError(f"QQ Bot gateway response missing url: {data}")
        return url

    # ------------------------------------------------------------------
    # WebSocket lifecycle
    # ------------------------------------------------------------------

    async def _open_ws(self, gateway_url: str) -> None:
        """Open a WebSocket connection to the QQ Bot gateway."""
        # Only clean up WebSocket resources — keep _http_client alive for REST API calls.
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

        # Honor WSL proxy env for QQ WebSocket. Hermes upgrades overwrite this
        # local patch, so QQ can regress to direct-connect timeouts after update.
        self._session = aiohttp.ClientSession(trust_env=True)
        ws_proxy = (
            os.getenv("WSS_PROXY")
            or os.getenv("wss_proxy")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALL_PROXY")
            or os.getenv("all_proxy")
        )
        self._ws = await self._session.ws_connect(
            gateway_url,
            headers={
                "User-Agent": build_user_agent(),
            },
            timeout=CONNECT_TIMEOUT_SECONDS,
            proxy=ws_proxy,
        )
        logger.info("[%s] WebSocket connected to %s", self._log_tag, gateway_url)

    async def _listen_loop(self) -> None:
        """Read WebSocket events and reconnect on errors.

        Close code handling follows the OpenClaw qqbot reference implementation:
          4004 → invalid token, refresh and reconnect
          4006/4007/4009 → session invalid, clear session and re-identify
          4008 → rate limited, back off 60s
          4914 → bot offline/sandbox, stop reconnecting
          4915 → bot banned, stop reconnecting
        """
        backoff_idx = 0
        connect_time = 0.0
        quick_disconnect_count = 0

        while self._running: