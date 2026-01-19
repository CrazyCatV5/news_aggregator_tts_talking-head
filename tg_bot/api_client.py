from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class ApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, detail: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class ApiClientConfig:
    base_url: str
    timeout_s: float = 15.0
    download_timeout_s: float = 120.0


def _default_base_url() -> str:
    return os.getenv("API_BASE_URL", "http://api:8088").rstrip("/")


def _has_api_prefix(path: str) -> bool:
    return path.startswith("/api/") or path == "/api"


class ApiClient:
    """HTTP client for internal FastAPI.

    Constraints: bot never touches SQLite/files directly; API is the source of truth.
    """

    def __init__(self, cfg: Optional[ApiClientConfig] = None):
        self.cfg = cfg or ApiClientConfig(base_url=_default_base_url())
        self._client = httpx.Client(
            base_url=self.cfg.base_url,
            timeout=httpx.Timeout(self.cfg.timeout_s, connect=self.cfg.timeout_s),
            headers={"User-Agent": "dfo-news-tg-bot/1.0"},
        )

    def close(self) -> None:
        self._client.close()

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=6.0),
        reraise=True,
    )
    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json: Any = None) -> httpx.Response:
        resp = self._client.request(method, path, params=params, json=json)
        if resp.status_code == 404 and (not _has_api_prefix(path)):
            resp2 = self._client.request(method, "/api" + path, params=params, json=json)
            return resp2
        return resp

    def _json_or_error(self, resp: httpx.Response) -> Dict[str, Any]:
        if resp.status_code >= 400:
            detail = None
            try:
                j = resp.json()
                detail = j.get("detail") if isinstance(j, dict) else None
            except Exception:
                detail = (resp.text or "").strip()[:500]
            raise ApiError(
                f"API error {resp.status_code} for {resp.request.method} {resp.request.url}",
                status_code=resp.status_code,
                detail=detail,
            )

        try:
            data = resp.json()
        except Exception as e:
            raise ApiError(f"API returned non-JSON response: {e}")
        if not isinstance(data, dict):
            raise ApiError("API returned unexpected JSON shape")
        return data

    # --- High-level API methods ---

    def get_digest(self, day: str) -> Dict[str, Any]:
        resp = self._request("GET", "/digests/daily", params={"day": day})
        return self._json_or_error(resp)

    def list_digests(self, limit: int = 14, offset: int = 0) -> Dict[str, Any]:
        resp = self._request("GET", "/digests", params={"limit": limit, "offset": offset})
        return self._json_or_error(resp)

    def tts_status(self, day: str, language: str = "ru") -> Dict[str, Any]:
        resp = self._request("GET", f"/tts/daily/{day}", params={"language": language})
        return self._json_or_error(resp)

    def tts_render(self, day: str, language: str = "ru") -> Dict[str, Any]:
        resp = self._request("POST", f"/tts/daily/{day}/render", params={"language": language})
        return self._json_or_error(resp)

    def video_status(self, day: str, language: str = "ru") -> Dict[str, Any]:
        resp = self._request("GET", f"/video/daily/{day}", params={"language": language})
        return self._json_or_error(resp)

    def video_render(self, day: str, language: str = "ru") -> Dict[str, Any]:
        resp = self._request("POST", f"/video/daily/{day}/render", params={"language": language})
        return self._json_or_error(resp)

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=6.0),
        reraise=True,
    )
    def download_to_file(self, download_url: str, dest_path: str) -> None:
        url = download_url if download_url.startswith("/") else "/" + download_url
        with httpx.Client(
            base_url=self.cfg.base_url,
            timeout=httpx.Timeout(self.cfg.download_timeout_s, connect=self.cfg.timeout_s),
            headers={"User-Agent": "dfo-news-tg-bot/1.0"},
        ) as client:
            r = client.get(url)
            if r.status_code == 404 and (not _has_api_prefix(url)):
                r = client.get("/api" + url)
            if r.status_code >= 400:
                raise ApiError(f"download failed: {r.status_code}", status_code=r.status_code, detail=r.text[:500])
            with open(dest_path, "wb") as f:
                f.write(r.content)
