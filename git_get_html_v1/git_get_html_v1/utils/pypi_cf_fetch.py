"""PyPI Cloudflare 过盾：单 Session 内完成 GET → script.js → PoW → 再 GET。"""

from __future__ import annotations

import hashlib
import json
import re
import time

from curl_cffi import requests as cffi_requests

from tools.key_token_config import PROXY_GITHUB_PYPI

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "cache-control": "max-age=0",
    "upgrade-insecure-requests": "1",
}

POST_BACK_URL = "https://pypi.org/_fs-ch-1T1wmsGaOgGaSxcX/fst-post-back"
RELOAD_SCRIPT_URL = "https://pypi.org/_fs-ch-1T1wmsGaOgGaSxcX/script.js?reload=true"
DEFAULT_IMPERSONATE = "chrome120"
MAX_ATTEMPTS = 2
REQUEST_DELAY = 1.0


def normalize_pypi_url(raw: str) -> str:
    url = (raw or "").strip()
    if not url:
        raise ValueError("empty pypi task")
    if url.startswith("pkg:pypi/"):
        url = "https://pypi.org/project/" + url[len("pkg:pypi/") :]
    elif url.startswith("pkg:pypi"):
        url = "https://pypi.org/project/" + url.replace("pkg:pypi/", "")
    elif not url.startswith("http://") and not url.startswith("https://"):
        url = "https://pypi.org/project/" + url.lstrip("/")
    return url.rstrip("/")


def _json_parse(input_string: str) -> dict:
    json_part, *other_parts = input_string.split(", ", 1)
    json_data = json.loads(json_part.strip())
    other_parts = other_parts[0].split(", ") if other_parts else []
    parsed_others = []
    for part in other_parts:
        part = part.strip()
        if part.lower() == "true":
            parsed_others.append(True)
        elif part.lower() == "false":
            parsed_others.append(False)
        elif part.startswith('"') and part.endswith('"'):
            parsed_others.append(part[1:-1])
        else:
            parsed_others.append(part)
    return {
        "json_part": json_data,
        "encrypted_str": parsed_others[0] if parsed_others else "",
    }


def _generate_pow_answer(base: str, target_hash: str) -> str:
    charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for char1 in charset:
        for char2 in charset:
            candidate = base + char1 + char2
            if hashlib.sha256(candidate.encode()).hexdigest() == target_hash:
                return char1 + char2
    return ""


class PypiHtmlFetcher:
    """PyPI 页面抓取：TLS 指纹 + Cloudflare Client Challenge 解算。"""

    def __init__(self, proxy: str | None = None, impersonate: str = DEFAULT_IMPERSONATE):
        self.proxy = proxy or PROXY_GITHUB_PYPI
        self.impersonate = impersonate

    def _new_session(self) -> cffi_requests.Session:
        return cffi_requests.Session(impersonate=self.impersonate)

    def _request(self, session: cffi_requests.Session, method: str, url: str, **kwargs):
        kwargs.setdefault("timeout", 30)
        kwargs.setdefault("proxies", {"http": self.proxy, "https": self.proxy})
        return session.request(method, url, **kwargs)

    def _solve_pow_from_script(self, session: cffi_requests.Session, url: str, script_text: str) -> bool:
        matches = re.findall(r"init(\(.*?\));", script_text or "", re.DOTALL)
        if not matches:
            return False

        structured = _json_parse(matches[-1].strip("()"))
        pow_data = structured["json_part"][0].get("data") or {}
        answer = _generate_pow_answer(pow_data.get("base", ""), pow_data.get("hash", ""))
        if not answer:
            return False

        payload = {
            "token": structured["encrypted_str"],
            "data": [{
                "ty": "pow",
                "base": pow_data.get("base"),
                "answer": answer,
                "hmac": pow_data.get("hmac"),
                "expires": pow_data.get("expires"),
            }],
        }
        post_headers = {
            **HEADERS,
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://pypi.org",
            "referer": url,
        }
        resp = self._request(session, "POST", POST_BACK_URL, headers=post_headers, json=payload)
        return resp.status_code == 200

    def _pass_cloudflare(self, session: cffi_requests.Session, url: str) -> cffi_requests.Response:
        page_headers = {**HEADERS, "referer": url}

        first = self._request(session, "GET", url, headers=page_headers)
        if first.status_code == 404:
            return first

        from .impersonate import is_valid_pypi_html

        if first.status_code == 200 and is_valid_pypi_html(first.text):
            return first

        script_resp = self._request(
            session,
            "GET",
            RELOAD_SCRIPT_URL,
            headers={**HEADERS, "referer": url},
        )
        if script_resp.status_code != 200 or "init(" not in (script_resp.text or ""):
            return first

        if not self._solve_pow_from_script(session, url, script_resp.text):
            return first

        return self._request(session, "GET", url, headers=page_headers)

    def fetch_html(self, url: str, max_attempts: int = MAX_ATTEMPTS) -> cffi_requests.Response:
        url = normalize_pypi_url(url)
        last_response = None

        for attempt in range(1, max_attempts + 1):
            session = self._new_session()
            try:
                resp = self._pass_cloudflare(session, url)
            except Exception:
                time.sleep(REQUEST_DELAY * attempt)
                continue
            last_response = resp

            if resp.status_code == 404:
                return resp

            from .impersonate import is_valid_pypi_html

            if resp.status_code == 200 and is_valid_pypi_html(resp.text):
                return resp

            time.sleep(REQUEST_DELAY * attempt)

        return last_response


def fetch_pypi_html(url: str, proxy: str | None = None) -> tuple[str, int, float]:
    """同步抓取 PyPI 项目页，返回 (html_text, status_code, latency_seconds)。"""
    started = time.time()
    fetcher = PypiHtmlFetcher(proxy=proxy)
    try:
        resp = fetcher.fetch_html(url)
    except Exception:
        return "", 0, time.time() - started
    return resp.text or "", resp.status_code, time.time() - started
