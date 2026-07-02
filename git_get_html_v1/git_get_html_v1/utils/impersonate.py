import random

# 较新的 Chrome 指纹，TLS 特征更接近真实浏览器
IMPERSONATE = ["chrome120", "chrome123", "chrome124"]

IMPERSONATE_UA = {
    "chrome120": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "chrome123": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "chrome124": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

IMPERSONATE_SEC_CH_UA = {
    "chrome120": '"Chromium";v="120", "Google Chrome";v="120", "Not-A.Brand";v="99"',
    "chrome123": '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
    "chrome124": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
}

# scrapy_impersonate 会将 impersonate_args 传给 AsyncSession.request()，
# 仅支持 default_headers / timeout / verify 等，不能传 curl_options。
IMPERSONATE_ARGS = {
    "default_headers": False,
}


def pick_impersonate():
    return random.choice(IMPERSONATE)


def normalize_impersonate(impersonate):
    """重试或旧请求 meta 里可能是已移除的指纹名，统一映射到当前支持列表。"""
    if impersonate in IMPERSONATE_UA:
        return impersonate
    return pick_impersonate()


def apply_impersonate_headers(headers, impersonate=None):
    impersonate = normalize_impersonate(impersonate) if impersonate else pick_impersonate()
    headers = dict(headers)
    headers["user-agent"] = IMPERSONATE_UA[impersonate]
    headers["sec-ch-ua"] = IMPERSONATE_SEC_CH_UA[impersonate]
    return headers, impersonate


def build_impersonate_meta(base_meta=None, impersonate=None):
    impersonate = normalize_impersonate(impersonate) if impersonate else pick_impersonate()
    meta = {
        "impersonate": impersonate,
        "impersonate_args": dict(IMPERSONATE_ARGS),
    }
    if base_meta:
        meta.update(base_meta)
    return meta


def sync_request_impersonate(request, impersonate=None):
    """保证 request 的 TLS 指纹与 HTTP headers 一致。"""
    if impersonate:
        impersonate = normalize_impersonate(impersonate)
    else:
        impersonate = normalize_impersonate(request.meta.get("impersonate"))
    request.meta["impersonate"] = impersonate
    request.meta["impersonate_args"] = dict(IMPERSONATE_ARGS)
    request.headers[b"User-Agent"] = IMPERSONATE_UA[impersonate].encode()
    request.headers[b"sec-ch-ua"] = IMPERSONATE_SEC_CH_UA[impersonate].encode()
    return impersonate


def is_valid_npm_html(text):
    """过滤 Cloudflare 挑战页等伪 200 响应。"""
    if not text or len(text) < 500:
        return False
    lower = text.lower()
    if "just a moment" in lower or "cf-mitigated" in lower or "challenge-platform" in lower:
        return False
    return "www.npmjs.com" in lower or "package-" in lower or 'id="app"' in lower


def is_valid_go_html(text):
    """过滤 pkg.go.dev 挑战页或空壳响应。"""
    if not text or len(text) < 500:
        return False
    lower = text.lower()
    if "just a moment" in lower or "cf-mitigated" in lower or "challenge-platform" in lower:
        return False
    if "pkg.go.dev" not in lower:
        return False
    return (
        'data-test-id="unitheader-title"' in lower
        or "unitheader-title" in lower
        or "unitdetails" in lower
    )
