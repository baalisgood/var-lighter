"""
将浏览器 DevTools 的 “Copy as cURL” 输出转换为 Variational 下单模板配置片段。

用途：
- Variational 无公开 API，只能逆向前端
- 你在浏览器里下单一次，复制 cURL
- 把 cURL 粘贴到文件中（例如 tools/variational_order.curl.txt）
- 运行本脚本输出可直接塞进 config.extra_params.trade.order_create_template 的 JSON

注意：
- 本脚本不做字段猜测/自动占位符识别
- 你需要手动把 body 中的 qty/side/price 等字段改成 {{qty}}/{{side}}/{{price}}
"""

from __future__ import annotations

import argparse
import json
import shlex
from typing import Any, Dict, Tuple, Optional
from urllib.parse import urlparse


def _parse_curl(curl_text: str) -> Tuple[str, str, Dict[str, str], Optional[str]]:
    tokens = shlex.split(curl_text.strip())
    if not tokens or tokens[0] != "curl":
        raise ValueError("输入不是 curl 命令（请粘贴 DevTools 的 Copy as cURL 输出）")

    method = "GET"
    url = ""
    headers: Dict[str, str] = {}
    data_raw: Optional[str] = None

    i = 1
    while i < len(tokens):
        t = tokens[i]
        if t in ("-X", "--request"):
            method = tokens[i + 1].upper()
            i += 2
            continue
        if t in ("-H", "--header"):
            kv = tokens[i + 1]
            if ":" in kv:
                k, v = kv.split(":", 1)
                headers[k.strip()] = v.strip()
            i += 2
            continue
        if t in ("--data-raw", "--data", "--data-binary", "-d"):
            data_raw = tokens[i + 1]
            i += 2
            continue
        if t.startswith("http://") or t.startswith("https://"):
            url = t
            i += 1
            continue
        i += 1

    if not url:
        raise ValueError("curl 中未找到 URL")

    return method, url, headers, data_raw


def _extract_cookies(headers: Dict[str, str]) -> Dict[str, str]:
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    if not cookie_header:
        return {}
    cookies: Dict[str, str] = {}
    parts = [p.strip() for p in cookie_header.split(";") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            cookies[k.strip()] = v.strip()
    # 下发 cookies 后，header 中移除 Cookie，避免重复
    headers.pop("cookie", None)
    headers.pop("Cookie", None)
    return cookies


def _try_json(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="包含 curl 命令的文本文件路径")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        curl_text = f.read()

    method, url, headers, data_raw = _parse_curl(curl_text)
    cookies = _extract_cookies(headers)
    body = _try_json(data_raw)

    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path
    if parsed.query:
        path = f"{path}?{parsed.query}"

    template = {
        "base_url_suggestion": base_url,
        "order_create_template": {
            "method": method,
            "url": path,
            "headers": headers,
            "cookies": cookies,
            "body": body,
            "timeout_seconds": 10,
        },
    }
    print(json.dumps(template, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


