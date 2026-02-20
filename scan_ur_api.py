#!/usr/bin/env python3
"""Periodic scanner for UR map_window API with Telegram subscriptions."""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

URL = "https://chintai.r6.ur-net.go.jp/chintai/api/bukken/search/map_window/"
DETAIL_BASE = "https://www.ur-net.go.jp"
DEFAULT_DANCHI_ID = "20_2860"
SUBSCRIBERS_FILE = "telegram_subscribers.json"
MONITOR_IDS_FILE = "monitor_ids.json"
CONFIG_FILE = "scan_config.json"

DEFAULT_CONFIG = {
    "scanner": {
        "interval_seconds": 60,
        "timeout_seconds": 20,
        "daily_start": "00:00",
        "daily_end": "23:59",
        "timezone": "Asia/Tokyo",
    },
    "telegram": {
        "enabled": False,
        "bot_token": "",
        "seed_chat_id": "",
        "send_room_now": False,
        "webhook_enabled": True,
        "webhook_host": "0.0.0.0",
        "webhook_port": 8787,
        "webhook_path": "/telegram/webhook",
        "webhook_secret": "",
    },
    "redis": {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 6379,
        "password": "19940106qc",
        "db": 0,
        "key_prefix": "ur",
    },
}


def now_str() -> str:
    return datetime.now(current_tz()).strftime("%Y-%m-%d %H:%M:%S")


def current_tz() -> ZoneInfo:
    tz_name = os.getenv("APP_TIMEZONE", "Asia/Tokyo")
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Tokyo")


def load_config(path: str) -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if not os.path.exists(path):
        return cfg
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return cfg
        for section in ("scanner", "telegram", "redis"):
            sec_raw = raw.get(section)
            if isinstance(sec_raw, dict):
                cfg[section].update(sec_raw)
    except Exception as e:
        print(f"[{now_str()}] config_load_error={e}")
        sys.stdout.flush()
    return cfg


def parse_hhmm(value: str) -> tuple[int, int]:
    m = re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(value).strip())
    if not m:
        raise ValueError(f"Invalid time format: {value}, expected HH:MM")
    hh, mm = value.split(":")
    return int(hh), int(mm)


def in_active_window(now: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    sh, sm = parse_hhmm(start_hhmm)
    eh, em = parse_hhmm(end_hhmm)
    start_minutes = sh * 60 + sm
    end_minutes = eh * 60 + em
    now_minutes = now.hour * 60 + now.minute
    if start_minutes == end_minutes:
        return True
    if start_minutes < end_minutes:
        return start_minutes <= now_minutes < end_minutes
    return now_minutes >= start_minutes or now_minutes < end_minutes


def seconds_until_window_start(now: datetime, start_hhmm: str, end_hhmm: str) -> int:
    if in_active_window(now, start_hhmm, end_hhmm):
        return 0
    sh, sm = parse_hhmm(start_hhmm)
    start_today = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if now < start_today:
        return int((start_today - now).total_seconds())
    start_tomorrow = start_today + timedelta(days=1)
    return int((start_tomorrow - now).total_seconds())


def _extract_update_chat_and_text(update: dict) -> tuple[str | None, str]:
    message = update.get("message", {})
    chat = (
        message.get("chat")
        or update.get("edited_message", {}).get("chat")
        or update.get("channel_post", {}).get("chat")
        or update.get("my_chat_member", {}).get("chat")
        or {}
    )
    cid = chat.get("id")
    text = str(message.get("text", "")).strip()
    return (str(cid) if cid is not None else None), text


async def start_telegram_webhook_server(
    host: str,
    port: int,
    path: str,
    queue: "asyncio.Queue[dict]",
    secret: str,
) -> asyncio.AbstractServer:
    async def _send_response(writer: asyncio.StreamWriter, code: int, body: str) -> None:
        phrase = "OK" if code == 200 else "Bad Request"
        body_bytes = body.encode("utf-8")
        writer.write(
            (
                f"HTTP/1.1 {code} {phrase}\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                f"Content-Length: {len(body_bytes)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode("utf-8")
        )
        writer.write(body_bytes)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            lines = head.decode("latin1", errors="replace").split("\r\n")
            req = lines[0].split()
            if len(req) < 2:
                await _send_response(writer, 400, "bad request")
                return
            method, req_path = req[0], req[1]
            headers: dict[str, str] = {}
            for ln in lines[1:]:
                if not ln:
                    continue
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if method.upper() != "POST" or req_path != path:
                await _send_response(writer, 400, "bad request")
                return
            if secret:
                got = headers.get("x-telegram-bot-api-secret-token", "")
                if got != secret:
                    await _send_response(writer, 400, "bad secret")
                    return

            length = int(headers.get("content-length", "0") or "0")
            if length < 0 or length > 2_000_000:
                await _send_response(writer, 400, "bad content-length")
                return
            body = await asyncio.wait_for(reader.readexactly(length), timeout=10) if length else b"{}"
            update = json.loads(body.decode("utf-8", errors="replace"))
            if isinstance(update, dict):
                await queue.put(update)
            await _send_response(writer, 200, "ok")
        except Exception:
            try:
                await _send_response(writer, 400, "bad request")
            except Exception:
                pass

    return await asyncio.start_server(_handle, host=host, port=port)


def request_once(danchi_id: str, timeout: int) -> tuple[int | None, str]:
    data = json.dumps({"id": danchi_id}).encode("utf-8")
    req = urllib.request.Request(
        URL,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "ur-sprider/1.0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
        return e.code, body
    except urllib.error.URLError as e:
        return None, f"URLError: {e}"
    except socket.timeout:
        return None, "socket.timeout"
    except TimeoutError:
        return None, "TimeoutError"
    except Exception as e:
        return None, f"RequestError: {e}"


def send_telegram_message(
    token: str,
    chat_id: str,
    text: str,
    timeout: int,
    parse_mode: str | None = None,
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
        payload["disable_web_page_preview"] = "true"
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=timeout):
        return


def _redis_encode(parts: list[str]) -> bytes:
    out = f"*{len(parts)}\r\n".encode("utf-8")
    for part in parts:
        b = str(part).encode("utf-8")
        out += f"${len(b)}\r\n".encode("utf-8") + b + b"\r\n"
    return out


def _redis_read_response(rf) -> object:
    prefix = rf.read(1)
    if not prefix:
        raise RuntimeError("Redis connection closed")
    line = rf.readline().rstrip(b"\r\n")
    if prefix == b"+":
        return line.decode("utf-8", errors="replace")
    if prefix == b"-":
        raise RuntimeError(line.decode("utf-8", errors="replace"))
    if prefix == b":":
        return int(line)
    if prefix == b"$":
        length = int(line)
        if length < 0:
            return None
        data = rf.read(length)
        rf.read(2)
        return data.decode("utf-8", errors="replace")
    if prefix == b"*":
        n = int(line)
        if n < 0:
            return None
        return [_redis_read_response(rf) for _ in range(n)]
    raise RuntimeError(f"Unknown Redis RESP prefix: {prefix!r}")


def redis_command(
    host: str,
    port: int,
    password: str,
    db: int,
    timeout: int,
    command: list[str],
) -> object:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        rf = sock.makefile("rb")
        if password:
            sock.sendall(_redis_encode(["AUTH", password]))
            _redis_read_response(rf)
        if db != 0:
            sock.sendall(_redis_encode(["SELECT", str(db)]))
            _redis_read_response(rf)
        sock.sendall(_redis_encode(command))
        return _redis_read_response(rf)


def store_snapshot_to_redis(
    host: str,
    port: int,
    password: str,
    db: int,
    key_prefix: str,
    timeout: int,
    danchi_id: str,
    status: int | None,
    danchi_name: str,
    address: str,
    map_url: str,
    room_count: int,
    room_preview: str,
    danchi_url: str,
) -> None:
    payload = {
        "timestamp": now_str(),
        "danchi_id": danchi_id,
        "danchi_name": danchi_name,
        "status": status,
        "address": address,
        "map_url": map_url,
        "room_count": room_count,
        "room_preview": room_preview,
        "danchi_url": danchi_url,
    }
    key = f"{key_prefix}:latest:{danchi_id}"
    redis_command(
        host=host,
        port=port,
        password=password,
        db=db,
        timeout=timeout,
        command=["SET", key, json.dumps(payload, ensure_ascii=False)],
    )


def monitor_ids_redis_key(key_prefix: str) -> str:
    return f"{key_prefix}:monitor_ids"


def monitor_ids_init_key(key_prefix: str) -> str:
    return f"{key_prefix}:monitor_ids_initialized"


def default_danchi_url(danchi_id: str) -> str:
    return f"https://www.ur-net.go.jp/chintai/kanto/tokyo/{danchi_id}.html"


def extract_danchi_name_and_url(body: str, danchi_id: str) -> tuple[str, str]:
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return danchi_id, default_danchi_url(danchi_id)
    name = str(obj.get("name", "")).strip() or danchi_id
    link = str(obj.get("link", "")).strip()
    url = urllib.parse.urljoin(DETAIL_BASE, link) if link else default_danchi_url(danchi_id)
    return name, url


def get_danchi_meta_for_list(
    danchi_id: str,
    timeout: int,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
) -> tuple[str, str]:
    if redis_enabled:
        try:
            key = f"{redis_key_prefix}:latest:{danchi_id}"
            raw = redis_command(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                timeout=timeout,
                command=["GET", key],
            )
            if isinstance(raw, str) and raw:
                obj = json.loads(raw)
                name = str(obj.get("danchi_name", "")).strip() or danchi_id
                url = str(obj.get("danchi_url", "")).strip() or default_danchi_url(danchi_id)
                return name, url
        except Exception:
            pass
    status, body = request_once(danchi_id=danchi_id, timeout=timeout)
    if status is None:
        return danchi_id, default_danchi_url(danchi_id)
    return extract_danchi_name_and_url(body, danchi_id)


def get_latest_snapshot(
    danchi_id: str,
    timeout: int,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
) -> dict | None:
    if not redis_enabled:
        return None
    try:
        key = f"{redis_key_prefix}:latest:{danchi_id}"
        raw = redis_command(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            timeout=timeout,
            command=["GET", key],
        )
        if isinstance(raw, str) and raw:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
    except Exception:
        return None
    return None


def load_subscribers(path: str) -> tuple[int, set[str], set[str]]:
    if not os.path.exists(path):
        return 0, set(), set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        last_update_id = int(data.get("last_update_id", 0))
        chat_ids = {str(x) for x in data.get("chat_ids", []) if str(x).strip()}
        pending_add = {str(x) for x in data.get("pending_add", []) if str(x).strip()}
        return last_update_id, chat_ids, pending_add
    except Exception:
        return 0, set(), set()


def save_subscribers(path: str, last_update_id: int, chat_ids: set[str], pending_add: set[str]) -> None:
    data = {
        "last_update_id": last_update_id,
        "chat_ids": sorted(chat_ids),
        "pending_add": sorted(pending_add),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_monitor_ids_file(path: str) -> set[str]:
    if not os.path.exists(path):
        return {DEFAULT_DANCHI_ID}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ids = {str(x) for x in data.get("ids", []) if str(x).strip()}
        return ids
    except Exception:
        return {DEFAULT_DANCHI_ID}


def save_monitor_ids_file(path: str, monitor_ids: set[str]) -> None:
    data = {"ids": sorted(monitor_ids)}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_monitor_ids(
    path: str,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
    timeout: int,
) -> set[str]:
    if redis_enabled:
        try:
            key = monitor_ids_redis_key(redis_key_prefix)
            init_key = monitor_ids_init_key(redis_key_prefix)
            init_exists = redis_command(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                timeout=timeout,
                command=["EXISTS", init_key],
            )
            resp = redis_command(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                timeout=timeout,
                command=["SMEMBERS", key],
            )
            ids = {str(x).strip() for x in (resp or []) if str(x).strip()}
            if int(init_exists or 0) == 0:
                redis_command(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    db=redis_db,
                    timeout=timeout,
                    command=["SADD", key, DEFAULT_DANCHI_ID],
                )
                redis_command(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    db=redis_db,
                    timeout=timeout,
                    command=["SET", init_key, "1"],
                )
                ids = {DEFAULT_DANCHI_ID}
            return ids
        except Exception as e:
            print(f"[{now_str()}] redis_monitor_load_error={e}")
            sys.stdout.flush()
    return load_monitor_ids_file(path)


def add_monitor_id(
    monitor_ids: set[str],
    danchi_id: str,
    path: str,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
    timeout: int,
) -> tuple[set[str], bool]:
    added = danchi_id not in monitor_ids
    monitor_ids.add(danchi_id)
    if redis_enabled:
        key = monitor_ids_redis_key(redis_key_prefix)
        init_key = monitor_ids_init_key(redis_key_prefix)
        redis_command(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            timeout=timeout,
            command=["SADD", key, danchi_id],
        )
        redis_command(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            timeout=timeout,
            command=["SET", init_key, "1"],
        )
    else:
        save_monitor_ids_file(path, monitor_ids)
    return monitor_ids, added


def clear_monitor_ids(
    path: str,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
    timeout: int,
) -> None:
    if redis_enabled:
        key = monitor_ids_redis_key(redis_key_prefix)
        init_key = monitor_ids_init_key(redis_key_prefix)
        redis_command(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            timeout=timeout,
            command=["DEL", key],
        )
        redis_command(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            timeout=timeout,
            command=["SET", init_key, "1"],
        )
    else:
        save_monitor_ids_file(path, set())


def extract_danchi_id(text: str) -> str | None:
    s = text.strip()
    m = re.search(r"/(\d+_\d+)(?:_room)?\.html", s)
    if m:
        return m.group(1)
    m = re.search(r"\b\d+_\d+\b", s)
    if m:
        return m.group(0)
    return None


def handle_bot_command(
    token: str,
    timeout: int,
    chat_id: str,
    text: str,
    monitor_ids: set[str],
    monitor_path: str,
    pending_add: set[str],
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
) -> tuple[set[str], set[str]]:
    msg = text.strip()
    if chat_id in pending_add and not msg.startswith("/"):
        danchi_id = extract_danchi_id(msg)
        if not danchi_id:
            reply = "链接格式不对，请发送类似: https://www.ur-net.go.jp/chintai/kanto/tokyo/20_7200.html"
            send_telegram_message(token, chat_id, reply, timeout=timeout)
            return monitor_ids, pending_add
        monitor_ids, added = add_monitor_id(
            monitor_ids=monitor_ids,
            danchi_id=danchi_id,
            path=monitor_path,
            redis_enabled=redis_enabled,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            redis_db=redis_db,
            redis_key_prefix=redis_key_prefix,
            timeout=timeout,
        )
        pending_add.discard(chat_id)
        if added:
            reply = f"已添加监控团地: {danchi_id}"
        else:
            reply = f"团地已在监控列表: {danchi_id}"
        send_telegram_message(token, chat_id, reply, timeout=timeout)
        return monitor_ids, pending_add

    if not msg.startswith("/"):
        return monitor_ids, pending_add

    if msg.startswith("/add"):
        arg = msg.split(maxsplit=1)[1].strip() if len(msg.split(maxsplit=1)) > 1 else ""
        if not arg:
            pending_add.add(chat_id)
            reply = "请发要订阅的团地链接，例如: https://www.ur-net.go.jp/chintai/kanto/tokyo/20_7200.html"
            send_telegram_message(token, chat_id, reply, timeout=timeout)
            return monitor_ids, pending_add
        danchi_id = extract_danchi_id(arg)
        if not danchi_id:
            reply = "用法: /add https://www.ur-net.go.jp/chintai/kanto/tokyo/20_7200.html"
            send_telegram_message(token, chat_id, reply, timeout=timeout)
            return monitor_ids, pending_add

        monitor_ids, added = add_monitor_id(
            monitor_ids=monitor_ids,
            danchi_id=danchi_id,
            path=monitor_path,
            redis_enabled=redis_enabled,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            redis_db=redis_db,
            redis_key_prefix=redis_key_prefix,
            timeout=timeout,
        )
        if added:
            reply = f"已添加监控团地: {danchi_id}"
        else:
            reply = f"团地已在监控列表: {danchi_id}"
        send_telegram_message(token, chat_id, reply, timeout=timeout)
        return monitor_ids, pending_add

    if msg.startswith("/list"):
        lines: list[str] = []
        for mid in sorted(monitor_ids):
            name, url = get_danchi_meta_for_list(
                danchi_id=mid,
                timeout=timeout,
                redis_enabled=redis_enabled,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_password=redis_password,
                redis_db=redis_db,
                redis_key_prefix=redis_key_prefix,
            )
            lines.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(name)}</a>')
        reply = "\n".join(lines) if lines else "(空)"
        send_telegram_message(token, chat_id, reply, timeout=timeout, parse_mode="HTML")
        return monitor_ids, pending_add

    if msg.startswith("/clear"):
        monitor_ids = set()
        clear_monitor_ids(
            path=monitor_path,
            redis_enabled=redis_enabled,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            redis_db=redis_db,
            redis_key_prefix=redis_key_prefix,
            timeout=timeout,
        )
        pending_add.discard(chat_id)
        send_telegram_message(token, chat_id, "已清空所有扫描任务", timeout=timeout)
        return monitor_ids, pending_add

    if msg.startswith("/log"):
        lines: list[str] = []
        for mid in sorted(monitor_ids):
            snap = get_latest_snapshot(
                danchi_id=mid,
                timeout=timeout,
                redis_enabled=redis_enabled,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_password=redis_password,
                redis_db=redis_db,
                redis_key_prefix=redis_key_prefix,
            )
            if not snap:
                lines.append(f"- {mid} | 暂无记录")
                continue
            name = str(snap.get("danchi_name", "")).strip() or mid
            ts = str(snap.get("timestamp", "-")).strip() or "-"
            rc = snap.get("room_count", "-")
            st = snap.get("status", "-")
            lines.append(f"- {name} | 请求时间: {ts} | 状态: {st} | 房源数: {rc}")
        reply = "最近扫描结果:\n" + ("\n".join(lines) if lines else "(空)")
        send_telegram_message(token, chat_id, reply, timeout=timeout)
        return monitor_ids, pending_add

    if msg.startswith("/help") or msg.startswith("/start"):
        reply = (
            "可用命令:\n"
            "/help  显示命令帮助\n"
            "/add  添加监控（先进入等待链接模式）\n"
            "/add <ur链接或团地ID>  直接添加监控\n"
            "/list  查看当前监控团地\n"
            "/clear  清空所有扫描任务\n"
            "/log  查看每个团地最后一次扫描结果\n"
            "示例:\n"
            "/add https://www.ur-net.go.jp/chintai/kanto/tokyo/20_7200.html"
        )
        send_telegram_message(token, chat_id, reply, timeout=timeout)
        return monitor_ids, pending_add

    return monitor_ids, pending_add


def sync_subscribers(
    token: str,
    timeout: int,
    state_path: str,
    monitor_ids: set[str],
    monitor_path: str,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
) -> tuple[int, set[str], set[str], set[str]]:
    last_update_id, chat_ids, pending_add = load_subscribers(state_path)
    offset = last_update_id + 1 if last_update_id > 0 else 0
    url = f"https://api.telegram.org/bot{token}/getUpdates?timeout=0&offset={offset}"
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    data = json.loads(body)
    if not data.get("ok"):
        return last_update_id, chat_ids, monitor_ids, pending_add

    changed = False
    for upd in data.get("result", []):
        upd_id = int(upd.get("update_id", 0))
        if upd_id > last_update_id:
            last_update_id = upd_id
            changed = True

        message = upd.get("message", {})
        chat = (
            message.get("chat")
            or upd.get("edited_message", {}).get("chat")
            or upd.get("channel_post", {}).get("chat")
            or upd.get("my_chat_member", {}).get("chat")
            or {}
        )
        cid = chat.get("id")
        if cid is not None:
            cid_str = str(cid)
            if cid_str not in chat_ids:
                chat_ids.add(cid_str)
                changed = True

        text = str(message.get("text", "")).strip()
        if text and cid is not None:
            try:
                monitor_ids, pending_add = handle_bot_command(
                    token=token,
                    timeout=timeout,
                    chat_id=str(cid),
                    text=text,
                    monitor_ids=monitor_ids,
                    monitor_path=monitor_path,
                    pending_add=pending_add,
                    redis_enabled=redis_enabled,
                    redis_host=redis_host,
                    redis_port=redis_port,
                    redis_password=redis_password,
                    redis_db=redis_db,
                    redis_key_prefix=redis_key_prefix,
                )
                changed = True
            except Exception as e:
                print(f"[{now_str()}] telegram_command_error={e}")
                sys.stdout.flush()

    if changed:
        save_subscribers(state_path, last_update_id, chat_ids, pending_add)
    return last_update_id, chat_ids, monitor_ids, pending_add


def broadcast_telegram(token: str, chat_ids: set[str], text: str, timeout: int) -> None:
    for cid in sorted(chat_ids):
        try:
            send_telegram_message(token, cid, text, timeout=timeout)
            print(f"[{now_str()}] telegram_sent chat_id={cid}")
        except Exception as e:
            print(f"[{now_str()}] telegram_error chat_id={cid} err={e}")
        sys.stdout.flush()


def format_room_preview(room: object, limit: int = 8) -> tuple[str, int]:
    if not isinstance(room, list):
        raw = json.dumps(room, ensure_ascii=False)[:300]
        return f"room(raw)={raw}", 0

    count = len(room)
    if count == 0:
        return "暂无可租房源", 0

    lines: list[str] = []
    for item in room[:limit]:
        if not isinstance(item, dict):
            lines.append(f"- {json.dumps(item, ensure_ascii=False)[:120]}")
            continue
        room_name = str(item.get("name", "")).strip() or "未知房源"
        rent = str(item.get("rent", "")).strip() or "-"
        room_type = str(item.get("type", "")).strip() or "-"
        floor = str(item.get("floor", "")).strip() or "-"
        floor_space = str(item.get("floorspace", "")).strip() or "-"
        link = str(item.get("link", "")).strip()
        detail_url = urllib.parse.urljoin(DETAIL_BASE, link) if link else "-"
        lines.append(f"- {room_name} | {rent} | {room_type} | {floor_space} | {floor} | {detail_url}")

    if count > limit:
        lines.append(f"... 其余 {count - limit} 套")
    return "\n".join(lines), count


def extract_room_snapshot(body: str) -> tuple[bool, str, str, str, int]:
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return False, "", "", "", 0

    danchi_name = str(obj.get("name", "")).strip() or "未知团地"
    room = obj.get("room", "__MISSING__")
    room_preview, room_count = format_room_preview(room)
    room_signature = json.dumps(room, ensure_ascii=False, sort_keys=True)
    return True, danchi_name, room_signature, room_preview, room_count


def extract_location_info(body: str) -> tuple[str, str]:
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return "-", "-"

    address = str(obj.get("address", "")).strip() or "-"
    lat = obj.get("lat")
    lng = obj.get("lng")
    if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
        q = urllib.parse.quote_plus(f"{lat},{lng}")
        map_url = f"https://www.google.com/maps/search/?api=1&query={q}"
    else:
        map_url = "-"
    return address, map_url


async def run_async(
    interval: int,
    timeout: int,
    once: bool,
    telegram: bool,
    send_room_now: bool,
    daily_start: str,
    daily_end: str,
    redis_enabled: bool,
    redis_host: str,
    redis_port: int,
    redis_password: str,
    redis_db: int,
    redis_key_prefix: str,
) -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    seed_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if telegram and not bot_token:
        raise SystemExit("Telegram enabled but TELEGRAM_BOT_TOKEN is missing")
    if redis_enabled:
        try:
            await asyncio.to_thread(
                redis_command,
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                timeout=timeout,
                command=["PING"],
            )
            print(f"[{now_str()}] redis_connected host={redis_host} port={redis_port} db={redis_db}")
            sys.stdout.flush()
        except Exception as e:
            err = str(e)
            if redis_password and "without any password configured" in err:
                try:
                    redis_password = ""
                    await asyncio.to_thread(
                        redis_command,
                        host=redis_host,
                        port=redis_port,
                        password=redis_password,
                        db=redis_db,
                        timeout=timeout,
                        command=["PING"],
                    )
                    print(
                        f"[{now_str()}] redis_connected_without_password "
                        f"host={redis_host} port={redis_port} db={redis_db}"
                    )
                    sys.stdout.flush()
                except Exception as e2:
                    print(f"[{now_str()}] redis_connect_error={e2}")
                    sys.stdout.flush()
                    redis_enabled = False
            else:
                print(f"[{now_str()}] redis_connect_error={e}")
                sys.stdout.flush()
                redis_enabled = False

    subscribers_path = os.path.join(os.getcwd(), SUBSCRIBERS_FILE)
    monitor_path = os.path.join(os.getcwd(), MONITOR_IDS_FILE)

    chat_ids: set[str] = set()
    pending_add: set[str] = set()
    monitor_ids = await asyncio.to_thread(
        load_monitor_ids,
        path=monitor_path,
        redis_enabled=redis_enabled,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        redis_db=redis_db,
        redis_key_prefix=redis_key_prefix,
        timeout=timeout,
    )
    webhook_enabled = bool(os.getenv("TELEGRAM_WEBHOOK_ENABLED", "1") not in ("0", "false", "False"))
    webhook_host = os.getenv("TELEGRAM_WEBHOOK_HOST", "0.0.0.0")
    webhook_port = int(os.getenv("TELEGRAM_WEBHOOK_PORT", "8787"))
    webhook_path = os.getenv("TELEGRAM_WEBHOOK_PATH", "/telegram/webhook")
    webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    webhook_queue: asyncio.Queue[dict] | None = None
    webhook_server: asyncio.AbstractServer | None = None
    state_lock = asyncio.Lock()

    async def _webhook_consumer() -> None:
        nonlocal chat_ids, monitor_ids, pending_add
        if webhook_queue is None:
            return
        while True:
            update = await webhook_queue.get()
            cid, text = _extract_update_chat_and_text(update)
            changed = False
            async with state_lock:
                if cid is not None and cid not in chat_ids:
                    chat_ids.add(cid)
                    changed = True
            if cid is not None and text:
                try:
                    async with state_lock:
                        monitor_ids, pending_add = await asyncio.to_thread(
                            handle_bot_command,
                            token=bot_token,
                            timeout=timeout,
                            chat_id=cid,
                            text=text,
                            monitor_ids=monitor_ids,
                            monitor_path=monitor_path,
                            pending_add=pending_add,
                            redis_enabled=redis_enabled,
                            redis_host=redis_host,
                            redis_port=redis_port,
                            redis_password=redis_password,
                            redis_db=redis_db,
                            redis_key_prefix=redis_key_prefix,
                        )
                        changed = True
                except Exception as e:
                    print(f"[{now_str()}] telegram_command_error={e}")
                    sys.stdout.flush()
            if changed:
                last_update_id, _, _ = load_subscribers(subscribers_path)
                save_subscribers(subscribers_path, last_update_id, chat_ids, pending_add)
    if telegram:
        last_update_id, chat_ids, pending_add = load_subscribers(subscribers_path)
        if seed_chat_id:
            chat_ids.add(seed_chat_id)
        save_subscribers(subscribers_path, last_update_id, chat_ids, pending_add)
        if not redis_enabled:
            save_monitor_ids_file(monitor_path, monitor_ids)
        if webhook_enabled:
            webhook_queue = asyncio.Queue()
            webhook_server = await start_telegram_webhook_server(
                host=webhook_host,
                port=webhook_port,
                path=webhook_path,
                queue=webhook_queue,
                secret=webhook_secret,
            )
            print(
                f"[{now_str()}] telegram_webhook_listening http://{webhook_host}:{webhook_port}{webhook_path}"
            )
            sys.stdout.flush()
            webhook_consumer_task = asyncio.create_task(_webhook_consumer())
        else:
            print(f"[{now_str()}] telegram_webhook_disabled commands_unavailable")
            sys.stdout.flush()
            webhook_consumer_task = None
    else:
        webhook_consumer_task = None

    last_room_signature_by_id: dict[str, str] = {}
    last_room_count_by_id: dict[str, int] = {}

    while True:
        now = datetime.now(current_tz())
        if not in_active_window(now, daily_start, daily_end):
            wait_sec = seconds_until_window_start(now, daily_start, daily_end)
            sleep_sec = max(1, min(wait_sec, 60))
            print(
                f"[{now_str()}] out_of_window {daily_start}-{daily_end}, "
                f"sleep={sleep_sec}s"
            )
            sys.stdout.flush()
            if once:
                return
            await asyncio.sleep(sleep_sec)
            continue

        async with state_lock:
            monitor_ids_snapshot = sorted(monitor_ids)
            chat_ids_snapshot = set(chat_ids)

        for danchi_id in monitor_ids_snapshot:
            status, body = await asyncio.to_thread(request_once, danchi_id=danchi_id, timeout=timeout)
            short_body = body[:500].replace("\n", " ")
            address, map_url = extract_location_info(body)
            room_ok, danchi_name, room_signature, room_preview, room_count = extract_room_snapshot(body)
            _, danchi_url = extract_danchi_name_and_url(body, danchi_id)

            if room_ok:
                print(
                    f"[{now_str()}] id={danchi_id} 团地={danchi_name} status={status} "
                    f"room_count={room_count} address={address} map={map_url}"
                )
            else:
                print(f"[{now_str()}] id={danchi_id} status={status} body={short_body}")
            sys.stdout.flush()

            if not room_ok:
                continue

            if redis_enabled:
                try:
                    await asyncio.to_thread(
                        store_snapshot_to_redis,
                        host=redis_host,
                        port=redis_port,
                        password=redis_password,
                        db=redis_db,
                        key_prefix=redis_key_prefix,
                        timeout=timeout,
                        danchi_id=danchi_id,
                        status=status,
                        danchi_name=danchi_name,
                        address=address,
                        map_url=map_url,
                        room_count=room_count,
                        room_preview=room_preview,
                        danchi_url=danchi_url,
                    )
                    print(f"[{now_str()}] redis_saved key={redis_key_prefix}:latest:{danchi_id}")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"[{now_str()}] redis_save_error id={danchi_id} err={e}")
                    sys.stdout.flush()

            if danchi_id not in last_room_signature_by_id:
                last_room_signature_by_id[danchi_id] = room_signature
                last_room_count_by_id[danchi_id] = room_count
                print(f"[{now_str()}] 团地={danchi_name} room_baseline={room_preview}")
                sys.stdout.flush()
                if telegram and send_room_now:
                    if room_count > 0:
                        init_text = (
                            f"UR 房源快照\n"
                            f"时间: {now_str()}\n"
                            f"团地: {danchi_name}\n"
                            f"ID: {danchi_id}\n"
                            f"地址: {address}\n"
                            f"Google Maps: {map_url}\n"
                            f"房源数: {room_count}\n"
                            f"{room_preview}"
                        )
                        await asyncio.to_thread(
                            broadcast_telegram, bot_token, chat_ids_snapshot, init_text, timeout=timeout
                        )
                continue

            if room_signature != last_room_signature_by_id[danchi_id]:
                old_count = last_room_count_by_id.get(danchi_id, 0)
                change_text = (
                    f"UR 房源变更\n"
                    f"时间: {now_str()}\n"
                    f"团地: {danchi_name}\n"
                    f"ID: {danchi_id}\n"
                    f"地址: {address}\n"
                    f"Google Maps: {map_url}\n"
                    f"房源数变化: {old_count} -> {room_count}\n"
                    f"当前房源:\n{room_preview}"
                )
                print(change_text)
                sys.stdout.flush()
                if telegram and room_count > 0:
                    await asyncio.to_thread(
                        broadcast_telegram, bot_token, chat_ids_snapshot, change_text, timeout=timeout
                    )
                last_room_signature_by_id[danchi_id] = room_signature
                last_room_count_by_id[danchi_id] = room_count

        if once:
            if webhook_consumer_task is not None:
                webhook_consumer_task.cancel()
                try:
                    await webhook_consumer_task
                except asyncio.CancelledError:
                    pass
            if webhook_server is not None:
                webhook_server.close()
                await webhook_server.wait_closed()
            return
        await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Periodically scan UR map_window API.")
    parser.add_argument("--config", default=CONFIG_FILE, help="Config file path")
    parser.add_argument("--interval", type=int, default=None, help="Scan interval in seconds")
    parser.add_argument("--timeout", type=int, default=None, help="Request timeout in seconds")
    parser.add_argument("--daily-start", default=None, help="Daily scan start time, HH:MM")
    parser.add_argument("--daily-end", default=None, help="Daily scan end time, HH:MM")
    parser.add_argument("--once", action="store_true", help="Run only once and exit")
    parser.add_argument(
        "--telegram",
        action="store_true",
        default=None,
        help="Enable room change notifications to Telegram",
    )
    parser.add_argument("--webhook-host", default=None, help="Telegram webhook bind host")
    parser.add_argument("--webhook-port", type=int, default=None, help="Telegram webhook bind port")
    parser.add_argument("--webhook-path", default=None, help="Telegram webhook request path")
    parser.add_argument("--webhook-secret", default=None, help="Telegram webhook secret token")
    parser.add_argument(
        "--send-room-now",
        action="store_true",
        default=None,
        help="When used with --telegram, send current room snapshot immediately",
    )
    parser.add_argument("--disable-redis", action="store_true", default=None, help="Disable Redis storage")
    parser.add_argument("--redis-host", default=None, help="Redis host")
    parser.add_argument("--redis-port", type=int, default=None, help="Redis port")
    parser.add_argument(
        "--redis-password",
        default=None,
        help="Redis password",
    )
    parser.add_argument("--redis-db", type=int, default=None, help="Redis db index")
    parser.add_argument(
        "--redis-key-prefix",
        default=None,
        help="Redis key prefix",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    scanner_cfg = cfg["scanner"]
    telegram_cfg = cfg["telegram"]
    redis_cfg = cfg["redis"]

    interval = int(args.interval if args.interval is not None else scanner_cfg.get("interval_seconds", 60))
    timeout = int(args.timeout if args.timeout is not None else scanner_cfg.get("timeout_seconds", 20))
    daily_start = str(args.daily_start if args.daily_start is not None else scanner_cfg.get("daily_start", "00:00"))
    daily_end = str(args.daily_end if args.daily_end is not None else scanner_cfg.get("daily_end", "23:59"))
    timezone_name = str(scanner_cfg.get("timezone", "Asia/Tokyo"))

    telegram_enabled = (
        bool(args.telegram)
        if args.telegram is not None
        else bool(telegram_cfg.get("enabled", False))
    )
    webhook_enabled = bool(telegram_cfg.get("webhook_enabled", True))
    webhook_host = str(args.webhook_host if args.webhook_host is not None else telegram_cfg.get("webhook_host", "0.0.0.0"))
    webhook_port = int(args.webhook_port if args.webhook_port is not None else telegram_cfg.get("webhook_port", 8787))
    webhook_path = str(args.webhook_path if args.webhook_path is not None else telegram_cfg.get("webhook_path", "/telegram/webhook"))
    webhook_secret = str(args.webhook_secret if args.webhook_secret is not None else telegram_cfg.get("webhook_secret", ""))
    send_room_now = (
        bool(args.send_room_now)
        if args.send_room_now is not None
        else bool(telegram_cfg.get("send_room_now", False))
    )

    redis_enabled = (
        (not bool(args.disable_redis))
        if args.disable_redis is not None
        else bool(redis_cfg.get("enabled", True))
    )
    redis_host = str(args.redis_host if args.redis_host is not None else redis_cfg.get("host", "127.0.0.1"))
    redis_port = int(args.redis_port if args.redis_port is not None else redis_cfg.get("port", 6379))
    redis_password = str(
        args.redis_password if args.redis_password is not None else redis_cfg.get("password", "19940106qc")
    )
    redis_db = int(args.redis_db if args.redis_db is not None else redis_cfg.get("db", 0))
    redis_key_prefix = str(
        args.redis_key_prefix if args.redis_key_prefix is not None else redis_cfg.get("key_prefix", "ur")
    )

    bot_token = str(telegram_cfg.get("bot_token", "")).strip()
    seed_chat_id = str(telegram_cfg.get("seed_chat_id", "")).strip()
    if bot_token and not os.getenv("TELEGRAM_BOT_TOKEN"):
        os.environ["TELEGRAM_BOT_TOKEN"] = bot_token
    if seed_chat_id and not os.getenv("TELEGRAM_CHAT_ID"):
        os.environ["TELEGRAM_CHAT_ID"] = seed_chat_id
    os.environ["TELEGRAM_WEBHOOK_ENABLED"] = "1" if webhook_enabled else "0"
    os.environ["TELEGRAM_WEBHOOK_HOST"] = webhook_host
    os.environ["TELEGRAM_WEBHOOK_PORT"] = str(webhook_port)
    os.environ["TELEGRAM_WEBHOOK_PATH"] = webhook_path
    os.environ["TELEGRAM_WEBHOOK_SECRET"] = webhook_secret
    if redis_host and not os.getenv("REDIS_HOST"):
        os.environ["REDIS_HOST"] = redis_host
    if str(redis_port) and not os.getenv("REDIS_PORT"):
        os.environ["REDIS_PORT"] = str(redis_port)
    if redis_password and not os.getenv("REDIS_PASSWORD"):
        os.environ["REDIS_PASSWORD"] = redis_password
    if str(redis_db) and not os.getenv("REDIS_DB"):
        os.environ["REDIS_DB"] = str(redis_db)
    if redis_key_prefix and not os.getenv("REDIS_KEY_PREFIX"):
        os.environ["REDIS_KEY_PREFIX"] = redis_key_prefix
    os.environ["APP_TIMEZONE"] = timezone_name

    if interval <= 0:
        raise SystemExit("--interval must be > 0")
    if timeout <= 0:
        raise SystemExit("--timeout must be > 0")
    parse_hhmm(daily_start)
    parse_hhmm(daily_end)

    asyncio.run(
        run_async(
            interval=interval,
            timeout=timeout,
            once=args.once,
            telegram=telegram_enabled,
            send_room_now=send_room_now,
            daily_start=daily_start,
            daily_end=daily_end,
            redis_enabled=redis_enabled,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            redis_db=redis_db,
            redis_key_prefix=redis_key_prefix,
        )
    )


if __name__ == "__main__":
    main()
