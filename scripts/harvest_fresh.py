import asyncio
import base64
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp_socks import ProxyConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("harvester")

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

SOURCES_JSON = [
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0001.json",
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0002.json",
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0003.json",
    "https://fastly.jsdelivr.net/gh/0xRadikal/Free-v2ray-Configs@main/all/singbox.json",
]

SOURCES_TXT = [
    "https://fastly.jsdelivr.net/gh/Epodonios/v2ray-configs@main/All_Configs_Sub.txt",
    "https://fastly.jsdelivr.net/gh/barry-far/V2ray-Config@main/All_Configs_Sub.txt",
    "https://fastly.jsdelivr.net/gh/SoliSpirit/v2ray-configs@main/all_configs.txt",
    "https://fastly.jsdelivr.net/gh/ebrasha/free-v2ray-public-list@main/separated-protocols/vless_configs.txt",
    "https://fastly.jsdelivr.net/gh/ebrasha/free-v2ray-public-list@main/separated-protocols/trojan_configs.txt",
    "https://fastly.jsdelivr.net/gh/Surfboardv2ray/TGParse@main/configtg.txt",
    "https://fastly.jsdelivr.net/gh/mrvcoder/V2rayCollector@main/mixed_iran.txt",
]

def is_valid_reality_pbk(pbk: Any) -> bool:
    if not isinstance(pbk, str):
        return False
    pbk = pbk.strip()
    if len(pbk) not in (43, 44):
        return False
    padded = pbk + "=" * ((4 - len(pbk) % 4) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded)
        return len(raw) == 32
    except Exception:
        return False

def normalize_outbound(ob: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    t = ob.get("type")
    if t not in {"vless", "trojan", "shadowsocks"}:
        return None
    server = ob.get("server")
    port = ob.get("server_port")
    if not server or not port:
        return None
    if ob.get("transport", {}).get("type") == "xhttp":
        return None

    clean = dict(ob)
    tls = clean.get("tls")
    if isinstance(tls, dict):
        if tls.get("reality", {}).get("enabled"):
            pbk = tls["reality"].get("public_key")
            if not is_valid_reality_pbk(pbk):
                return None
    return clean

async def fetch_all_sources() -> List[Dict[str, Any]]:
    all_obs = []
    seen = set()

    async with aiohttp.ClientSession(headers=HEADERS) as sess:
        # JSON
        for url in SOURCES_JSON:
            try:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        for ob in data.get("outbounds", []):
                            norm = normalize_outbound(ob)
                            if norm:
                                key = (norm["type"], str(norm["server"]), int(norm["server_port"]))
                                if key not in seen:
                                    seen.add(key)
                                    all_obs.append(norm)
                        logger.info(f"Loaded JSON {url.split('/')[-1]}: candidates={len(all_obs)}")
            except Exception as e:
                logger.warning(f"Error loading {url}: {e}")

    logger.info(f"Total parsed unique candidates: {len(all_obs)}")
    return all_obs

async def check_tcp(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(250)
    alive = []

    async def _dial(ob):
        async with sem:
            loop = asyncio.get_running_loop()
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setblocking(False)
                await asyncio.wait_for(loop.sock_connect(s, (ob["server"], int(ob["server_port"]))), timeout=1.2)
                s.close()
                alive.append(ob)
            except Exception:
                pass

    await asyncio.gather(*[_dial(ob) for ob in candidates])
    logger.info(f"TCP Alive: {len(alive)} / {len(candidates)}")
    return alive

def purge_invalid(outbounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    clean_list = [dict(ob) for ob in outbounds]
    for idx, ob in enumerate(clean_list):
        ob["tag"] = f"chk-{idx}"

    for _ in range(30):
        sb_config = {
            "log": {"level": "error"},
            "inbounds": [{"type": "socks", "tag": "test", "listen": "127.0.0.1", "listen_port": 29998}],
            "outbounds": [{"type": "direct", "tag": "direct"}] + clean_list,
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
            json.dump(sb_config, tf)
            tmp_name = tf.name

        try:
            res = subprocess.run(["/usr/local/bin/sing-box", "check", "-c", tmp_name], capture_output=True, text=True)
            if res.returncode == 0:
                logger.info(f"sing-box check passed: {len(clean_list)} outbounds valid")
                return clean_list
            m = re.search(r"outbound\[(\d+)\]", res.stderr)
            if m:
                bad_idx = int(m.group(1)) - 1
                if 0 <= bad_idx < len(clean_list):
                    clean_list.pop(bad_idx)
                    continue
            logger.warning(f"sing-box check error: {res.stderr[:100]}")
            break
        finally:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
    return clean_list

async def probe_opencode(nodes: List[Dict[str, Any]], target: int = 200) -> List[Dict[str, Any]]:
    port = 20970
    users = []
    rules = []
    sb_obs = []
    for idx, ob in enumerate(nodes):
        u = f"p{idx}"
        tag = f"n{idx}"
        users.append({"username": u, "password": "p"})
        rules.append({"auth_user": [u], "outbound": tag})
        ob_c = dict(ob)
        ob_c["tag"] = tag
        sb_obs.append(ob_c)

    sb_obs.append({"type": "direct", "tag": "direct"})
    cfg = {
        "log": {"level": "error"},
        "inbounds": [{"type": "socks", "tag": "s", "listen": "127.0.0.1", "listen_port": port, "users": users}],
        "outbounds": sb_obs,
        "route": {"rules": rules, "final": "direct"}
    }

    tf = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(cfg, tf)
    tf.close()

    proc = subprocess.Popen(["/usr/local/bin/sing-box", "run", "-c", tf.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for _ in range(80):
        await asyncio.sleep(0.05)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break

    logger.info(f"Sing-box ready on port {port}. Probing {len(nodes)} nodes strictly against OpenCode Free Gate...")
    verified = []
    sem = asyncio.Semaphore(40)

    async def _test_node(idx, ob):
        if len(verified) >= target:
            return
        async with sem:
            conn = ProxyConnector.from_url(f"socks5://p{idx}:p@127.0.0.1:{port}")
            try:
                to = aiohttp.ClientTimeout(total=4.5, connect=2.5, sock_connect=2.5)
                async with aiohttp.ClientSession(connector=conn, timeout=to) as sess:
                    headers = {
                        "Authorization": "Bearer public",
                        "User-Agent": "opencode/latest/2.0.32/cli",
                    }
                    async with sess.get("https://opencode.ai/zen/v1/models", headers=headers) as resp:
                        if resp.status == 200:
                            verified.append(ob)
                            logger.info(f"[+] REAL 200 OK Node {idx} ({ob.get('server')})! Total verified: {len(verified)}")
            except Exception:
                pass
            finally:
                await conn.close()

    await asyncio.gather(*[_test_node(i, ob) for i, ob in enumerate(nodes)])

    proc.terminate()
    if os.path.exists(tf.name):
        os.remove(tf.name)

    logger.info(f"=== STRICT VERIFICATION COMPLETED: {len(verified)} / {len(nodes)} PASSED WITH 200 OK ===")
    return verified

async def main():
    cands = await fetch_all_sources()
    alive = await check_tcp(cands)
    clean = purge_invalid(alive)
    verified = await probe_opencode(clean, target=200)

    if verified:
        out = {"outbounds": verified}
        dest = "/home/ubuntu/.local/share/o2a/proxies/expanded_verified.json"
        with open(dest, "w") as f:
            json.dump(out, f, indent=2)
        logger.info(f"Wrote {len(verified)} real verified nodes to {dest}!")

if __name__ == "__main__":
    asyncio.run(main())
