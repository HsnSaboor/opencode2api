import base64
import concurrent.futures
import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("verifier")

SOURCES = [
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0001.json",
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0002.json",
    "https://fastly.jsdelivr.net/gh/Au1rxx/free-vpn-subscriptions@main/output/stable/singbox-0003.json",
    "https://fastly.jsdelivr.net/gh/0xRadikal/Free-v2ray-Configs@main/verified/singbox.json",
    "https://fastly.jsdelivr.net/gh/0xRadikal/Free-v2ray-Configs@main/fast/singbox.json",
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

def purge_invalid(outbounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    clean_list = [dict(ob) for ob in outbounds]
    for idx, ob in enumerate(clean_list):
        ob["tag"] = f"chk-{idx}"

    for _ in range(30):
        sb_config = {
            "log": {"level": "error"},
            "inbounds": [{"type": "socks", "tag": "test", "listen": "127.0.0.1", "listen_port": 29999}],
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
                bad_idx = int(m.group(1))
                list_idx = bad_idx - 1
                if 0 <= list_idx < len(clean_list):
                    clean_list.pop(list_idx)
                    continue
            logger.warning(f"sing-box check error: {res.stderr[:100]}")
            break
        finally:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
    return clean_list

def test_tcp(ob: Dict[str, Any]) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.2)
        s.connect((ob["server"], int(ob["server_port"])))
        s.close()
        return True
    except Exception:
        return False

def main():
    target_verified = 130
    logger.info(f"Starting proxy harvester & verifier for >= {target_verified} nodes...")

    # 1. Fetch sources
    raw_obs = []
    seen = set()
    for url in SOURCES:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.load(resp)
                for ob in data.get("outbounds", []):
                    norm = normalize_outbound(ob)
                    if not norm:
                        continue
                    key = (norm["type"], str(norm["server"]), int(norm["server_port"]))
                    if key not in seen:
                        seen.add(key)
                        raw_obs.append(norm)
            logger.info(f"Loaded {url}: total unique={len(raw_obs)}")
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")

    logger.info(f"Total unique candidates: {len(raw_obs)}")

    # 2. Fast TCP Handshake
    logger.info("Running parallel TCP handshake check...")
    tcp_passed = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        results = executor.map(lambda ob: (ob, test_tcp(ob)), raw_obs)
        for ob, ok in results:
            if ok:
                tcp_passed.append(ob)

    logger.info(f"TCP Handshake Passed: {len(tcp_passed)} / {len(raw_obs)}")

    # 3. Clean via sing-box check
    clean_nodes = purge_invalid(tcp_passed)

    # 4. Sing-box Probe
    port = 20980
    users = []
    rules = []
    sb_obs = []
    for idx, ob in enumerate(clean_nodes):
        u = f"p{idx}"
        tag = f"n{idx}"
        users.append({"username": u, "password": "p"})
        rules.append({"auth_user": [u], "outbound": tag})
        ob_c = dict(ob)
        ob_c["tag"] = tag
        sb_obs.append(ob_c)

    sb_obs.append({"type": "direct", "tag": "direct"})
    sb_cfg = {
        "log": {"level": "error"},
        "inbounds": [{"type": "socks", "tag": "s", "listen": "127.0.0.1", "listen_port": port, "users": users}],
        "outbounds": sb_obs,
        "route": {"rules": rules, "final": "direct"}
    }

    tf = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(sb_cfg, tf)
    tf.close()

    proc = subprocess.Popen(["/usr/local/bin/sing-box", "run", "-c", tf.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for _ in range(60):
        time.sleep(0.05)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break

    logger.info("Sing-box probe daemon ready. Running HTTPS probes...")
    verified_nodes = []

    def probe_node(idx_ob):
        idx, ob = idx_ob
        if len(verified_nodes) >= target_verified:
            return None
        proxies = {
            "http": f"socks5://p{idx}:p@127.0.0.1:{port}",
            "https": f"socks5://p{idx}:p@127.0.0.1:{port}"
        }
        headers = {"Authorization": "Bearer public", "User-Agent": "opencode/1.18.29"}
        # 1. Try opencode models
        try:
            r = requests.get("https://opencode.ai/zen/v1/models", headers=headers, proxies=proxies, timeout=4)
            if r.status_code == 200:
                return ob
        except Exception:
            pass

        # 2. Try Google 204 fallback
        try:
            r = requests.get("https://www.gstatic.com/generate_204", proxies=proxies, timeout=3)
            if r.status_code == 204:
                return ob
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        for res in executor.map(probe_node, enumerate(clean_nodes)):
            if res:
                verified_nodes.append(res)
                if len(verified_nodes) % 10 == 0:
                    logger.info(f"Verified nodes count: {len(verified_nodes)} / {target_verified}")
                if len(verified_nodes) >= target_verified:
                    break

    proc.terminate()
    if os.path.exists(tf.name):
        os.remove(tf.name)

    logger.info(f"Verified {len(verified_nodes)} nodes (target was {target_verified})")
    if len(verified_nodes) < 100:
        logger.error("Failed to reach 100 verified nodes!")
        sys.exit(1)

    # Clean tags and format for expanded_verified.json
    final_outbounds = []
    for idx, ob in enumerate(verified_nodes):
        ob_c = dict(ob)
        ob_c["tag"] = f"node-{idx}"
        final_outbounds.append(ob_c)

    dest_profile = "/home/ubuntu/.local/share/o2a/proxies/expanded_verified.json"
    dest_state = "/home/ubuntu/.local/share/o2a/proxies/proxy-state.json"

    # Backup existing
    if os.path.exists(dest_profile):
        os.replace(dest_profile, dest_profile + ".bak")

    profile_doc = {"outbounds": final_outbounds}
    profile_bytes = json.dumps(profile_doc, indent=2).encode("utf-8")
    with open(dest_profile, "wb") as f:
        f.write(profile_bytes)

    fp = hashlib.sha256(profile_bytes).hexdigest()
    state_doc = {
        "version": 1,
        "fingerprint": fp,
        "verified": list(range(len(final_outbounds))),
        "cooling": {},
        "failures": {}
    }
    with open(dest_state, "w") as f:
        json.dump(state_doc, f, indent=2)

    logger.info(f"Successfully generated {len(final_outbounds)} verified nodes in {dest_profile} and {dest_state} (fingerprint: {fp[:12]})")

if __name__ == "__main__":
    main()
