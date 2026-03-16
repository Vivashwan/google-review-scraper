"""
Proxy Checker
=============
Checks all proxies in proxies.txt and reports:
- IP address
- Country
- City
- ISP / Organisation
- Whether Google Maps is accessible

Usage:
    python3 check_proxies.py
    python3 check_proxies.py --file proxies.txt
    python3 check_proxies.py --file proxies.txt --test-google
"""

import asyncio
import argparse
import sys
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("Installing aiohttp...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "aiohttp", "-q"])
    import aiohttp


# ─────────────────────────────────────────────────────────────────────────────
# PROXY LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_proxies(filepath: str) -> list:
    proxies = []
    with open(filepath) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) >= 4:
                host     = parts[0]
                port     = parts[1]
                user     = parts[2]
                password = ":".join(parts[3:])
                proxies.append({
                    "index":    i,
                    "line":     line,
                    "server":   f"http://{host}:{port}",
                    "username": user,
                    "password": password,
                    "url":      f"http://{user}:{password}@{host}:{port}",
                })
            elif len(parts) == 2:
                proxies.append({
                    "index":  i,
                    "line":   line,
                    "server": f"http://{parts[0]}:{parts[1]}",
                    "url":    f"http://{parts[0]}:{parts[1]}",
                })
            else:
                print(f"  [SKIP] Line {i}: invalid format — {line!r}")
    return proxies


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS
# ─────────────────────────────────────────────────────────────────────────────

STATUS_SYMBOLS = {
    "ok":      "✅",
    "blocked": "⚠️ ",
    "dead":    "❌",
}

COUNTRY_NAMES = {
    "IN": "India 🇮🇳",
    "US": "USA 🇺🇸",
    "GB": "UK 🇬🇧",
    "DE": "Germany 🇩🇪",
    "ID": "Indonesia 🇮🇩",
    "PK": "Pakistan 🇵🇰",
    "PH": "Philippines 🇵🇭",
    "VN": "Vietnam 🇻🇳",
    "MY": "Malaysia 🇲🇾",
    "SG": "Singapore 🇸🇬",
    "JP": "Japan 🇯🇵",
    "CN": "China 🇨🇳",
    "BR": "Brazil 🇧🇷",
    "RU": "Russia 🇷🇺",
}


async def check_ip_info(session: aiohttp.ClientSession, proxy: dict) -> dict:
    """Check IP info via ipinfo.io"""
    try:
        async with session.get(
            "https://ipinfo.io/json",
            proxy=proxy["url"],
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
        ) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                return {
                    "status":  "ok",
                    "ip":      data.get("ip", "?"),
                    "country": data.get("country", "?"),
                    "city":    data.get("city", "?"),
                    "org":     data.get("org", "?"),
                }
            else:
                return {"status": "dead", "error": f"HTTP {resp.status}"}
    except asyncio.TimeoutError:
        return {"status": "dead", "error": "Timeout"}
    except Exception as e:
        return {"status": "dead", "error": str(e)[:60]}


async def check_google(session: aiohttp.ClientSession, proxy: dict) -> str:
    """Check if Google is reachable — returns 'ok', 'blocked', or 'dead'"""
    try:
        async with session.get(
            "https://www.google.com",
            proxy=proxy["url"],
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
            allow_redirects=True,
        ) as resp:
            if resp.status in (200, 301, 302):
                return "ok"
            elif resp.status in (403, 429, 503):
                return "blocked"
            else:
                return "dead"
    except asyncio.TimeoutError:
        return "blocked"   # timeout = tarpit = Google holding connection = blocked
    except Exception:
        return "dead"


async def check_google_maps(session: aiohttp.ClientSession, proxy: dict) -> str:
    """Check if Google Maps specifically is reachable"""
    try:
        async with session.get(
            "https://www.google.com/maps?hl=en&gl=in",
            proxy=proxy["url"],
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
            allow_redirects=True,
        ) as resp:
            if resp.status in (200, 301, 302):
                return "ok"
            elif resp.status in (403, 429, 503):
                return "blocked"
            else:
                return "dead"
    except asyncio.TimeoutError:
        return "blocked"
    except Exception:
        return "dead"


async def check_one(proxy: dict, test_google: bool) -> dict:
    """Run all checks for a single proxy."""
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        result = await check_ip_info(session, proxy)
        result["index"] = proxy["index"]

        if result["status"] == "ok" and test_google:
            result["google"]      = await check_google(session, proxy)
            result["google_maps"] = await check_google_maps(session, proxy)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_report(results: list, test_google: bool):
    print()
    print("=" * 100)
    print(f"  PROXY CHECK RESULTS")
    print("=" * 100)

    if test_google:
        print(f"  {'#':>3}  {'Status':<6}  {'IP':<20}  {'Country':<18}  {'City':<18}  {'Google':<8}  {'Maps':<8}  ISP")
        print("-" * 100)
    else:
        print(f"  {'#':>3}  {'Status':<6}  {'IP':<20}  {'Country':<18}  {'City':<18}  ISP")
        print("-" * 100)

    ok_count      = 0
    indian_count  = 0
    maps_ok_count = 0

    for r in sorted(results, key=lambda x: x["index"]):
        idx    = r["index"]
        status = r["status"]

        if status == "ok":
            ok_count += 1
            ip       = r.get("ip", "?")
            country  = r.get("country", "?")
            city     = r.get("city", "?")
            org      = r.get("org", "?")[:35]
            cc_label = COUNTRY_NAMES.get(country, country)

            if country == "IN":
                indian_count += 1

            sym = STATUS_SYMBOLS["ok"]

            if test_google:
                g_status  = r.get("google", "?")
                gm_status = r.get("google_maps", "?")
                g_sym     = "✅" if g_status  == "ok" else ("⚠️ " if g_status  == "blocked" else "❌")
                gm_sym    = "✅" if gm_status == "ok" else ("⚠️ " if gm_status == "blocked" else "❌")
                if gm_status == "ok":
                    maps_ok_count += 1
                print(f"  {idx:>3}  {sym:<6}  {ip:<20}  {cc_label:<18}  {city:<18}  {g_sym:<8}  {gm_sym:<8}  {org}")
            else:
                print(f"  {idx:>3}  {sym:<6}  {ip:<20}  {cc_label:<18}  {city:<18}  {org}")
        else:
            error = r.get("error", "unknown error")
            sym   = STATUS_SYMBOLS["dead"]
            print(f"  {idx:>3}  {sym:<6}  {'DEAD':<20}  {'—':<18}  {'—':<18}  {error}")

    print("=" * 100)
    print(f"  Total proxies  : {len(results)}")
    print(f"  Working        : {ok_count}")
    print(f"  Dead           : {len(results) - ok_count}")
    print(f"  Indian IPs     : {indian_count}")
    if test_google:
        print(f"  Google Maps OK : {maps_ok_count}  ← usable for scraping")
    print("=" * 100)

    if test_google and maps_ok_count > 0:
        print(f"\n  ✅ {maps_ok_count} proxies can reach Google Maps.")
        print(f"     Expected scraping speed: ~{maps_ok_count * 3600:,} reviews/hour")
    elif test_google and maps_ok_count == 0:
        print(f"\n  ❌ No proxies can reach Google Maps.")
        print(f"     Use your home IP instead (run without --proxies flag).")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main(proxies_file: str, test_google: bool):
    if not Path(proxies_file).exists():
        print(f"Error: {proxies_file} not found")
        sys.exit(1)

    proxies = load_proxies(proxies_file)
    if not proxies:
        print("No proxies found in file")
        sys.exit(1)

    print(f"Checking {len(proxies)} proxies", end="")
    if test_google:
        print(" (including Google Maps connectivity)...", end="")
    print()

    tasks   = [check_one(p, test_google) for p in proxies]
    results = await asyncio.gather(*tasks)
    print_report(results, test_google)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check proxy health and Google Maps accessibility")
    parser.add_argument("--file",         default="proxies.txt", help="Path to proxies file")
    parser.add_argument("--test-google",  action="store_true",   help="Also test Google and Google Maps access")
    args = parser.parse_args()

    asyncio.run(main(args.file, args.test_google))