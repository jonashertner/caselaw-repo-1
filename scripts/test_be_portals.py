#!/usr/bin/env python3
"""Test BE Tribuna portals for DB connectivity."""
import re
import time

import requests
import urllib3
urllib3.disable_warnings()

PORTALS = [
    ("BVD", "https://www.bvd-entscheide.apps.be.ch/tribunapublikation"),
    ("AA", "https://www.aa-entscheide.apps.be.ch/tribunapublikation"),
    ("STRK", "https://www.strk-entscheide.apps.be.ch/tribunapublikation"),
]

CONFIG_HASH = "7225438C30B96853F589E2336CAF98F1"
LOADTABLE_HASH = "CAC80118FB77794F1FDFC1B51371CC63"

for name, base_url in PORTALS:
    gwt_base = f"{base_url}/tribunavtplus"
    try:
        # Discover permutation
        resp = requests.get(f"{gwt_base}/tribunavtplus.nocache.js", timeout=10, verify=False)
        perms = re.findall(r"'([A-F0-9]{32})'", resp.text)
        perm = perms[0] if perms else "?"
        print(f"{name}: permutation={perm}")

        headers = {"Content-Type": "text/x-gwt-rpc; charset=utf-8", "X-GWT-Permutation": perm}

        # Get config/credential
        config_body = (
            f"7|0|4|{gwt_base}/|{CONFIG_HASH}|"
            "tribunavtplus.client.zugriff.ConfigService|readConfigFile|"
            "1|2|3|4|0|"
        )
        time.sleep(1)
        resp = requests.post(f"{gwt_base}/config", data=config_body, headers=headers, verify=False, timeout=10)
        hex_strs = re.findall(r'"([0-9a-f]{20,})"', resp.text)
        cred = sorted(hex_strs, key=lambda s: abs(len(s) - 96))[0] if hex_strs else ""
        print(f"  credential len={len(cred)}")

        # Test getInitialSearch (3 String params)
        init_body = (
            f"7|0|8|{gwt_base}/|{LOADTABLE_HASH}|"
            "tribunavtplus.client.zugriff.LoadTableService|getInitialSearch|"
            f"java.lang.String/2004016611|{cred}||de|"
            "1|2|3|4|3|5|5|5|6|7|8|"
        )
        time.sleep(1)
        resp = requests.post(f"{gwt_base}/loadTable", data=init_body, headers=headers, verify=False, timeout=10)

        if "DB_SERVER is null" in resp.text:
            print("  getInitialSearch: DB DISCONNECTED")
        elif "//OK" in resp.text:
            print(f"  getInitialSearch: OK (len={len(resp.text)})")
        elif "Could not locate requested method" in resp.text:
            print("  getInitialSearch: method not found (old Tribuna)")
        elif "//EX" in resp.text:
            errs = re.findall(r'"([^"]{10,})"', resp.text)
            print(f"  getInitialSearch: ERROR - {errs[1][:120] if len(errs) > 1 else resp.text[:120]}")
        else:
            print(f"  getInitialSearch: {resp.text[:120]}")

    except Exception as e:
        print(f"{name}: FAILED - {e}")
    print()
