#!/usr/bin/env python3
"""Test ALL BE Tribuna portals with both getInitialSearch and search methods."""
import re
import time
import requests
import urllib3
urllib3.disable_warnings()

PORTALS = [
    ("ZSG",  "https://www.zsg-entscheide.apps.be.ch/tribunapublikation", ["", "ZSG"]),
    ("VG",   "https://www.vg-urteile.apps.be.ch/tribunapublikation",     ["VG", ""]),
    ("BVD",  "https://www.bvd-entscheide.apps.be.ch/tribunapublikation", ["BVD", ""]),
    ("STRK", "https://www.strk-entscheide.apps.be.ch/tribunapublikation",["STRK", ""]),
    ("AA",   "https://www.aa-entscheide.apps.be.ch/tribunapublikation",  ["OG_AA", "AA", ""]),
]

CONFIG_HASH = "7225438C30B96853F589E2336CAF98F1"
LOADTABLE_HASH = "CAC80118FB77794F1FDFC1B51371CC63"

# Column defs for search body (same as base_tribuna.py)
COLUMNS = [
    ("decisionDate", "Entscheiddatum"), ("dossierNumber", "Dossier"),
    ("classification", "Zusatzeigenschaft"), ("indexCode", "Quelle"),
    ("dossierObject", "Betreff"), ("law", "Rechtsgebiet"),
    ("shortText", "Vorschautext"), ("department", "Abteilung"),
    ("createDate", "Erfasst am"), ("creater", "Ersteller"),
    ("judge", "Richter"), ("executiontype", "Erledigungsart"),
    ("legalDate", "Rechtskraftdatum"), ("objecttype", "Objekttyp"),
    ("typist", "Schreiber"), ("description", "Beschreibung"),
    ("reference", "Referenz"), ("relevance", None),
]


def build_search_body(gwt_base, credential, court_filter, nf):
    """Build search body with SEARCH_FIELD_COUNT=nf (20 or 21)."""
    strings = [
        f"{gwt_base}/", LOADTABLE_HASH,
        "tribunavtplus.client.zugriff.LoadTableService", "search",
        "java.lang.String/2004016611", "java.util.ArrayList/4159755760",
        "Z", "I", "java.lang.Integer/3438268394", "java.util.Map",
        "", "0", court_filter, "0;false", "5;true", credential, "1",
        "java.util.HashMap/1797211028",
    ]
    for key, label in COLUMNS:
        strings.append(key)
        if label:
            strings.append(label)
    strings.append("de")

    num_strings = len(strings)
    st = "|".join(strings)

    field_types = "|".join(["5"] * nf)
    num_params = 5 + nf + 21
    types = (
        f"5|5|6|7|6|{field_types}|8|8|8|5|5|"
        f"9|9|9|5|5|5|5|7|10|5|5|5|5|5|5|5"
    )

    empties = "|".join(["11"] * nf)
    col_refs = []
    idx = 19
    for key, label in COLUMNS:
        col_refs.append(f"5|{idx}")
        idx += 1
        if label:
            col_refs.append(f"5|{idx}")
            idx += 1
        else:
            col_refs.append("5|11")
    col_section = "|".join(col_refs)
    locale_ref = idx

    values = (
        f"11|12|6|0|0|6|1|5|13|"
        f"{empties}|"
        f"20|0|-1|"
        f"11|11|"
        f"0|9|0|9|-1|"
        f"14|15|16|17|0|18|18|"
        f"{col_section}|"
        f"11|{locale_ref}|"
        f"11|11|12|12|0|"
    )

    return f"7|0|{num_strings}|{st}|1|2|3|4|{num_params}|{types}|{values}"


for name, base_url, court_filters in PORTALS:
    gwt_base = f"{base_url}/tribunavtplus"
    print(f"=== {name}: {base_url} ===")
    try:
        # Step 1: Discover permutation
        sess = requests.Session()
        sess.get(base_url + "/", timeout=10, verify=False)
        resp = sess.get(f"{gwt_base}/tribunavtplus.nocache.js", timeout=10, verify=False)
        perms = re.findall(r"[A-F0-9]{32}", resp.text)
        perm = perms[0] if perms else "?"
        print(f"  permutation: {perm}")

        headers = {
            "Content-Type": "text/x-gwt-rpc; charset=utf-8",
            "X-GWT-Permutation": perm,
            "X-GWT-Module-Base": f"{gwt_base}/",
        }

        # Step 2: readConfigFile
        config_body = (
            f"7|0|4|{gwt_base}/|{CONFIG_HASH}|"
            "tribunavtplus.client.zugriff.ConfigService|readConfigFile|"
            "1|2|3|4|0|"
        )
        time.sleep(1)
        resp = sess.post(f"{gwt_base}/config", data=config_body, headers=headers, verify=False, timeout=10)
        hex_strs = re.findall(r'"([0-9a-f]{20,})"', resp.text)
        cred = sorted(hex_strs, key=lambda s: abs(len(s) - 96))[0] if hex_strs else ""
        print(f"  credential: len={len(cred)}")

        if "//EX" in resp.text:
            errs = re.findall(r'"([^"]{10,})"', resp.text)
            print(f"  config ERROR: {errs[1][:200] if len(errs) > 1 else resp.text[:200]}")

        # Step 3: getInitialSearch (diagnostic only)
        init_body = (
            f"7|0|8|{gwt_base}/|{LOADTABLE_HASH}|"
            "tribunavtplus.client.zugriff.LoadTableService|getInitialSearch|"
            f"java.lang.String/2004016611|{cred}||de|"
            "1|2|3|4|3|5|5|5|6|7|8|"
        )
        time.sleep(1)
        resp = sess.post(f"{gwt_base}/loadTable", data=init_body, headers=headers, verify=False, timeout=10)
        if "DB_SERVER is null" in resp.text:
            print("  getInitialSearch: DB DISCONNECTED")
        elif "//OK" in resp.text:
            print(f"  getInitialSearch: OK (len={len(resp.text)})")
        elif "//EX" in resp.text:
            errs = re.findall(r'"([^"]{10,})"', resp.text)
            print(f"  getInitialSearch: EX - {errs[1][:200] if len(errs) > 1 else resp.text[:200]}")
        else:
            print(f"  getInitialSearch: {resp.text[:200]}")

        # Step 4: getBerechtigungen
        berech_body = (
            f"7|0|6|{gwt_base}/|{LOADTABLE_HASH}|"
            "tribunavtplus.client.zugriff.LoadTableService|getBerechtigungen|"
            "java.lang.String/2004016611||"
            "1|2|3|4|2|5|5|6|6|"
        )
        time.sleep(1)
        resp = sess.post(f"{gwt_base}/loadTable", data=berech_body, headers=headers, verify=False, timeout=10)
        if "//OK" in resp.text:
            print("  getBerechtigungen: OK")
        elif "//EX" in resp.text:
            errs = re.findall(r'"([^"]{10,})"', resp.text)
            print(f"  getBerechtigungen: EX - {errs[1][:200] if len(errs) > 1 else resp.text[:200]}")
        else:
            print(f"  getBerechtigungen: {resp.text[:200]}")

        # Step 5: Try search() with different field counts and court filters
        for nf in [20, 21]:
            for cf in court_filters:
                time.sleep(1.5)
                try:
                    body = build_search_body(gwt_base, cred, cf, nf)
                    resp = sess.post(f"{gwt_base}/loadTable", data=body, headers=headers, verify=False, timeout=15)
                    if resp.text.startswith("//OK"):
                        total_match = re.match(r"^//OK\[(\d+)", resp.text)
                        total = total_match.group(1) if total_match else "?"
                        print(f"  search(nf={nf}, court='{cf}'): OK total={total} (len={len(resp.text)})")
                    elif "//EX" in resp.text:
                        errs = re.findall(r'"([^"]{10,})"', resp.text)
                        msg = errs[1][:150] if len(errs) > 1 else resp.text[:150]
                        print(f"  search(nf={nf}, court='{cf}'): EX - {msg}")
                    else:
                        print(f"  search(nf={nf}, court='{cf}'): {resp.text[:150]}")
                except Exception as e:
                    print(f"  search(nf={nf}, court='{cf}'): FAILED - {e}")

    except Exception as e:
        print(f"  FAILED: {e}")
    print()
