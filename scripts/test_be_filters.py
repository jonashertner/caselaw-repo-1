#!/usr/bin/env python3
"""Quick test: try ZSG, BVD, STRK with various court filters."""
import re
import time
import requests
import urllib3
urllib3.disable_warnings()

CONFIG_HASH = "7225438C30B96853F589E2336CAF98F1"
LOADTABLE_HASH = "CAC80118FB77794F1FDFC1B51371CC63"

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


def build_search(gwt_base, cred, court, nf=20):
    strings = [
        f"{gwt_base}/", LOADTABLE_HASH,
        "tribunavtplus.client.zugriff.LoadTableService", "search",
        "java.lang.String/2004016611", "java.util.ArrayList/4159755760",
        "Z", "I", "java.lang.Integer/3438268394", "java.util.Map",
        "", "0", court, "0;false", "5;true", cred, "1",
        "java.util.HashMap/1797211028",
    ]
    for key, label in COLUMNS:
        strings.append(key)
        if label:
            strings.append(label)
    strings.append("de")
    st = "|".join(strings)
    num_strings = len(strings)
    num_params = 5 + nf + 21
    field_types = "|".join(["5"] * nf)
    types = f"5|5|6|7|6|{field_types}|8|8|8|5|5|9|9|9|5|5|5|5|7|10|5|5|5|5|5|5|5"
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
        f"11|12|6|0|0|6|1|5|13|{empties}|20|0|-1|11|11|"
        f"0|9|0|9|-1|14|15|16|17|0|18|18|{col_section}|"
        f"11|{locale_ref}|11|11|12|12|0|"
    )
    return f"7|0|{num_strings}|{st}|1|2|3|4|{num_params}|{types}|{values}"


def init_portal(name, base_url):
    gwt_base = f"{base_url}/tribunavtplus"
    sess = requests.Session()
    sess.get(base_url + "/", timeout=10, verify=False)
    resp = sess.get(f"{gwt_base}/tribunavtplus.nocache.js", timeout=10, verify=False)
    perms = re.findall(r"[A-F0-9]{32}", resp.text)
    perm = perms[0] if perms else "?"
    headers = {
        "Content-Type": "text/x-gwt-rpc; charset=utf-8",
        "X-GWT-Permutation": perm,
        "X-GWT-Module-Base": f"{gwt_base}/",
    }
    config_body = (
        f"7|0|4|{gwt_base}/|{CONFIG_HASH}|"
        "tribunavtplus.client.zugriff.ConfigService|readConfigFile|1|2|3|4|0|"
    )
    time.sleep(0.5)
    resp = sess.post(f"{gwt_base}/config", data=config_body, headers=headers, verify=False, timeout=10)
    hex_strs = re.findall(r'"([0-9a-f]{20,})"', resp.text)
    cred = sorted(hex_strs, key=lambda s: abs(len(s) - 96))[0] if hex_strs else ""
    return sess, gwt_base, headers, cred


# Test ZSG with actual court filters
TESTS = [
    ("ZSG", "https://www.zsg-entscheide.apps.be.ch/tribunapublikation",
     20, ["OG", "BM", "BJS", "EO", "O", "WSG"]),
    ("BVD", "https://www.bvd-entscheide.apps.be.ch/tribunapublikation",
     20, ["BVD", "OG_BVD", "REG_BVD", "RR", ""]),
    ("STRK", "https://www.strk-entscheide.apps.be.ch/tribunapublikation",
     21, ["STRK", "OG_STRK", "SKE", ""]),
]

for name, base_url, nf, filters in TESTS:
    print(f"=== {name} (nf={nf}) ===")
    try:
        sess, gwt_base, headers, cred = init_portal(name, base_url)
        print(f"  cred len={len(cred)}")
        for cf in filters:
            time.sleep(1)
            body = build_search(gwt_base, cred, cf, nf)
            resp = sess.post(f"{gwt_base}/loadTable", data=body, headers=headers, verify=False, timeout=15)
            if resp.text.startswith("//OK"):
                m = re.match(r"^//OK\[(\d+)", resp.text)
                total = m.group(1) if m else "?"
                print(f"  court='{cf}': total={total}")
            elif "//EX" in resp.text:
                errs = re.findall(r'"([^"]{10,})"', resp.text)
                print(f"  court='{cf}': EX - {errs[1][:120] if len(errs) > 1 else 'error'}")
            else:
                print(f"  court='{cf}': {resp.text[:120]}")
    except Exception as e:
        print(f"  FAILED: {e}")
    print()
