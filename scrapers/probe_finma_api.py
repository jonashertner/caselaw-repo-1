#!/usr/bin/env python3
"""Probe FINMA /de/api/search/getresult API."""
import requests
import json

HOST = "https://www.finma.ch"
API = "/de/api/search/getresult"
s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; Research/1.0)",
    "Accept-Language": "de-CH,de;q=0.9",
})

SOURCES = {
    "kasuistik": "{2FBD0DFE-112F-4176-BE8D-07C2D0BE0903}",
    "court": "{4C699740-8893-4B35-B7D9-152A2702ABCD}",
    "art34": "{CC4C8714-99CE-4264-8E85-95D43B8F1861}",
    "circulars": "{3009DAA1-E9A3-4CF1-B0F0-8059B9A37AFA}",
}

# Try different request formats
for name, guid in SOURCES.items():
    print(f"\n{'='*70}")
    print(f"=== {name.upper()} (GUID: {guid})")
    print(f"{'='*70}")
    
    # Try 1: GET with query params
    print("\n--- GET with params ---")
    params = {"ds": guid}
    try:
        r = s.get(HOST + API, params=params, timeout=15,
                 headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
        ct = r.headers.get("content-type", "")
        print(f"  Status: {r.status_code} | CT: {ct[:40]} | Len: {len(r.text)}")
        if r.status_code == 200 and len(r.text) > 10:
            try:
                data = r.json()
                print(f"  JSON keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                print(f"  Sample: {json.dumps(data, ensure_ascii=False)[:500]}")
            except:
                print(f"  Body: {r.text[:500]}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Try 2: POST with JSON
    print("\n--- POST JSON ---")
    try:
        r = s.post(HOST + API, json={"ds": guid}, timeout=15,
                  headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest",
                           "Content-Type": "application/json"})
        ct = r.headers.get("content-type", "")
        print(f"  Status: {r.status_code} | CT: {ct[:40]} | Len: {len(r.text)}")
        if r.status_code == 200 and len(r.text) > 10:
            try:
                data = r.json()
                print(f"  JSON keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                print(f"  Sample: {json.dumps(data, ensure_ascii=False)[:500]}")
            except:
                print(f"  Body: {r.text[:500]}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Try 3: POST form-encoded
    print("\n--- POST form ---")
    try:
        r = s.post(HOST + API, data={"ds": guid}, timeout=15,
                  headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
        ct = r.headers.get("content-type", "")
        print(f"  Status: {r.status_code} | CT: {ct[:40]} | Len: {len(r.text)}")
        if r.status_code == 200 and len(r.text) > 10:
            try:
                data = r.json()
                print(f"  JSON keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                print(f"  Sample: {json.dumps(data, ensure_ascii=False)[:500]}")
            except:
                print(f"  Body: {r.text[:500]}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Try 4: GET with more params from the JS (order, page)
    print("\n--- GET with full params ---")
    try:
        params = {"ds": guid, "order": "4", "page": "1", "pagesize": "10"}
        r = s.get(HOST + API, params=params, timeout=15,
                 headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
        ct = r.headers.get("content-type", "")
        print(f"  Status: {r.status_code} | CT: {ct[:40]} | Len: {len(r.text)}")
        if r.status_code == 200 and len(r.text) > 10:
            try:
                data = r.json()
                if isinstance(data, dict):
                    print(f"  Keys: {list(data.keys())}")
                    if "Items" in data:
                        print(f"  Items count: {len(data['Items'])}")
                        if data['Items']:
                            print(f"  First item keys: {list(data['Items'][0].keys())}")
                            print(f"  First item: {json.dumps(data['Items'][0], ensure_ascii=False)[:500]}")
                    if "TotalCount" in data:
                        print(f"  TotalCount: {data['TotalCount']}")
                    if "NextPageLink" in data:
                        print(f"  NextPageLink: {data['NextPageLink']}")
                    # Print full response if small
                    if len(r.text) < 2000:
                        print(f"  Full: {json.dumps(data, ensure_ascii=False)[:2000]}")
                else:
                    print(f"  Sample: {json.dumps(data, ensure_ascii=False)[:500]}")
            except:
                print(f"  Body: {r.text[:500]}")
    except Exception as e:
        print(f"  Error: {e}")

    # Only do first source in detail, others just the working format
    if name == "kasuistik":
        # Try larger page size
        print("\n--- GET pagesize=100 ---")
        try:
            params = {"ds": guid, "order": "4", "page": "1", "pagesize": "100"}
            r = s.get(HOST + API, params=params, timeout=15,
                     headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
            if r.status_code == 200:
                try:
                    data = r.json()
                    if isinstance(data, dict) and "Items" in data:
                        print(f"  Items: {len(data['Items'])}, Total: {data.get('TotalCount', '?')}")
                        print(f"  NextPage: {data.get('NextPageLink', 'none')}")
                except:
                    pass
        except:
            pass