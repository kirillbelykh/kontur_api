# main.py
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests

# ---------------- config ----------------
COOKIES_FILE = Path("kontur_cookies.json")
BASE = "https://mk.kontur.ru"

# Проверь/измени при необходимости:
ORGANIZATION_ID = "5cda50fa-523f-4bb5-85b6-66d7241b23cd"
WAREHOUSE_ID = "59739360-7d62-434b-ad13-4617c87a6d13"

DOCUMENT_NUMBER = "ТЕСТ"
PRODUCT_GROUP = "wheelChairs"   # как в devtools у тебя
RELEASE_METHOD_TYPE = "production"
CIS_TYPE = "unit"
FILLING_METHOD = "productsCatalog"

POSITIONS = [
    {
        "gtin": "04660537612754",
        "name": "Перчатки Sterä диагностические из натурального латекса, р-р M",
        "tnvedCode": "4015120009",
        "quantity": 90
    }
]

# debug files
LAST_SINGLE_REQ = Path("last_single_request.json")
LAST_SINGLE_RESP = Path("last_single_response.json")
LAST_MULTI_LOG = Path("last_multistep_log.json")

# ---------------- helpers ----------------
def load_cookies() -> Optional[Dict[str,str]]:
    if COOKIES_FILE.exists():
        try:
            data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass
    return None

def make_session_with_cookies(cookies: Dict[str,str]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    for k,v in cookies.items():
        s.cookies.set(k, v, domain="mk.kontur.ru", path="/")
    return s

# ---------------- API flows ----------------
def try_single_post(session: requests.Session, warehouse_id: str, document_number: str,
                    product_group: str, release_method_type: str, positions: List[Dict[str,Any]],
                    cis_type: str = "unit", filling_method: str = "productsCatalog") -> Optional[requests.Response]:
    path = f"/api/v1/codes-order?warehouseId={warehouse_id}"
    url = BASE + path
    body = {
        "documentNumber": document_number,
        "comment": "",
        "productGroup": product_group,
        "releaseMethodType": release_method_type,
        "fillingMethod": filling_method,
        "cisType": cis_type,
        "positions": [
            {
                "gtin": p["gtin"],
                "name": p.get("name",""),
                "tnvedCode": p.get("tnvedCode",""),
                "quantity": p.get("quantity", 1)
            } for p in positions
        ]
    }
    try:
        LAST_SINGLE_REQ.write_text(json.dumps({"url": url, "body": body}, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    try:
        resp = session.post(url, json=body, timeout=30)
    except Exception as e:
        print("Single POST exception:", e)
        return None

    try:
        LAST_SINGLE_RESP.write_text(json.dumps({"status": resp.status_code, "text": resp.text}, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return resp

def create_order_multistep(session: requests.Session, warehouse_id: str, document_number: str,
                           product_group: str, release_method_type: str, positions: List[Dict[str,Any]],
                           cis_type: str = "unit", filling_method: str = "productsCatalog") -> Dict[str,Any]:
    log = {"steps": []}

    # STEP1: create
    create_url = f"{BASE}/api/v1/codes-order?warehouseId={warehouse_id}"
    create_body = {
        "releaseMethodType": release_method_type,
        "comment": "",
        "documentNumber": "",
        "productGroup": product_group,
        "hasServiceProvider": False
    }
    r1 = session.post(create_url, json=create_body, timeout=30)
    log["steps"].append({"step":"create", "status": r1.status_code, "text": r1.text})
    if r1.status_code not in (200,201):
        LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"Create failed {r1.status_code}: {r1.text}")

    # extract order id
    order_id = None
    try:
        j = r1.json()
        if isinstance(j, dict) and j.get("id"):
            order_id = j["id"]
        elif isinstance(j, str):
            order_id = j
    except Exception:
        pass
    if not order_id:
        loc = r1.headers.get("Location")
        if loc:
            order_id = loc.rstrip("/").split("/")[-1]
    if not order_id:
        LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"Cannot determine order id from create response: {r1.status_code} {r1.text}")

    # STEP2: PUT update
    put_url = f"{BASE}/api/v1/codes-order/{order_id}"
    put_body = {
        "documentNumber": document_number,
        "comment": "",
        "fillingMethod": filling_method,
        "hasProducerInn": False,
        "withUnitsForSets": False,
        "paymentType": "uponApplication",
        "cisType": cis_type
    }
    r2 = session.put(put_url, json=put_body, timeout=30)
    log["steps"].append({"step":"put", "status": r2.status_code, "text": r2.text})
    if r2.status_code not in (200,201):
        LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"PUT failed {r2.status_code}: {r2.text}")

    # STEP3/4: add & patch positions
    position_ids = []
    for idx, p in enumerate(positions, start=1):
        add_url = f"{BASE}/api/v1/codes-order/{order_id}/positions/position"
        add_body = {"gtin": p["gtin"], "name": p.get("name",""), "tnvedCode": p.get("tnvedCode","")}
        r_add = session.post(add_url, json=add_body, timeout=30)
        log["steps"].append({"step": f"add_pos_{idx}", "status": r_add.status_code, "text": r_add.text})
        if r_add.status_code not in (200,201):
            LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
            raise RuntimeError(f"Add position failed {r_add.status_code}: {r_add.text}")

        new_pos_id = None
        try:
            add_json = r_add.json()
            if isinstance(add_json, dict) and add_json.get("id"):
                new_pos_id = add_json["id"]
            elif isinstance(add_json, int):
                new_pos_id = str(add_json)
        except Exception:
            pass
        if not new_pos_id:
            new_pos_id = str(idx)

        position_ids.append(new_pos_id)

        patch_url = f"{BASE}/api/v1/codes-order/{order_id}/positions/{new_pos_id}"
        patch_body = {"name": p.get("name",""), "quantity": p.get("quantity", 1), "tnvedCode": p.get("tnvedCode","")}
        r_patch = session.patch(patch_url, json=patch_body, timeout=30)
        log["steps"].append({"step": f"patch_pos_{idx}", "status": r_patch.status_code, "text": r_patch.text})
        if r_patch.status_code not in (200,201):
            LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
            raise RuntimeError(f"Patch failed {r_patch.status_code}: {r_patch.text}")

    # final get
    final_url = f"{BASE}/api/v1/codes-order/{order_id}"
    r_final = session.get(final_url, timeout=30)
    log["steps"].append({"step":"final_get", "status": r_final.status_code, "text": r_final.text})
    LAST_MULTI_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
    if r_final.status_code == 200:
        try:
            return r_final.json()
        except Exception:
            return {"order_id": order_id, "note": "created but final GET returned non-json", "text": r_final.text}
    return {"order_id": order_id, "note": "created but final GET failed", "status": r_final.status_code, "text": r_final.text}

# ---------------- main ----------------
def main():
    # 1) try load cookies file
    cookies = load_cookies()
    if not cookies:
        # try import get_cookies from get_cookies
        try:
            from cookies import get_cookies as external_collect  # type: ignore
            print("Calling get_cookiesesin cookies...")
            cookies = external_collect()
        except Exception as e:
            print("Cannot import/call get_cookies module:", e)
            print("Either run get_cookies.py manually or fix import.")
            return

    if not cookies:
        print("Cookies not obtained; aborting.")
        return

    print("Cookies keys:", list(cookies.keys()))
    session = make_session_with_cookies(cookies)

    # try single POST
    resp = try_single_post(session, WAREHOUSE_ID, DOCUMENT_NUMBER, PRODUCT_GROUP, RELEASE_METHOD_TYPE, POSITIONS, cis_type=CIS_TYPE, filling_method=FILLING_METHOD)
    if resp is not None and resp.status_code in (200,201):
        print("Single POST succeeded. Response:")
        try:
            print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
        except Exception:
            print(resp.text)
        return
    if resp is not None:
        print("Single POST failed:", resp.status_code, resp.text[:1000])

    # fallback multistep
    print("Falling back to multistep flow...")
    try:
        res = create_order_multistep(session, WAREHOUSE_ID, DOCUMENT_NUMBER, PRODUCT_GROUP, RELEASE_METHOD_TYPE, POSITIONS, cis_type=CIS_TYPE, filling_method=FILLING_METHOD)
        print("Multistep result:")
        try:
            print(json.dumps(res, ensure_ascii=False, indent=2))
        except Exception:
            print(res)
    except Exception as e:
        print("Multistep failed:", e)

if __name__ == "__main__":
    main()
