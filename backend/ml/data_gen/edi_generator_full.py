#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
import os
import random
import re
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# project_root = .../neurobiz-proj
PROJECT_ROOT = Path(__file__).resolve().parents[5]
GOLDEN_SCHEMAS_DIR = PROJECT_ROOT / "backend" / "golden_schemas"

import numpy as np

# -----------------------------
# X12 Defaults
# -----------------------------
DEFAULT_SEGMENT_TERMINATOR = "~"
DEFAULT_ELEMENT_SEPARATOR = "*"
COMPOSITE_SEPARATOR = ":"

# -----------------------------
# Masters (you can swap with your real masters later)
# -----------------------------
SUPPLIERS = [
    "SUPPLIER001",
    "SUPP_ACE_MFG",
    "WIDGET_CO",
    "ACME_PARTS",
    "INDUSTRIAL_GOODS_LTD",
    "TECH_SUPPLY_INT",
    "LOGISTICS_PLUS",
    "PREMIUM_GOODS",
]

BUYERS = [
    "BUYER_RETAIL_A",
    "BUYER_DISTRIB_B",
    "BUYER_WAREHOUSE_C",
    "BUYER_CHAIN_D",
    "BUYER_ECOMM_E",
]

SKUS = [
    "SKU-10001",
    "SKU-10002",
    "SKU-10003",
    "SKU-20001",
    "SKU-20002",
    "SKU-30001",
    "PART-XYZ-100",
    "PART-ABC-200",
    "WIDGET-BLUE-SM",
    "WIDGET-RED-LG",
]

UNITS_OF_MEASURE = ["EA", "CS", "DZ", "BOX", "PLT", "CT"]
PAYMENT_TERMS = ["NET30", "NET45", "NET60", "2%10NET30"]
CARRIERS = ["UPS", "FEDEX", "DHL", "XPO", "OLD_DOMINION", "JB_HUNT"]

LOCATIONS = [
    {"location_code": "WH-NE-01", "name": "Northeast DC", "city": "Newark", "state": "NJ", "timezone": "America/New_York"},
    {"location_code": "WH-SE-01", "name": "Southeast DC", "city": "Atlanta", "state": "GA", "timezone": "America/New_York"},
    {"location_code": "WH-MW-01", "name": "Midwest DC", "city": "Chicago", "state": "IL", "timezone": "America/Chicago"},
    {"location_code": "WH-W-01", "name": "West DC", "city": "Reno", "state": "NV", "timezone": "America/Los_Angeles"},
]

CURRENCY_CODES = ["USD"]

# ------------------------------------------------------------
# OPTION B Label Set (business oriented + demo-friendly)
# ------------------------------------------------------------
LABELS_OPTION_B = [
    "NORMAL",
    "MISSING_DOC",                 # missing ASN or Invoice
    "THREE_WAY_QTY_MISMATCH",       # PO vs ASN vs INV qty mismatch
    "THREE_WAY_PRICE_MISMATCH",     # PO vs INV unit price mismatch (or INV ext mismatch)
    "LATE_SHIPMENT",               # ASN ship date late vs PO expected
    "SHORT_SHIP",                  # ASN qty < PO qty beyond tolerance
    "OVERBILL",                    # INV qty/price causes overbill beyond tolerance
    "CHARGES_ANOMALY",             # freight/discount/tax ratios weird
    "DUPLICATE_DOC",               # duplicate ASN/INV patterns
]

# ------------------------------------------------------------
# Realism configuration (tune as needed)
# ------------------------------------------------------------
CFG = {
    # volume / drift
    "history_days": 120,
    "recent_days": 21,
    "recent_qty_mult": 1.10,

    # base distributions (fallback if no golden parsed)
    "avg_line_items": 6,
    "line_items_max": 14,

    "qty_mean": 120.0,
    "qty_std": 70.0,
    "qty_max": 6000,

    "price_mean": 50.0,
    "price_std": 18.0,
    "price_max": 2500.0,

    # lead time + shipping behavior
    "supplier_lead_days_min": 2,
    "supplier_lead_days_max": 14,
    "ship_jitter_mean": 0.0,
    "ship_jitter_std": 1.2,
    "ship_jitter_min": -2,
    "ship_jitter_max": 4,

    # invoice timing
    "invoice_after_ship_days_min": 0,
    "invoice_after_ship_days_max": 10,

    # charges (percent of subtotal)
    "freight_pct_mean": 0.028,
    "freight_pct_std": 0.012,
    "discount_pct_mean": 0.015,
    "discount_pct_std": 0.010,
    "tax_pct_mean": 0.020,
    "tax_pct_std": 0.008,

    # missing docs / dup docs
    "p_missing_asn": 0.03,
    "p_missing_invoice": 0.02,
    "p_duplicate_doc": 0.03,

    # tolerance profiles (context-aware tolerance)
    # These are not hard rules; you use them as *context* for features/labels.
    "tol_profiles": [
        {"id": "STRICT", "qty_pct": 0.01, "price_pct": 0.005, "charge_pct": 0.01},
        {"id": "STANDARD", "qty_pct": 0.02, "price_pct": 0.01, "charge_pct": 0.02},
        {"id": "LOOSE", "qty_pct": 0.05, "price_pct": 0.02, "charge_pct": 0.04},
    ],

    # anomaly magnitudes (beyond tolerance)
    "anom_qty_mult_min": 1.05,
    "anom_qty_mult_max": 1.40,
    "anom_price_mult_min": 1.02,
    "anom_price_mult_max": 1.25,
    "anom_charge_mult_min": 1.6,
    "anom_charge_mult_max": 4.0,

    # severity mapping knobs
    "sev_low_risk_max": 0.35,
    "sev_med_risk_max": 0.70,
}

# -----------------------------
# Helpers
# -----------------------------
def _p(msg: str) -> None:
    print(msg, flush=True)

def _stable_seed_from_str(s: str) -> int:
    b = (s or "").encode("utf-8", errors="ignore")
    return int(zlib.crc32(b) & 0xFFFFFFFF)

def _ctrl9(n: int) -> str:
    return str(int(n) % 1000000000).zfill(9)

def _ctrl_st(n: int) -> str:
    return str(int(n) % 10000).zfill(4)

_INTERCHANGE_COUNTER = 0

def _make_interchange_ids(seed: Optional[int] = None) -> Tuple[str, str, str]:
    global _INTERCHANGE_COUNTER
    _INTERCHANGE_COUNTER += 1
    base = int(time.time() * 1000)
    if seed is not None:
        base += int(seed)
    base += (_INTERCHANGE_COUNTER % 1000000)
    isa_ctrl = _ctrl9(base)
    gs_ctrl = str((base // 10) % 100000)
    st_ctrl = _ctrl_st(base // 100)
    return isa_ctrl, gs_ctrl, st_ctrl

def _now_isa_date_time() -> Tuple[str, str]:
    now = datetime.now()
    return now.strftime("%y%m%d"), now.strftime("%H%M")

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _sha1(s: str) -> str:
    try:
        return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()
    except Exception:
        return ""

def detect_terminator_and_separator(content: str) -> Tuple[str, str]:
    content = content.strip()
    elem_sep = DEFAULT_ELEMENT_SEPARATOR
    if content.startswith("ISA") and len(content) >= 4:
        elem_sep = content[3]
    candidates = ["~", "\n"]
    best = DEFAULT_SEGMENT_TERMINATOR
    best_score = -1
    for cand in candidates:
        segs = content.split(cand)
        joined = " ".join(segs[:50])
        score = 0
        if "GS" in joined: score += 2
        if "ST" in joined: score += 2
        if len(segs) > 10: score += 1
        if score > best_score:
            best_score = score
            best = cand
    return best, elem_sep

def split_segments(content: str) -> List[str]:
    content = content.strip()
    if "~" in content:
        segs = content.split("~")
    else:
        segs = re.split(r"[\r\n]+", content)
    return [s.strip() for s in segs if s.strip()]

# -----------------------------
# Simple X12 Parsers for golden dist extraction
# (We only need a few segments to estimate distributions)
# -----------------------------
class X12Parser:
    def __init__(self):
        self.segment_terminator = DEFAULT_SEGMENT_TERMINATOR
        self.element_separator = DEFAULT_ELEMENT_SEPARATOR

    def parse_text(self, content: str) -> List[Dict]:
        seg_term, elem_sep = detect_terminator_and_separator(content)
        self.segment_terminator = seg_term
        self.element_separator = elem_sep
        out: List[Dict] = []
        for raw_seg in split_segments(content):
            els = raw_seg.split(self.element_separator)
            tag = els[0].strip() if els else ""
            if tag:
                out.append({"tag": tag, "elements": els, "raw": raw_seg})
        return out

    def parse_file(self, path: Path) -> List[Dict]:
        content = path.read_text(encoding="utf-8", errors="ignore")
        return self.parse_text(content)

    def tx_type(self, segs: List[Dict]) -> Optional[str]:
        for s in segs:
            if s["tag"] == "ST" and len(s["elements"]) > 1:
                return s["elements"][1].strip()
        return None

    def extract_850(self, segs: List[Dict]) -> Optional[Dict[str, Any]]:
        po_number = None
        line_items = []
        for s in segs:
            if s["tag"] == "BEG" and len(s["elements"]) > 3:
                po_number = s["elements"][3].strip()
            if s["tag"] == "PO1":
                # PO1*1*QTY*UOM*PRICE****SKU
                qty = s["elements"][2].strip() if len(s["elements"]) > 2 else None
                price = s["elements"][4].strip() if len(s["elements"]) > 4 else None
                sku = None
                if len(s["elements"]) > 8 and s["elements"][8].strip():
                    sku = s["elements"][8].strip()
                elif len(s["elements"]) > 7 and s["elements"][7].strip():
                    sku = s["elements"][7].strip()
                if sku:
                    line_items.append({"sku": sku, "quantity": qty, "unit_price": price})
        if not po_number or not line_items:
            return None
        return {"po_number": po_number, "line_items": line_items}

    def extract_856(self, segs: List[Dict]) -> Optional[Dict[str, Any]]:
        # We'll look for BSN and SN1 quantities, LIN sku
        bsn = None
        ship_date = None
        items = []
        current_sku = None
        for s in segs:
            if s["tag"] == "BSN" and len(s["elements"]) > 2:
                bsn = s["elements"][2].strip()
            if s["tag"] == "DTM" and len(s["elements"]) > 2:
                # DTM*011*YYYYMMDD (ship date commonly 011)
                if s["elements"][1].strip() in ("011", "017"):
                    ship_date = s["elements"][2].strip()
            if s["tag"] == "LIN":
                # LIN**BP*SKU
                for i in range(1, len(s["elements"]) - 1):
                    if s["elements"][i].strip() in ("BP", "SK", "VP"):
                        current_sku = s["elements"][i + 1].strip()
                        break
            if s["tag"] == "SN1":
                # SN1**QTY*UOM
                q = s["elements"][2].strip() if len(s["elements"]) > 2 else None
                u = s["elements"][3].strip() if len(s["elements"]) > 3 else None
                if current_sku:
                    items.append({"sku": current_sku, "ship_qty": q, "uom": u})
        if not items:
            return None
        return {"bsn": bsn, "ship_date": ship_date, "line_items": items}

    def extract_810(self, segs: List[Dict]) -> Optional[Dict[str, Any]]:
        inv_number = None
        items = []
        for s in segs:
            if s["tag"] == "BIG" and len(s["elements"]) > 2:
                inv_number = s["elements"][2].strip()
            if s["tag"] == "IT1":
                # IT1*1*QTY*UOM*UNITPRICE**BP*SKU
                qty = s["elements"][2].strip() if len(s["elements"]) > 2 else None
                price = s["elements"][4].strip() if len(s["elements"]) > 4 else None
                sku = None
                for i in range(1, len(s["elements"]) - 1):
                    if s["elements"][i].strip() in ("BP", "SK", "VP"):
                        sku = s["elements"][i + 1].strip()
                        break
                if sku:
                    items.append({"sku": sku, "quantity": qty, "unit_price": price})
        if not inv_number or not items:
            return None
        return {"invoice_number": inv_number, "line_items": items}

# -----------------------------
# Distribution extraction
# -----------------------------
@dataclass
class Dist:
    avg_lines: int = CFG["avg_line_items"]
    qty_mean: float = CFG["qty_mean"]
    qty_std: float = CFG["qty_std"]
    price_mean: float = CFG["price_mean"]
    price_std: float = CFG["price_std"]

def extract_distributions_from_golden(golden_dir: Path) -> Dist:
    parser = X12Parser()
    if not golden_dir.exists():
        _p(f"[WARN] Golden dir not found: {golden_dir}. Using defaults.")
        return Dist()

    qtys = []
    prices = []
    line_counts = []

    files = [p for p in golden_dir.rglob("*") if p.is_file()]
    if not files:
        _p(f"[WARN] No golden files under: {golden_dir}. Using defaults.")
        return Dist()

    for f in files:
        if f.suffix.lower() in (".pdf", ".png", ".jpg", ".jpeg"):
            continue
        try:
            segs = parser.parse_file(f)
            tx = parser.tx_type(segs)
            if tx == "850":
                po = parser.extract_850(segs)
                if not po:
                    continue
                items = po.get("line_items") or []
                line_counts.append(len(items))
                for it in items:
                    q = _safe_float(it.get("quantity"), None)
                    p = _safe_float(it.get("unit_price"), None)
                    if q and q > 0: qtys.append(q)
                    if p and p > 0: prices.append(p)
        except Exception:
            continue

    if not line_counts:
        return Dist()

    avg_lines = int(np.clip(np.mean(line_counts), 1, CFG["line_items_max"]))
    qty_mean = float(np.mean(qtys)) if qtys else CFG["qty_mean"]
    qty_std = float(np.std(qtys)) if qtys else CFG["qty_std"]
    price_mean = float(np.mean(prices)) if prices else CFG["price_mean"]
    price_std = float(np.std(prices)) if prices else CFG["price_std"]

    return Dist(
        avg_lines=avg_lines,
        qty_mean=float(np.clip(qty_mean, 1, CFG["qty_max"])),
        qty_std=float(max(1.0, min(qty_std, CFG["qty_max"]))),
        price_mean=float(np.clip(price_mean, 1, CFG["price_max"])),
        price_std=float(max(0.5, min(price_std, CFG["price_max"]))),
    )

# -----------------------------
# Master data
# -----------------------------
def build_master(dist: Dist, seed: int) -> Dict[str, Any]:
    random.seed(seed)
    np.random.seed(seed)

    supplier_master = []
    for s in SUPPLIERS:
        supplier_master.append({
            "supplier_code": s,
            "supplier_name": s.replace("_", " ").title(),
            "lead_time_days": int(random.randint(CFG["supplier_lead_days_min"], CFG["supplier_lead_days_max"])),
            "default_payment_terms": random.choice(PAYMENT_TERMS),
            "preferred_carrier": random.choice(CARRIERS),
            "default_tol_profile": random.choice(CFG["tol_profiles"])["id"],
        })

    buyer_master = []
    for b in BUYERS:
        buyer_master.append({
            "buyer_code": b,
            "buyer_name": b.replace("_", " ").title(),
            "default_ship_to": random.choice(LOCATIONS)["location_code"],
            "default_bill_to": random.choice(LOCATIONS)["location_code"],
        })

    item_master = [{"sku": sku, "description": sku.replace("-", " ").title()} for sku in SKUS]

    # simple “pricing contracts”
    pricing = []
    today = datetime.now().date()
    for s in SUPPLIERS:
        for sku in SKUS:
            base_price = float(np.clip(np.random.normal(dist.price_mean, max(1.0, dist.price_mean * 0.20)), 1, CFG["price_max"]))
            discount_pct = float(np.clip(np.random.normal(0.03, 0.02), 0.0, 0.15))
            pricing.append({
                "supplier_code": s,
                "sku": sku,
                "contract_unit_price": round(base_price, 2),
                "discount_pct": round(discount_pct, 4),
                "currency": "USD",
                "effective_start": str(today - timedelta(days=180)),
                "effective_end": str(today + timedelta(days=180)),
            })

    return {
        "supplier_master": supplier_master,
        "buyer_master": buyer_master,
        "item_master": item_master,
        "pricing_contracts": pricing,
        "location_master": [dict(x) for x in LOCATIONS],
        "tol_profiles": CFG["tol_profiles"],
    }

# -----------------------------
# Core generator: PO -> ASN -> Invoice
# -----------------------------
class OptionBGenerator:
    def __init__(self, *, dist: Dist, master: Dict[str, Any], seed: int):
        self.dist = dist
        self.master = master
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)

        self._supplier_lookup = {s["supplier_code"]: s for s in self.master["supplier_master"]}
        self._buyer_lookup = {b["buyer_code"]: b for b in self.master["buyer_master"]}
        self._price_lookup = {(p["supplier_code"], p["sku"]): p for p in self.master["pricing_contracts"]}
        self._tol_lookup = {t["id"]: t for t in self.master["tol_profiles"]}

    def _choose_tol_profile(self, supplier_code: str) -> str:
        s = self._supplier_lookup.get(supplier_code, {})
        pid = str(s.get("default_tol_profile") or "STANDARD")
        return pid if pid in self._tol_lookup else "STANDARD"

    def _derive_charges(self, subtotal: float) -> Dict[str, float]:
        subtotal = max(0.01, float(subtotal))
        freight_pct = float(np.clip(np.random.normal(CFG["freight_pct_mean"], CFG["freight_pct_std"]), 0.0, 0.25))
        disc_pct = float(np.clip(np.random.normal(CFG["discount_pct_mean"], CFG["discount_pct_std"]), 0.0, 0.35))
        tax_pct = float(np.clip(np.random.normal(CFG["tax_pct_mean"], CFG["tax_pct_std"]), 0.0, 0.25))
        return {
            "freight_amount": round(subtotal * freight_pct, 2),
            "discount_amount": round(subtotal * disc_pct, 2),
            "tax_amount": round(subtotal * tax_pct, 2),
        }

    def _make_po(self, i: int) -> Dict[str, Any]:
        n_lines = int(np.clip(np.random.poisson(lam=self.dist.avg_lines), 1, CFG["line_items_max"]))
        buyer_code = random.choice(BUYERS)
        supplier_code = random.choice(SUPPLIERS)

        # spread across history window
        order_dt = datetime.now() - timedelta(days=random.randint(0, CFG["history_days"]))
        is_recent = order_dt > (datetime.now() - timedelta(days=CFG["recent_days"]))
        qty_mult = CFG["recent_qty_mult"] if is_recent else 1.0

        supplier = self._supplier_lookup.get(supplier_code, {})
        buyer = self._buyer_lookup.get(buyer_code, {})
        lead = int(supplier.get("lead_time_days", 7))

        jitter = int(np.clip(
            np.random.normal(CFG["ship_jitter_mean"], CFG["ship_jitter_std"]),
            CFG["ship_jitter_min"], CFG["ship_jitter_max"]
        ))
        expected_ship_dt = order_dt + timedelta(days=max(1, lead + jitter))

        po_number = f"PO-{int(time.time() * 1000) % 1000000}-{i}"

        ship_to = buyer.get("default_ship_to") or random.choice(LOCATIONS)["location_code"]
        bill_to = buyer.get("default_bill_to") or random.choice(LOCATIONS)["location_code"]
        pay_terms = supplier.get("default_payment_terms") or random.choice(PAYMENT_TERMS)
        carrier = supplier.get("preferred_carrier") or random.choice(CARRIERS)

        line_items = []
        subtotal = 0.0

        for ln in range(1, n_lines + 1):
            sku = random.choice(SKUS)
            c = self._price_lookup.get((supplier_code, sku), {})
            contract_price = float(c.get("contract_unit_price", self.dist.price_mean))
            discount_pct = float(c.get("discount_pct", 0.0))

            qty = int(np.clip(np.random.normal(self.dist.qty_mean * qty_mult, max(5.0, self.dist.qty_std)), 1, CFG["qty_max"]))
            raw_price = float(np.clip(np.random.normal(contract_price, max(0.5, contract_price * 0.05)), 0.01, CFG["price_max"]))
            unit_price = round(raw_price * (1.0 - discount_pct), 2)

            line_items.append({
                "line_number": ln,
                "sku": sku,
                "quantity": qty,
                "unit_of_measure": random.choice(UNITS_OF_MEASURE),
                "unit_price": unit_price,
                "contract_unit_price": round(contract_price, 2),
                "discount_pct": round(discount_pct, 4),
            })
            subtotal += qty * unit_price

        charges = self._derive_charges(subtotal)

        tol_profile_id = self._choose_tol_profile(supplier_code)

        po = {
            "po_id": str(uuid.uuid4()),
            "po_number": po_number,
            "buyer_code": buyer_code,
            "supplier_code": supplier_code,
            "order_date": order_dt.isoformat(),
            "expected_ship_date": expected_ship_dt.isoformat(),
            "ship_to_location": ship_to,
            "bill_to_location": bill_to,
            "payment_terms": pay_terms,
            "currency": random.choice(CURRENCY_CODES),
            "carrier_code": carrier,
            "tolerance_profile_id": tol_profile_id,
            "line_items": line_items,
            **charges,
        }
        return po

    def _make_asn_from_po(self, po: Dict[str, Any]) -> Dict[str, Any]:
        # baseline: ships all quantities on/near expected date
        expected = datetime.fromisoformat(po["expected_ship_date"])
        ship_dt = expected + timedelta(days=random.randint(-1, 2))

        asn = {
            "asn_id": str(uuid.uuid4()),
            "asn_number": f"ASN-{po['po_number']}",
            "po_number": po["po_number"],
            "buyer_code": po["buyer_code"],
            "supplier_code": po["supplier_code"],
            "ship_date": ship_dt.isoformat(),
            "carrier_code": po.get("carrier_code"),
            "ship_to_location": po.get("ship_to_location"),
            "line_items": [],
        }
        for li in po["line_items"]:
            asn["line_items"].append({
                "line_number": li["line_number"],
                "sku": li["sku"],
                "ship_qty": int(li["quantity"]),
                "unit_of_measure": li["unit_of_measure"],
            })
        return asn

    def _make_invoice_from_po_asn(self, po: Dict[str, Any], asn: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        # baseline: invoices shipped qty at PO price
        if asn:
            ship_dt = datetime.fromisoformat(asn["ship_date"])
        else:
            ship_dt = datetime.fromisoformat(po["expected_ship_date"])
        inv_dt = ship_dt + timedelta(days=random.randint(CFG["invoice_after_ship_days_min"], CFG["invoice_after_ship_days_max"]))

        inv = {
            "invoice_id": str(uuid.uuid4()),
            "invoice_number": f"INV-{po['po_number']}",
            "po_number": po["po_number"],
            "buyer_code": po["buyer_code"],
            "supplier_code": po["supplier_code"],
            "invoice_date": inv_dt.isoformat(),
            "currency": po.get("currency", "USD"),
            "line_items": [],
            # invoice charges can differ slightly but should be close in NORMAL
            "freight_amount": float(po.get("freight_amount") or 0.0),
            "discount_amount": float(po.get("discount_amount") or 0.0),
            "tax_amount": float(po.get("tax_amount") or 0.0),
        }

        # map ASN qty by sku if present
        asn_qty = {}
        if asn:
            for x in asn.get("line_items") or []:
                asn_qty[str(x["sku"])] = int(x.get("ship_qty") or 0)

        subtotal = 0.0
        for li in po["line_items"]:
            sku = li["sku"]
            qty = asn_qty.get(sku, int(li["quantity"])) if asn else int(li["quantity"])
            price = float(li["unit_price"])
            inv["line_items"].append({
                "line_number": li["line_number"],
                "sku": sku,
                "quantity": int(qty),
                "unit_of_measure": li["unit_of_measure"],
                "unit_price": round(price, 2),
            })
            subtotal += qty * price

        inv["subtotal_amount"] = round(subtotal, 2)
        inv["total_amount"] = round(subtotal + float(inv["freight_amount"]) + float(inv["tax_amount"]) - float(inv["discount_amount"]), 2)
        return inv

    # ------------------------------------------------------------
    # 3-way mismatch injection (Option B)
    # ------------------------------------------------------------
    def _apply_anomaly(self, *, po: Dict[str, Any], asn: Optional[Dict[str, Any]], inv: Optional[Dict[str, Any]], label: str) -> Tuple[Optional[Dict], Optional[Dict], Optional[Dict], Dict[str, Any]]:
        """
        Returns possibly modified (po, asn, inv) and label payload:
        - reason_codes: list[str]
        - owner_team: str
        - recommended_action: str
        - estimated_dollar_impact: float
        - risk_score: float (0-1)
        - severity: LOW/MED/HIGH
        """
        tol_id = str(po.get("tolerance_profile_id") or "STANDARD")
        tol = self._tol_lookup.get(tol_id, self._tol_lookup["STANDARD"])

        # helper: compute impacts
        def po_subtotal(p: Dict[str, Any]) -> float:
            s = 0.0
            for li in p.get("line_items") or []:
                s += float(li["quantity"]) * float(li["unit_price"])
            return float(max(0.0, s))

        base_sub = po_subtotal(po)
        base_total = base_sub + float(po.get("freight_amount") or 0.0) + float(po.get("tax_amount") or 0.0) - float(po.get("discount_amount") or 0.0)
        base_total = float(max(0.0, base_total))

        reason_codes: List[str] = []
        owner_team = "OPERATIONS"
        action = "REVIEW"
        impact = 0.0

        if label == "NORMAL":
            pass

        elif label == "MISSING_DOC":
            # randomly drop ASN or INV (not both)
            if random.random() < 0.55:
                asn = None
                reason_codes.append("MISSING_ASN")
                owner_team = "LOGISTICS"
                action = "REQUEST_ASN_PROOF"
            else:
                inv = None
                reason_codes.append("MISSING_INVOICE")
                owner_team = "AP"
                action = "REQUEST_INVOICE"
            impact = base_total * 0.15

        elif label == "LATE_SHIPMENT":
            if asn:
                exp = datetime.fromisoformat(po["expected_ship_date"])
                late_days = random.randint(3, 18)
                asn["ship_date"] = (exp + timedelta(days=late_days)).isoformat()
                reason_codes.append(f"LATE_SHIP_{late_days}D")
                owner_team = "LOGISTICS"
                action = "EXPEDITE_OR_ESCALATE"
                impact = base_total * min(0.30, 0.02 * late_days)
            else:
                # if missing ASN, convert to missing-doc story
                return self._apply_anomaly(po=po, asn=asn, inv=inv, label="MISSING_DOC")

        elif label in ("SHORT_SHIP", "THREE_WAY_QTY_MISMATCH"):
            # make ASN ship qty different vs PO
            if not asn:
                return self._apply_anomaly(po=po, asn=asn, inv=inv, label="MISSING_DOC")

            k = random.randint(0, len(asn["line_items"]) - 1)
            li_asn = asn["line_items"][k]
            sku = li_asn["sku"]
            po_line = next((x for x in po["line_items"] if x["sku"] == sku), None)
            if not po_line:
                return po, asn, inv, self._make_label_payload(["QTY_MISMATCH"], "LOGISTICS", "RECONCILE_QTY", base_total * 0.10)

            q_po = int(po_line["quantity"])
            # push beyond tolerance
            mult = random.uniform(CFG["anom_qty_mult_min"], CFG["anom_qty_mult_max"])
            if label == "SHORT_SHIP" or random.random() < 0.65:
                q_asn = max(0, int(round(q_po / mult)))
                reason_codes.append("SHORT_SHIP_BEYOND_TOL")
                owner_team = "LOGISTICS"
                action = "FILE_SHORTAGE_CLAIM"
            else:
                q_asn = int(round(q_po * mult))
                reason_codes.append("OVER_SHIP_BEYOND_TOL")
                owner_team = "RECEIVING"
                action = "VERIFY_RECEIPT"

            li_asn["ship_qty"] = int(q_asn)

            # invoice follows ASN qty in NORMAL; for mismatch, sometimes invoice follows PO
            if inv:
                for li in inv["line_items"]:
                    if li["sku"] == sku:
                        if random.random() < 0.55:
                            li["quantity"] = int(q_po)   # invoice matches PO, not ASN
                            reason_codes.append("INV_QTY_MATCHES_PO_NOT_ASN")
                        else:
                            li["quantity"] = int(q_asn)  # invoice matches ASN
                        break

            impact = abs((q_po - q_asn) * float(po_line["unit_price"]))
            impact = max(impact, base_total * 0.05)

        elif label in ("OVERBILL", "THREE_WAY_PRICE_MISMATCH"):
            # invoice price deviates beyond tolerance
            if not inv:
                return self._apply_anomaly(po=po, asn=asn, inv=inv, label="MISSING_DOC")

            k = random.randint(0, len(inv["line_items"]) - 1)
            li_inv = inv["line_items"][k]
            sku = li_inv["sku"]
            po_line = next((x for x in po["line_items"] if x["sku"] == sku), None)
            if not po_line:
                return po, asn, inv, self._make_label_payload(["PRICE_MISMATCH"], "AP", "HOLD_PAYMENT", base_total * 0.10)

            p_po = float(po_line["unit_price"])
            tol_price = float(tol["price_pct"])
            mult = random.uniform(CFG["anom_price_mult_min"], CFG["anom_price_mult_max"])
            # ensure beyond tolerance
            if mult < (1.0 + tol_price + 0.002):
                mult = 1.0 + tol_price + 0.02 + random.random() * 0.05

            new_price = round(p_po * mult, 2)
            li_inv["unit_price"] = new_price
            reason_codes.append("INVOICE_UNIT_PRICE_ABOVE_TOL")

            owner_team = "AP"
            action = "DISPUTE_INVOICE_OR_REQUEST_CREDIT_MEMO"

            qty = int(li_inv.get("quantity") or 0)
            impact = max(0.0, (new_price - p_po) * qty)

        elif label == "CHARGES_ANOMALY":
            if not inv:
                return self._apply_anomaly(po=po, asn=asn, inv=inv, label="MISSING_DOC")
            mult = random.uniform(CFG["anom_charge_mult_min"], CFG["anom_charge_mult_max"])
            which = random.choice(["freight", "tax", "discount", "combo"])

            if which in ("freight", "combo"):
                inv["freight_amount"] = round(float(inv.get("freight_amount") or 0.0) * mult + 10.0, 2)
                reason_codes.append("FREIGHT_OUTSIDE_PROFILE")
            if which in ("tax", "combo"):
                inv["tax_amount"] = round(float(inv.get("tax_amount") or 0.0) * mult + 5.0, 2)
                reason_codes.append("TAX_OUTSIDE_PROFILE")
            if which in ("discount", "combo"):
                inv["discount_amount"] = round(float(inv.get("discount_amount") or 0.0) * mult, 2)
                reason_codes.append("DISCOUNT_OUTSIDE_PROFILE")

            owner_team = "AP"
            action = "RECONCILE_CHARGES_WITH_CONTRACT"

            # impact ~ net additional charges
            inv_sub = float(inv.get("subtotal_amount") or base_sub)
            inv_total = inv_sub + float(inv.get("freight_amount") or 0.0) + float(inv.get("tax_amount") or 0.0) - float(inv.get("discount_amount") or 0.0)
            impact = max(0.0, inv_total - base_total)

        elif label == "DUPLICATE_DOC":
            # duplicates are handled in outer generation; here we just label payload
            reason_codes.append("DUPLICATE_DOCUMENT_PATTERN")
            owner_team = "OPERATIONS"
            action = "DEDUPE_AND_CONFIRM_VALID_DOC"
            impact = base_total * 0.10

        else:
            # fallback to NORMAL if unknown
            label = "NORMAL"

        payload = self._make_label_payload(reason_codes, owner_team, action, impact)
        return po, asn, inv, payload

    def _make_label_payload(self, reason_codes: List[str], owner_team: str, action: str, impact: float) -> Dict[str, Any]:
        impact = float(max(0.0, impact))

        # risk_score: bounded smooth mapping from impact + reason count
        # (Not perfect, but stable and demo-friendly. Later: learn this from outcomes.)
        base = 1.0 - np.exp(-impact / 2500.0)  # saturates with $ scale
        base += 0.05 * min(6, len(reason_codes))
        risk = float(np.clip(base, 0.0, 1.0))

        if risk <= CFG["sev_low_risk_max"]:
            severity = "LOW"
        elif risk <= CFG["sev_med_risk_max"]:
            severity = "MED"
        else:
            severity = "HIGH"

        return {
            "reason_codes": reason_codes,
            "owner_team": owner_team,
            "recommended_action": action,
            "estimated_dollar_impact": impact,
            "risk_score": risk,
            "severity": severity,
        }

    # ------------------------------------------------------------
    # Document writers (minimal, valid-enough X12 for demos)
    # ------------------------------------------------------------
    def render_850(self, po: Dict[str, Any]) -> str:
        isa_date, isa_time = _now_isa_date_time()
        isa_ctrl, gs_ctrl, st_ctrl = _make_interchange_ids(seed=_stable_seed_from_str(str(po["po_number"])))
        beg_date = _yyyymmdd(datetime.fromisoformat(po["order_date"]))

        tx = []
        tx.append(f"ST*850*{st_ctrl}")
        tx.append(f"BEG*00*SA*{po['po_number']}*{beg_date}")
        tx.append(f"N1*BY*{po['buyer_code']}")
        tx.append(f"N1*SU*{po['supplier_code']}")
        tx.append(f"ITD*01******{po.get('payment_terms','NET30')}")

        # charges on PO (optional)
        if po.get("freight_amount") is not None:
            tx.append(f"SAC*C*FREIGHT***{float(po['freight_amount']):.2f}")
        if po.get("discount_amount") is not None and float(po["discount_amount"]) > 0:
            tx.append(f"SAC*A*DISCOUNT***{float(po['discount_amount']):.2f}")
        if po.get("tax_amount") is not None and float(po["tax_amount"]) > 0:
            tx.append(f"SAC*C*TAX***{float(po['tax_amount']):.2f}")

        for i, li in enumerate(po["line_items"], 1):
            tx.append(f"PO1*{i}*{li['quantity']}*{li['unit_of_measure']}*{li['unit_price']}****{li['sku']}")

        tx.append(f"CTT*{len(po['line_items'])}")
        se_count = len(tx) + 1
        tx.append(f"SE*{se_count}*{st_ctrl}")

        hdr = []
        hdr.append(f"ISA*00*          *00*          *ZZ*SENDER_ID       *ZZ*RECEIVER_ID     *{isa_date}*{isa_time}*U*00400*{isa_ctrl}*0*P*:")
        hdr.append(f"GS*PO*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{datetime.now().strftime('%H%M')}*{gs_ctrl}*X*004010")
        lines = hdr + tx + [f"GE*1*{gs_ctrl}", f"IEA*1*{isa_ctrl}"]
        return "~".join(lines) + "~"

    def render_856(self, asn: Dict[str, Any]) -> str:
        isa_date, isa_time = _now_isa_date_time()
        isa_ctrl, gs_ctrl, st_ctrl = _make_interchange_ids(seed=_stable_seed_from_str(str(asn["asn_number"])))
        ship_date = _yyyymmdd(datetime.fromisoformat(asn["ship_date"]))

        tx = []
        tx.append(f"ST*856*{st_ctrl}")
        tx.append(f"BSN*00*{asn['asn_number']}*{ship_date}*{datetime.now().strftime('%H%M')}")
        tx.append(f"DTM*011*{ship_date}")
        tx.append(f"TD5*****{asn.get('carrier_code','UPS')}")

        # minimal HL/line info (not full compliance, but consistent)
        for i, li in enumerate(asn["line_items"], 1):
            tx.append(f"HL*{i}**I")
            tx.append(f"LIN**BP*{li['sku']}")
            tx.append(f"SN1**{li['ship_qty']}*{li.get('unit_of_measure','EA')}")

        tx.append(f"CTT*{len(asn['line_items'])}")
        se_count = len(tx) + 1
        tx.append(f"SE*{se_count}*{st_ctrl}")

        hdr = []
        hdr.append(f"ISA*00*          *00*          *ZZ*SENDER_ID       *ZZ*RECEIVER_ID     *{isa_date}*{isa_time}*U*00400*{isa_ctrl}*0*P*:")
        hdr.append(f"GS*SH*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{datetime.now().strftime('%H%M')}*{gs_ctrl}*X*004010")
        lines = hdr + tx + [f"GE*1*{gs_ctrl}", f"IEA*1*{isa_ctrl}"]
        return "~".join(lines) + "~"

    def render_810(self, inv: Dict[str, Any]) -> str:
        isa_date, isa_time = _now_isa_date_time()
        isa_ctrl, gs_ctrl, st_ctrl = _make_interchange_ids(seed=_stable_seed_from_str(str(inv["invoice_number"])))
        inv_date = _yyyymmdd(datetime.fromisoformat(inv["invoice_date"]))

        tx = []
        tx.append(f"ST*810*{st_ctrl}")
        tx.append(f"BIG*{inv_date}*{inv['invoice_number']}")
        tx.append(f"N1*BY*{inv['buyer_code']}")
        tx.append(f"N1*SU*{inv['supplier_code']}")

        # invoice charges
        if inv.get("freight_amount") is not None:
            tx.append(f"SAC*C*FREIGHT***{float(inv['freight_amount']):.2f}")
        if inv.get("discount_amount") is not None and float(inv["discount_amount"]) > 0:
            tx.append(f"SAC*A*DISCOUNT***{float(inv['discount_amount']):.2f}")
        if inv.get("tax_amount") is not None and float(inv["tax_amount"]) > 0:
            tx.append(f"SAC*C*TAX***{float(inv['tax_amount']):.2f}")

        for i, li in enumerate(inv["line_items"], 1):
            tx.append(f"IT1*{i}*{li['quantity']}*{li['unit_of_measure']}*{li['unit_price']}**BP*{li['sku']}")

        total = float(inv.get("total_amount") or 0.0)
        tx.append(f"TDS*{int(round(total * 100))}")  # cents
        se_count = len(tx) + 1
        tx.append(f"SE*{se_count}*{st_ctrl}")

        hdr = []
        hdr.append(f"ISA*00*          *00*          *ZZ*SENDER_ID       *ZZ*RECEIVER_ID     *{isa_date}*{isa_time}*U*00400*{isa_ctrl}*0*P*:")
        hdr.append(f"GS*IN*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{datetime.now().strftime('%H%M')}*{gs_ctrl}*X*004010")
        lines = hdr + tx + [f"GE*1*{gs_ctrl}", f"IEA*1*{isa_ctrl}"]
        return "~".join(lines) + "~"

# -----------------------------
# Oracle flags (data-quality only)
# -----------------------------
def build_oracle_flags(pos: List[Dict[str, Any]], asns: List[Dict[str, Any]], invs: List[Dict[str, Any]]) -> Dict[str, Any]:
    # doc presence + signature hints (NOT training labels)
    po_by_num = {p["po_number"]: p for p in pos}
    asn_by_po = {}
    inv_by_po = {}
    for a in asns:
        asn_by_po.setdefault(a["po_number"], []).append(a)
    for v in invs:
        inv_by_po.setdefault(v["po_number"], []).append(v)

    out = {}
    for po in pos:
        pn = po["po_number"]
        has_asn = pn in asn_by_po and len(asn_by_po[pn]) > 0
        has_inv = pn in inv_by_po and len(inv_by_po[pn]) > 0

        sig = _sha1(
            f"{po.get('buyer_code','')}||{po.get('supplier_code','')}||" +
            "||".join(sorted([f"{li['sku']}|{li['quantity']}|{li['unit_price']}" for li in po.get("line_items") or []]))
        )

        out[pn] = {
            "oracle_flags": {
                "missing_asn": (not has_asn),
                "missing_invoice": (not has_inv),
                "po_signature": sig,
                "asn_count": int(len(asn_by_po.get(pn, []))),
                "invoice_count": int(len(inv_by_po.get(pn, []))),
            },
            "oracle_label_version": "optionB_flags_only_v1",
        }
    return out

# -----------------------------
# Main dataset builder
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--golden-dir", type=str, default=str(GOLDEN_SCHEMAS_DIR))

    ap.add_argument(
        "--quotas",
        type=str,
        default="NORMAL=8000,THREE_WAY_QTY_MISMATCH=1200,THREE_WAY_PRICE_MISMATCH=1200,LATE_SHIPMENT=900,SHORT_SHIP=900,OVERBILL=900,CHARGES_ANOMALY=800,MISSING_DOC=600,DUPLICATE_DOC=500",
    )

    ap.add_argument("--outdir", type=str, default="data_full/gold")
    ap.add_argument("--bronze-dir", type=str, default="data_full/bronze")
    ap.add_argument("--write-bronze", action="store_true")

    args = ap.parse_args()
    random.seed(int(args.seed))
    np.random.seed(int(args.seed))

    golden_dir = Path(args.golden_dir)
    dist = extract_distributions_from_golden(golden_dir)
    _p(f"[INFO] dist: avg_lines={dist.avg_lines} qty_mean={dist.qty_mean:.2f} qty_std={dist.qty_std:.2f} price_mean={dist.price_mean:.2f} price_std={dist.price_std:.2f}")

    master = build_master(dist, seed=int(args.seed))
    gen = OptionBGenerator(dist=dist, master=master, seed=int(args.seed))

    # parse quotas
    quotas: Dict[str, int] = {}
    for part in (args.quotas or "").split(","):
        part = part.strip()
        if not part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = int(v.strip())
        if k not in LABELS_OPTION_B:
            raise ValueError(f"Unknown label in quotas: {k}")
        quotas[k] = v
    for k in LABELS_OPTION_B:
        quotas.setdefault(k, 0)

    pos: List[Dict[str, Any]] = []
    asns: List[Dict[str, Any]] = []
    invs: List[Dict[str, Any]] = []
    labels: Dict[str, Any] = {}
    links: List[Dict[str, Any]] = []

    i = 0
    _p("[GEN] Building triplets...")

    # First generate base NORMAL triplets, then mutate for each class.
    def build_normal_triplet(i: int) -> Tuple[Dict, Optional[Dict], Optional[Dict]]:
        po = gen._make_po(i)
        asn = gen._make_asn_from_po(po)
        inv = gen._make_invoice_from_po_asn(po, asn)
        return po, asn, inv

    # build a pool for DUPLICATE_DOC
    normal_pool: List[Tuple[Dict, Dict, Dict]] = []

    for label, n in quotas.items():
        if n <= 0:
            continue

        _p(f"[GEN] {label}: {n}")
        for _ in range(n):
            po, asn, inv = build_normal_triplet(i)
            i += 1

            # doc missingness should happen mostly in MISSING_DOC class, but you can still have rare missing in NORMAL
            if label == "NORMAL":
                if random.random() < CFG["p_missing_asn"]:
                    asn = None
                if random.random() < CFG["p_missing_invoice"]:
                    inv = None

            # apply anomaly payload + doc edits
            po2, asn2, inv2, payload = gen._apply_anomaly(po=po, asn=asn, inv=inv, label=label)

            # write docs if still present
            pos.append(po2)
            if asn2:
                asns.append(asn2)
            if inv2:
                invs.append(inv2)

            pn = po2["po_number"]
            labels[pn] = {
                "label": label if label in LABELS_OPTION_B else "NORMAL",
                "severity": payload["severity"],
                "risk_score": payload["risk_score"],
                "estimated_dollar_impact": payload["estimated_dollar_impact"],
                "reason_codes": payload["reason_codes"],
                "owner_team": payload["owner_team"],
                "recommended_action": payload["recommended_action"],
                "tolerance_profile_id": po2.get("tolerance_profile_id"),
            }

            links.append({
                "po_number": pn,
                "po_id": po2["po_id"],
                "asn_numbers": [asn2["asn_number"]] if asn2 else [],
                "invoice_numbers": [inv2["invoice_number"]] if inv2 else [],
            })

            # keep pool for duplicates
            if label == "NORMAL" and asn2 and inv2:
                normal_pool.append((po2, asn2, inv2))

    # handle DUPLICATE_DOC as “copy a prior doc pattern”
    # (adds extra ASN/INV for same PO)
    dup_n = quotas.get("DUPLICATE_DOC", 0)
    if dup_n > 0 and normal_pool:
        _p(f"[GEN] Adding duplicate-doc instances: {dup_n}")
        for _ in range(dup_n):
            src_po, src_asn, src_inv = random.choice(normal_pool)
            pn = src_po["po_number"]

            # duplicate ASN and/or Invoice docs
            if random.random() < 0.6:
                dup_asn = copy.deepcopy(src_asn)
                dup_asn["asn_id"] = str(uuid.uuid4())
                dup_asn["asn_number"] = f"{src_asn['asn_number']}-D{random.randint(10,999)}"
                asns.append(dup_asn)

            if random.random() < 0.6:
                dup_inv = copy.deepcopy(src_inv)
                dup_inv["invoice_id"] = str(uuid.uuid4())
                dup_inv["invoice_number"] = f"{src_inv['invoice_number']}-D{random.randint(10,999)}"
                invs.append(dup_inv)

            # relabel that PO as DUPLICATE_DOC (business view)
            labels[pn]["label"] = "DUPLICATE_DOC"
            labels[pn]["reason_codes"] = list(set(labels[pn].get("reason_codes", []) + ["DUPLICATE_DOCUMENT_PATTERN"]))
            labels[pn]["owner_team"] = "OPERATIONS"
            labels[pn]["recommended_action"] = "DEDUPE_AND_CONFIRM_VALID_DOC"
            labels[pn]["risk_score"] = float(np.clip(labels[pn]["risk_score"] + 0.10, 0.0, 1.0))
            if labels[pn]["risk_score"] <= CFG["sev_low_risk_max"]:
                labels[pn]["severity"] = "LOW"
            elif labels[pn]["risk_score"] <= CFG["sev_med_risk_max"]:
                labels[pn]["severity"] = "MED"
            else:
                labels[pn]["severity"] = "HIGH"

            # update links for that po_number
            for lk in links:
                if lk["po_number"] == pn:
                    # we don't know which duplicates were added here, so we just leave arrays as-is
                    break

    # oracle flags
    oracle_flags = build_oracle_flags(pos, asns, invs)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / "training_dataset_full.json"

    dataset = {
        "mode": "optionB_po_asn_invoice_3way",
        "generator_version": "optionB_v1",
        "seed": int(args.seed),
        "dist": {
            "avg_lines": dist.avg_lines,
            "qty_mean": dist.qty_mean,
            "qty_std": dist.qty_std,
            "price_mean": dist.price_mean,
            "price_std": dist.price_std,
        },
        "cfg": CFG,
        "label_set": LABELS_OPTION_B,
        "master_data": master,

        # canonical docs
        "pos": pos,
        "asns": asns,
        "invoices": invs,

        # linkage and labels
        "links": links,            # row per PO_number
        "labels": labels,          # keyed by PO_number
        "oracle_flags": oracle_flags,  # keyed by PO_number (flags only)
    }
    out_path.write_text(json.dumps(dataset, indent=2, default=str), encoding="utf-8")
    _p(f"[OK] Wrote: {out_path}")
    _p(f"[INFO] counts: pos={len(pos)} asns={len(asns)} invs={len(invs)} labels={len(labels)}")

    if args.write_bronze:
        bronze_dir = Path(args.bronze_dir)
        bronze_dir.mkdir(parents=True, exist_ok=True)
        _p(f"[BRONZE] Writing X12 docs to: {bronze_dir}")

        # write 850 entries
        for po in pos:
            (bronze_dir / f"{po['po_number']}.850").write_text(gen.render_850(po), encoding="utf-8")

        # write 856 entries
        for asn in asns:
            (bronze_dir / f"{asn['asn_number']}.856").write_text(gen.render_856(asn), encoding="utf-8")

        # write 810 entries
        for inv in invs:
            (bronze_dir / f"{inv['invoice_number']}.810").write_text(gen.render_810(inv), encoding="utf-8")

        _p("[BRONZE] Done.")

if __name__ == "__main__":
    main()