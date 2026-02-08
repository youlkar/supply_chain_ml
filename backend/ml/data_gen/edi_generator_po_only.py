#!/usr/bin/env python3
import re
import json
import uuid
import time
import random
import zlib
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import os
import argparse

import pandas as pd
import numpy as np

try:
    from faker import Faker  # type: ignore
except Exception:
    Faker = None  # type: ignore


DEFAULT_SEGMENT_TERMINATOR = "~"
DEFAULT_ELEMENT_SEPARATOR = "*"
COMPOSITE_SEPARATOR = ":"

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

PO_ONLY_CLASSES = [
    "NORMAL",
    "PO_MISSING_FIELDS",
    "PO_INVALID_DATE",
    "PO_PRICE_OUTLIER",
    "PO_QTY_OUTLIER",
    "PO_UNKNOWN_ITEM",
]

REALISM_CFG = {
    # Benign missingness ONLY for NORMAL and ONLY non-critical
    "p_missing_freight_normal": 0.04,
    "p_missing_discount_normal": 0.06,
    "p_missing_tax_normal": 0.04,

    "expected_ship_jitter_mean": 0.0,
    "expected_ship_jitter_std": 1.0,
    "expected_ship_jitter_min": -2,
    "expected_ship_jitter_max": 3,

    # Oracle thresholds (must match feature builder/training assumptions)
    "price_outlier_pct_min": 0.06,
    "qty_outlier_z": 2.5,

    "p_unknown_sku_for_unknown_item": 0.95,

    "price_outlier_mult_hi_min": 1.18,
    "price_outlier_mult_hi_max": 1.45,
    "price_outlier_mult_lo_min": 0.55,
    "price_outlier_mult_lo_max": 0.85,

    "qty_outlier_sigma": 6.0,

    # IMPORTANT: keep NORMAL contract deviation under this fraction of threshold
    "normal_price_dev_frac_of_threshold": 0.45,

    # IMPORTANT: keep NORMAL qty within mean + this many std
    "normal_qty_sigma_cap": 2.0,
}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

os.chdir("..")
GOLDEN_SAMPLES_DIR = Path(os.getcwd()) / "golden_schemas"


def _p(msg: str) -> None:
    try:
        print(msg, flush=True)
    except Exception:
        print(msg.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore"), flush=True)


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
        if "GS" in joined:
            score += 2
        if "ST" in joined:
            score += 2
        if len(segs) > 10:
            score += 1
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


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def _now_isa_date_time() -> Tuple[str, str]:
    now = datetime.now()
    return now.strftime("%y%m%d"), now.strftime("%H%M")


def _fmt_yyyymmdd_from_iso(iso_dt: str) -> str:
    try:
        dt = datetime.fromisoformat(str(iso_dt))
        return dt.strftime("%Y%m%d")
    except Exception:
        try:
            return str(iso_dt)[:10].replace("-", "")
        except Exception:
            return datetime.now().strftime("%Y%m%d")


def _ctrl9(n: int) -> str:
    return str(int(n) % 1000000000).zfill(9)


def _ctrl_st(n: int) -> str:
    return str(int(n) % 10000).zfill(4)


_INTERCHANGE_COUNTER = 0


def _stable_seed_from_str(s: str) -> int:
    try:
        b = (s or "").encode("utf-8", errors="ignore")
    except Exception:
        b = b""
    return int(zlib.crc32(b) & 0xFFFFFFFF)


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


class X12Parser:
    def __init__(self):
        self.segment_terminator = DEFAULT_SEGMENT_TERMINATOR
        self.element_separator = DEFAULT_ELEMENT_SEPARATOR

    def parse_file(self, filepath: str) -> List[Dict]:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        seg_term, elem_sep = detect_terminator_and_separator(content)
        self.segment_terminator = seg_term
        self.element_separator = elem_sep

        segments: List[Dict] = []
        for raw_seg in split_segments(content):
            elements = raw_seg.split(self.element_separator)
            tag = elements[0].strip() if elements else None
            if tag:
                segments.append({"tag": tag, "elements": elements, "raw": raw_seg})
        return segments

    def detect_transaction_type(self, segments: List[Dict]) -> Optional[str]:
        for seg in segments:
            if seg["tag"] == "ST" and len(seg["elements"]) > 1:
                return seg["elements"][1].strip()
        return None

    def extract_po_data(self, segments: List[Dict]) -> Dict:
        po_data = {
            "po_id": str(uuid.uuid4()),
            "po_number": None,
            "buyer_code": None,
            "supplier_code": None,
            "order_date": None,
            "line_items": [],
        }

        for seg in segments:
            if seg["tag"] == "BEG":
                if len(seg["elements"]) > 3:
                    po_data["po_number"] = seg["elements"][3].strip()
                if len(seg["elements"]) > 4:
                    po_data["order_date"] = seg["elements"][4].strip()

            elif seg["tag"] == "N1":
                if len(seg["elements"]) > 2:
                    role = seg["elements"][1].strip()
                    code = seg["elements"][2].strip()
                    if role == "BY":
                        po_data["buyer_code"] = code
                    elif role == "SU":
                        po_data["supplier_code"] = code

            elif seg["tag"] == "PO1":
                line_item = {
                    "line_number": seg["elements"][1].strip() if len(seg["elements"]) > 1 else None,
                    "quantity": seg["elements"][2].strip() if len(seg["elements"]) > 2 else None,
                    "unit_of_measure": seg["elements"][3].strip() if len(seg["elements"]) > 3 else None,
                    "unit_price": seg["elements"][4].strip() if len(seg["elements"]) > 4 else None,
                    "sku": None,
                }
                if len(seg["elements"]) > 8 and seg["elements"][8].strip():
                    line_item["sku"] = seg["elements"][8].strip()
                elif len(seg["elements"]) > 7 and seg["elements"][7].strip():
                    line_item["sku"] = seg["elements"][7].strip()

                po_data["line_items"].append(line_item)

        return po_data


class SyntheticDataGenerator:
    def __init__(self, golden_pos: Optional[List[Dict]] = None, seed_val: int = 42):
        self.fake = Faker() if Faker is not None else None
        random.seed(seed_val)
        np.random.seed(seed_val)
        self.golden_pos = golden_pos or []
        self._extract_distributions()
        self.master_data = self._generate_master_data()

        self._supplier_lookup = {s["supplier_code"]: s for s in self.master_data["supplier_master"]}
        self._buyer_lookup = {b["buyer_code"]: b for b in self.master_data["buyer_master"]}
        self._pricing_lookup = {(p["supplier_code"], p["sku"]): p for p in self.master_data["pricing_contracts"]}

    def _extract_distributions(self):
        if not self.golden_pos:
            self.avg_line_items = 5
            self.avg_quantity = 100.0
            self.std_quantity = 50.0
            self.avg_unit_price = 50.0
            self.std_unit_price = 20.0
            return

        line_counts, qtys, prices = [], [], []
        for po in self.golden_pos:
            items = po.get("line_items", []) or []
            line_counts.append(len(items))
            for it in items:
                try:
                    if it.get("quantity") is not None:
                        qtys.append(float(it["quantity"]))
                except Exception:
                    pass
                try:
                    if it.get("unit_price") is not None:
                        prices.append(float(it["unit_price"]))
                except Exception:
                    pass

        self.avg_line_items = int(np.clip(np.mean(line_counts) if line_counts else 5, 1, 15))
        q_mean = float(np.mean(qtys) if qtys else 100.0)
        q_std = float(np.std(qtys) if qtys else 50.0)
        p_mean = float(np.mean(prices) if prices else 50.0)
        p_std = float(np.std(prices) if prices else 20.0)

        self.avg_quantity = float(np.clip(q_mean, 1, 10000))
        self.std_quantity = float(np.clip(q_std, 1, 10000))
        self.avg_unit_price = float(np.clip(p_mean, 1, 10000))
        self.std_unit_price = float(np.clip(p_std, 1, 10000))

    def _generate_master_data(self) -> Dict[str, List[Dict]]:
        supplier_master: List[Dict] = []
        for code in SUPPLIERS:
            supplier_master.append(
                {
                    "supplier_code": code,
                    "supplier_name": code.replace("_", " ").title(),
                    "lead_time_days": int(random.randint(2, 14)),
                    "sla_ship_days": int(random.randint(1, 7)),
                    "default_payment_terms": random.choice(PAYMENT_TERMS),
                    "preferred_carrier": random.choice(CARRIERS),
                }
            )

        buyer_master: List[Dict] = []
        for code in BUYERS:
            buyer_master.append(
                {
                    "buyer_code": code,
                    "buyer_name": code.replace("_", " ").title(),
                    "default_ship_to": random.choice(LOCATIONS)["location_code"],
                    "default_bill_to": random.choice(LOCATIONS)["location_code"],
                }
            )

        item_master: List[Dict] = []
        for sku in SKUS:
            item_master.append({"sku": sku, "description": sku.replace("-", " ").title()})

        pricing_contracts: List[Dict] = []
        today = datetime.now().date()
        for s in SUPPLIERS:
            for sku in SKUS:
                base_price = float(np.clip(np.random.normal(self.avg_unit_price, max(1.0, self.avg_unit_price * 0.20)), 1, 2000))
                discount_pct = float(np.clip(np.random.normal(0.03, 0.02), 0.0, 0.15))
                pricing_contracts.append(
                    {
                        "supplier_code": s,
                        "sku": sku,
                        "contract_unit_price": round(base_price, 2),
                        "discount_pct": round(discount_pct, 4),
                        "currency": "USD",
                        "effective_start": str(today - timedelta(days=180)),
                        "effective_end": str(today + timedelta(days=180)),
                    }
                )

        location_master = [dict(l) for l in LOCATIONS]

        return {
            "supplier_master": supplier_master,
            "buyer_master": buyer_master,
            "item_master": item_master,
            "pricing_contracts": pricing_contracts,
            "location_master": location_master,
        }

    def _base_po_shell(self, i: int, seen_po_numbers: set) -> Dict:
        n_lines = int(np.clip(np.random.poisson(lam=self.avg_line_items), 1, 12))
        buyer_code = random.choice(BUYERS)
        supplier_code = random.choice(SUPPLIERS)
        order_dt = datetime.now() - timedelta(days=random.randint(0, 90))

        buyer_m = self._buyer_lookup.get(buyer_code, {})
        supplier_m = self._supplier_lookup.get(supplier_code, {})

        ship_to = buyer_m.get("default_ship_to") or random.choice(LOCATIONS)["location_code"]
        bill_to = buyer_m.get("default_bill_to") or random.choice(LOCATIONS)["location_code"]
        pay_terms = supplier_m.get("default_payment_terms") or random.choice(PAYMENT_TERMS)

        lead = int(supplier_m.get("lead_time_days", 7))
        jitter = int(np.clip(
            np.random.normal(float(REALISM_CFG["expected_ship_jitter_mean"]), float(REALISM_CFG["expected_ship_jitter_std"])),
            float(REALISM_CFG["expected_ship_jitter_min"]), float(REALISM_CFG["expected_ship_jitter_max"])
        ))
        expected_ship_dt = order_dt + timedelta(days=max(1, lead + jitter))

        po_number = f"PO-{int(time.time() * 1000) % 1000000}-{i}"
        while po_number in seen_po_numbers:
            po_number = f"PO-{int(time.time() * 1000) % 1000000}-{i}-{random.randint(0,999)}"
        seen_po_numbers.add(po_number)

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
            "line_items": [],
            "anomaly": "NORMAL",
            "intended_anomaly": "NORMAL",
            "freight_amount": round(float(np.clip(np.random.normal(45.0, 15.0), 0, 250)), 2),
            "discount_amount": round(float(np.clip(np.random.normal(20.0, 12.0), 0, 200)), 2),
            "tax_amount": round(float(np.clip(np.random.normal(10.0, 8.0), 0, 150)), 2),
        }

        for j in range(n_lines):
            sku = random.choice(SKUS)
            contract = self._pricing_lookup.get((supplier_code, sku), {})
            contract_price = float(contract.get("contract_unit_price", self.avg_unit_price))
            discount_pct = float(contract.get("discount_pct", 0.0))

            qty = int(np.clip(np.random.normal(self.avg_quantity, max(5.0, self.std_quantity)), 1, 5000))

            raw_price = float(np.clip(np.random.normal(contract_price, max(0.5, contract_price * 0.05)), 0.01, 2000))
            unit_price = round(raw_price * (1.0 - discount_pct), 2)

            po["line_items"].append(
                {
                    "line_number": j + 1,
                    "sku": sku,
                    "quantity": qty,
                    "unit_of_measure": random.choice(UNITS_OF_MEASURE),
                    "unit_price": unit_price,
                    "contract_unit_price": round(contract_price, 2),
                    "discount_pct": round(discount_pct, 4),
                }
            )

        return po

    def _sanitize_for_clean_normal(self, po: Dict) -> None:
        # critical fields present
        if not po.get("payment_terms"):
            po["payment_terms"] = random.choice(PAYMENT_TERMS)
        if not po.get("ship_to_location"):
            po["ship_to_location"] = random.choice(LOCATIONS)["location_code"]
        if not po.get("bill_to_location"):
            po["bill_to_location"] = random.choice(LOCATIONS)["location_code"]

        # valid order_date and not future
        try:
            dt = datetime.fromisoformat(str(po.get("order_date")))
        except Exception:
            dt = datetime.now() - timedelta(days=5)
        if dt > datetime.now():
            dt = datetime.now() - timedelta(days=1)
        po["order_date"] = dt.isoformat()

        # known SKUs
        for li in po.get("line_items", []) or []:
            li["sku"] = random.choice(SKUS)

        # ensure contract deviation stays below threshold * frac
        thr = float(REALISM_CFG["price_outlier_pct_min"])
        max_dev = thr * float(REALISM_CFG["normal_price_dev_frac_of_threshold"])
        for li in po.get("line_items", []) or []:
            cp = float(li.get("contract_unit_price") or 0.0)
            if cp <= 0:
                continue
            li["unit_price"] = round(cp * random.uniform(1.0 - max_dev, 1.0 + max_dev), 2)

        # qty cap to avoid accidental z outlier
        cap = float(self.avg_quantity + float(REALISM_CFG["normal_qty_sigma_cap"]) * self.std_quantity)
        cap = max(5.0, cap)
        for li in po.get("line_items", []) or []:
            q = int(li.get("quantity") or 1)
            li["quantity"] = int(np.clip(q, 1, int(min(5000, cap))))

        # benign missingness on non-critical only
        if random.random() < float(REALISM_CFG["p_missing_freight_normal"]):
            po["freight_amount"] = None
        if random.random() < float(REALISM_CFG["p_missing_discount_normal"]):
            po["discount_amount"] = None
        if random.random() < float(REALISM_CFG["p_missing_tax_normal"]):
            po["tax_amount"] = None

    def _apply_single_anomaly(self, po: Dict, anomaly: str) -> None:
        # start from clean normal (removes overlap)
        self._sanitize_for_clean_normal(po)

        if anomaly == "PO_MISSING_FIELDS":
            which = random.choice(["payment_terms", "ship_to_location", "bill_to_location"])
            po[which] = None

        elif anomaly == "PO_INVALID_DATE":
            if random.random() < 0.7:
                po["order_date"] = (datetime.now() + timedelta(days=random.randint(2, 30))).isoformat()
            else:
                po["order_date"] = "BAD_DATE"
            # keep missing_fields false
            if not po.get("payment_terms"):
                po["payment_terms"] = random.choice(PAYMENT_TERMS)
            if not po.get("ship_to_location"):
                po["ship_to_location"] = random.choice(LOCATIONS)["location_code"]
            if not po.get("bill_to_location"):
                po["bill_to_location"] = random.choice(LOCATIONS)["location_code"]

        elif anomaly == "PO_UNKNOWN_ITEM":
            if po.get("line_items"):
                k = random.randint(0, len(po["line_items"]) - 1)
                po["line_items"][k]["sku"] = f"UNKNOWN-{random.randint(1000,9999)}"

        elif anomaly == "PO_PRICE_OUTLIER":
            if po.get("line_items"):
                k = random.randint(0, len(po["line_items"]) - 1)
                li = po["line_items"][k]
                cp = float(li.get("contract_unit_price") or li.get("unit_price") or self.avg_unit_price)
                if random.random() < 0.7:
                    mult = random.uniform(float(REALISM_CFG["price_outlier_mult_hi_min"]), float(REALISM_CFG["price_outlier_mult_hi_max"]))
                else:
                    mult = random.uniform(float(REALISM_CFG["price_outlier_mult_lo_min"]), float(REALISM_CFG["price_outlier_mult_lo_max"]))
                li["unit_price"] = round(max(0.01, cp * mult), 2)

        elif anomaly == "PO_QTY_OUTLIER":
            if po.get("line_items"):
                k = random.randint(0, len(po["line_items"]) - 1)
                big = int(np.clip(self.avg_quantity + float(REALISM_CFG["qty_outlier_sigma"]) * self.std_quantity, 50, 5000))
                po["line_items"][k]["quantity"] = big

        else:
            # NORMAL already clean
            pass

        po["anomaly"] = anomaly
        po["intended_anomaly"] = anomaly

    def generate_pos_with_quotas(self, quotas: Dict[str, int], start_index: int = 0) -> List[Dict]:
        for k in quotas.keys():
            if k not in PO_ONLY_CLASSES:
                raise ValueError(f"Unknown PO-only class in quotas: {k}")
        for k in PO_ONLY_CLASSES:
            quotas.setdefault(k, 0)

        seen_po_numbers: set[str] = set()
        pos: List[Dict] = []
        i = int(start_index)

        for label in PO_ONLY_CLASSES:
            target = int(quotas.get(label, 0))
            if target <= 0:
                continue

            _p(f"[GEN] Target {label}: {target}")
            for _ in range(target):
                po = self._base_po_shell(i=i, seen_po_numbers=seen_po_numbers)
                self._apply_single_anomaly(po, label)
                pos.append(po)
                i += 1

        random.shuffle(pos)
        return pos

    def generate_x12_850(self, po: Dict) -> str:
        isa_date, isa_time = _now_isa_date_time()
        isa_ctrl, gs_ctrl, st_ctrl = _make_interchange_ids(seed=_stable_seed_from_str(str(po.get("po_number") or "")))
        beg_date = _fmt_yyyymmdd_from_iso(po.get("order_date", ""))

        tx: List[str] = []
        tx.append(f"ST*850*{st_ctrl}")
        tx.append(f"BEG*00*SA*{po['po_number']}*{beg_date}")
        tx.append(f"N1*BY*{po['buyer_code']}")
        tx.append(f"N1*SU*{po['supplier_code']}")

        if po.get("payment_terms"):
            tx.append(f"ITD*01******{po['payment_terms']}")

        if po.get("freight_amount") is not None:
            tx.append(f"SAC*C*FREIGHT***{float(po['freight_amount']):.2f}")
        if po.get("discount_amount") is not None and float(po["discount_amount"]) > 0:
            tx.append(f"SAC*A*DISCOUNT***{float(po['discount_amount']):.2f}")

        for idx, item in enumerate(po.get("line_items", []), 1):
            tx.append(f"PO1*{idx}*{item['quantity']}*{item['unit_of_measure']}*{item['unit_price']}****{item['sku']}")

        tx.append(f"CTT*{len(po.get('line_items', []))}")
        se_count = len(tx) + 1
        se = f"SE*{se_count}*{st_ctrl}"

        lines: List[str] = []
        lines.append(f"ISA*00*          *00*          *ZZ*SENDER_ID       *ZZ*RECEIVER_ID     *{isa_date}*{isa_time}*U*00400*{isa_ctrl}*0*P*:")
        lines.append(f"GS*PO*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{datetime.now().strftime('%H%M')}*{gs_ctrl}*X*004010")
        lines.extend(tx)
        lines.append(se)
        lines.append(f"GE*1*{gs_ctrl}")
        lines.append(f"IEA*1*{isa_ctrl}")
        return "~".join(lines) + "~"


def parse_golden_samples() -> List[Dict]:
    golden_pos: List[Dict] = []
    parser = X12Parser()

    if not GOLDEN_SAMPLES_DIR.exists():
        _p(f"[WARN] golden_schemas dir not found at: {GOLDEN_SAMPLES_DIR}")
        return golden_pos

    files = sorted([p for p in GOLDEN_SAMPLES_DIR.rglob("*") if p.is_file()])
    if not files:
        _p(f"[WARN] No files found under: {GOLDEN_SAMPLES_DIR}")
        return golden_pos

    for file in files:
        try:
            if file.suffix.lower() in [".pdf", ".png", ".jpg", ".jpeg"]:
                continue
            segments = parser.parse_file(str(file))
            tx = parser.detect_transaction_type(segments)
            if tx == "850":
                po = parser.extract_po_data(segments)
                if po.get("po_number"):
                    golden_pos.append(po)
        except Exception as e:
            _p(f"[WARN] Failed parsing {file.name}: {e}")

    return golden_pos


def build_oracle_labels_po_only(
    pos: List[Dict],
    *,
    price_outlier_pct_min: float,
    qty_outlier_z: float,
    known_skus: set,
) -> Dict[str, Dict]:
    all_qty: List[float] = []
    for po in pos or []:
        for li in po.get("line_items", []) or []:
            try:
                all_qty.append(float(li.get("quantity") or 0))
            except Exception:
                pass

    qty_mean = float(np.mean(all_qty) if all_qty else 100.0)
    qty_std = float(np.std(all_qty) if all_qty else 1.0)
    qty_std = max(1.0, qty_std)

    def _parse_iso_dt(val: Optional[str]) -> Optional[datetime]:
        try:
            if not val:
                return None
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    oracle: Dict[str, Dict] = {}
    for po in pos or []:
        po_id = str(po.get("po_id") or "")
        if not po_id:
            continue

        flags = {"missing_fields": False, "invalid_date": False, "unknown_item": False, "price_outlier": False, "qty_outlier": False}

        if (po.get("payment_terms") in (None, "")) or (po.get("ship_to_location") in (None, "")) or (po.get("bill_to_location") in (None, "")):
            flags["missing_fields"] = True

        dt = _parse_iso_dt(str(po.get("order_date") or ""))
        if dt is None or dt > (datetime.now() + timedelta(days=1)):
            flags["invalid_date"] = True

        for li in po.get("line_items", []) or []:
            sku = str(li.get("sku") or "")
            if sku and (sku not in known_skus):
                flags["unknown_item"] = True

            try:
                up = float(li.get("unit_price") or 0.0)
            except Exception:
                up = 0.0
            try:
                cp = float(li.get("contract_unit_price") or 0.0)
            except Exception:
                cp = 0.0

            if up > 0 and cp > 0:
                pct = abs(up - cp) / max(0.01, cp)
                if pct >= float(price_outlier_pct_min):
                    flags["price_outlier"] = True

            try:
                q = float(li.get("quantity") or 0.0)
            except Exception:
                q = 0.0
            z = abs(q - qty_mean) / qty_std
            if z >= float(qty_outlier_z):
                flags["qty_outlier"] = True

        if flags["unknown_item"]:
            label = "PO_UNKNOWN_ITEM"
        elif flags["invalid_date"]:
            label = "PO_INVALID_DATE"
        elif flags["missing_fields"]:
            label = "PO_MISSING_FIELDS"
        elif flags["price_outlier"]:
            label = "PO_PRICE_OUTLIER"
        elif flags["qty_outlier"]:
            label = "PO_QTY_OUTLIER"
        else:
            label = "NORMAL"

        oracle[po_id] = {
            "oracle_anomaly_type": label,
            "oracle_is_anomaly": label != "NORMAL",
            "oracle_flags": flags,
            "oracle_label_version": "po_only_v3_clean",
            "po_number": str(po.get("po_number") or ""),
        }

    return oracle


def create_anomaly_labels(pos: List[Dict], oracle_labels: Dict[str, Dict]) -> Dict:
    labels: Dict[str, Dict] = {}
    for po in pos or []:
        po_id = str(po.get("po_id") or "")
        if not po_id:
            continue
        o = oracle_labels.get(po_id, {})
        labels[po_id] = {
            "anomaly_type": str(po.get("anomaly") or "NORMAL"),
            "is_anomaly": str(po.get("anomaly") or "NORMAL") != "NORMAL",
            "intended_anomaly_type": str(po.get("intended_anomaly") or po.get("anomaly") or "NORMAL"),
            "oracle_anomaly_type": str(o.get("oracle_anomaly_type") or "UNKNOWN"),
            "oracle_is_anomaly": bool(o.get("oracle_is_anomaly")),
            "oracle_flags": o.get("oracle_flags") or {},
            "oracle_label_version": str(o.get("oracle_label_version") or ""),
        }
    return labels


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--quotas",
        type=str,
        default="NORMAL=10000,PO_MISSING_FIELDS=2000,PO_INVALID_DATE=2000,PO_PRICE_OUTLIER=2000,PO_QTY_OUTLIER=2000,PO_UNKNOWN_ITEM=2000",
    )
    ap.add_argument("--label-source", choices=["intended", "oracle"], default="oracle")
    ap.add_argument("--bronze-mode", choices=["all", "sample", "none"], default="sample")
    ap.add_argument("--bronze-sample-size", type=int, default=2000)
    args = ap.parse_args()

    def _parse_quota_arg(s: str) -> Dict[str, int]:
        out: Dict[str, int] = {}
        parts = [p.strip() for p in (s or "").split(",") if p.strip()]
        for p in parts:
            if "=" not in p:
                raise ValueError(f"Bad quota entry: {p} (expected KEY=NUM)")
            k, v = p.split("=", 1)
            k, v = k.strip(), v.strip()
            if k not in PO_ONLY_CLASSES:
                raise ValueError(f"Unknown class in quotas: {k}")
            out[k] = int(v)
        return out

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    _p("[1/4] Parsing golden samples...")
    golden_pos = parse_golden_samples()
    _p(f"[INFO] Golden POs: {len(golden_pos)}")

    quotas = _parse_quota_arg(str(args.quotas))
    _p("[2/4] Generating quota-driven POs...")
    gen = SyntheticDataGenerator(golden_pos=golden_pos, seed_val=int(args.seed))
    pos = gen.generate_pos_with_quotas(quotas=quotas)

    _p("[3/4] Building oracle labels...")
    oracle_labels = build_oracle_labels_po_only(
        pos,
        price_outlier_pct_min=float(REALISM_CFG["price_outlier_pct_min"]),
        qty_outlier_z=float(REALISM_CFG["qty_outlier_z"]),
        known_skus=set(SKUS),
    )

    if str(args.label_source) == "oracle":
        for po in pos:
            oid = str(po.get("po_id") or "")
            po["anomaly"] = str(oracle_labels.get(oid, {}).get("oracle_anomaly_type") or po.get("anomaly") or "NORMAL")

    labels = create_anomaly_labels(pos, oracle_labels)

    # Save Silver
    write_csv(pd.DataFrame(pos), SILVER_DIR / "pos.csv")

    # Save Gold
    training_data = {
        "pos": pos,
        "asns": [],
        "invoices": [],
        "anomaly_labels": labels,
        "master_data": gen.master_data,
        "oracle_labels": oracle_labels,
        "mode": "option_a_po_only",
        "generator_version": "po_only_v3_clean",
    }
    (GOLD_DIR / "training_dataset.json").write_text(json.dumps(training_data, indent=2, default=str), encoding="utf-8")
    _p(f"[OK] Wrote: {GOLD_DIR / 'training_dataset.json'}")

    # Bronze (optional)
    if str(args.bronze_mode) != "none":
        if str(args.bronze_mode) == "sample":
            rng = np.random.default_rng(int(args.seed))
            n = len(pos)
            k = max(1, min(int(args.bronze_sample_size), n))
            idxs = set(rng.choice(np.arange(n), size=k, replace=False).tolist())
            subset = [po for i, po in enumerate(pos) if i in idxs]
        else:
            subset = pos

        for po in subset:
            (BRONZE_DIR / f"PO_{po['po_number']}.850").write_text(gen.generate_x12_850(po), encoding="utf-8")
        _p(f"[OK] Bronze wrote {len(subset)} files to: {BRONZE_DIR}")

    _p("[DONE]")


if __name__ == "__main__":
    main()