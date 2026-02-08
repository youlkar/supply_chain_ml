# #!/usr/bin/env python3
# import re
# import json
# import uuid
# import time
# import random
# import zlib
# from datetime import datetime, timedelta
# from pathlib import Path
# from typing import Dict, List, Tuple, Optional, Any
# import os
# import argparse

# import pandas as pd
# import numpy as np

# try:
#     from faker import Faker  # type: ignore
# except Exception:
#     Faker = None  # type: ignore


# DEFAULT_SEGMENT_TERMINATOR = "~"
# DEFAULT_ELEMENT_SEPARATOR = "*"
# COMPOSITE_SEPARATOR = ":"

# SUPPLIERS = [
#     "SUPPLIER001",
#     "SUPP_ACE_MFG",
#     "WIDGET_CO",
#     "ACME_PARTS",
#     "INDUSTRIAL_GOODS_LTD",
#     "TECH_SUPPLY_INT",
#     "LOGISTICS_PLUS",
#     "PREMIUM_GOODS",
# ]

# BUYERS = [
#     "BUYER_RETAIL_A",
#     "BUYER_DISTRIB_B",
#     "BUYER_WAREHOUSE_C",
#     "BUYER_CHAIN_D",
#     "BUYER_ECOMM_E",
# ]

# SKUS = [
#     "SKU-10001",
#     "SKU-10002",
#     "SKU-10003",
#     "SKU-20001",
#     "SKU-20002",
#     "SKU-30001",
#     "PART-XYZ-100",
#     "PART-ABC-200",
#     "WIDGET-BLUE-SM",
#     "WIDGET-RED-LG",
# ]

# UNITS_OF_MEASURE = ["EA", "CS", "DZ", "BOX", "PLT", "CT"]
# PAYMENT_TERMS = ["NET30", "NET45", "NET60", "2%10NET30"]
# CARRIERS = ["UPS", "FEDEX", "DHL", "XPO", "OLD_DOMINION", "JB_HUNT"]

# LOCATIONS = [
#     {"location_code": "WH-NE-01", "name": "Northeast DC", "city": "Newark", "state": "NJ", "timezone": "America/New_York"},
#     {"location_code": "WH-SE-01", "name": "Southeast DC", "city": "Atlanta", "state": "GA", "timezone": "America/New_York"},
#     {"location_code": "WH-MW-01", "name": "Midwest DC", "city": "Chicago", "state": "IL", "timezone": "America/Chicago"},
#     {"location_code": "WH-W-01", "name": "West DC", "city": "Reno", "state": "NV", "timezone": "America/Los_Angeles"},
# ]

# CURRENCY_CODES = ["USD"]

# PO_ONLY_CLASSES = [
#     "NORMAL",
#     "PO_MISSING_FIELDS",
#     "PO_INVALID_DATE",
#     "PO_PRICE_OUTLIER",
#     "PO_QTY_OUTLIER",
#     "PO_UNKNOWN_ITEM",
# ]

# REALISM_CFG = {
#     # Benign missingness ONLY for NORMAL and ONLY non-critical
#     "p_missing_freight_normal": 0.04,
#     "p_missing_discount_normal": 0.06,
#     "p_missing_tax_normal": 0.04,

#     "expected_ship_jitter_mean": 0.0,
#     "expected_ship_jitter_std": 1.0,
#     "expected_ship_jitter_min": -2,
#     "expected_ship_jitter_max": 3,

#     # Oracle thresholds (must match feature builder/training assumptions)
#     "price_outlier_pct_min": 0.06,
#     "qty_outlier_z": 2.5,

#     # Strong outlier multipliers (still used sometimes)
#     "price_outlier_mult_hi_min": 1.18,
#     "price_outlier_mult_hi_max": 1.45,
#     "price_outlier_mult_lo_min": 0.55,
#     "price_outlier_mult_lo_max": 0.85,
#     "qty_outlier_sigma": 6.0,

#     # NORMAL contract deviation cap (kept clean)
#     "normal_price_dev_frac_of_threshold": 0.45,
#     "normal_qty_sigma_cap": 2.0,

#     # ---- MIN REALISTIC UPGRADES ----
#     # Borderline anomalies: not always extreme
#     "p_borderline_price_outlier": 0.45,  # ~half of price outliers are only slightly above threshold
#     "p_borderline_qty_outlier": 0.45,    # same for qty outliers
#     "borderline_over_threshold_min": 1.02,
#     "borderline_over_threshold_max": 1.25,

#     # Some "unknown items" look like typos (realistic)
#     "p_unknown_sku_typo": 0.35,

#     # Time drift: recent orders differ slightly (more qty/freight)
#     "drift_recent_days": 21,
#     "drift_qty_mult_recent": 1.12,
#     "drift_freight_mult_recent": 1.15,

#     # Label noise (real-world ops labels are imperfect)
#     "p_label_noise_anom_to_normal": 0.05,   # 5% of true anomalies labeled NORMAL
#     "p_label_noise_normal_to_anom": 0.01,   # 1% of NORMAL mislabeled as some anomaly
# }

# SCRIPT_DIR = Path(__file__).resolve().parent
# DATA_DIR = SCRIPT_DIR / "data"
# BRONZE_DIR = DATA_DIR / "bronze"
# SILVER_DIR = DATA_DIR / "silver"
# GOLD_DIR = DATA_DIR / "gold"

# os.chdir("../../../..")
# GOLDEN_SAMPLES_DIR = Path(os.getcwd()) / "golden_schemas"


# def _p(msg: str) -> None:
#     try:
#         print(msg, flush=True)
#     except Exception:
#         print(msg.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore"), flush=True)


# def detect_terminator_and_separator(content: str) -> Tuple[str, str]:
#     content = content.strip()
#     elem_sep = DEFAULT_ELEMENT_SEPARATOR
#     if content.startswith("ISA") and len(content) >= 4:
#         elem_sep = content[3]

#     candidates = ["~", "\n"]
#     best = DEFAULT_SEGMENT_TERMINATOR
#     best_score = -1

#     for cand in candidates:
#         segs = content.split(cand)
#         joined = " ".join(segs[:50])
#         score = 0
#         if "GS" in joined:
#             score += 2
#         if "ST" in joined:
#             score += 2
#         if len(segs) > 10:
#             score += 1
#         if score > best_score:
#             best_score = score
#             best = cand

#     return best, elem_sep


# def split_segments(content: str) -> List[str]:
#     content = content.strip()
#     if "~" in content:
#         segs = content.split("~")
#     else:
#         segs = re.split(r"[\r\n]+", content)
#     return [s.strip() for s in segs if s.strip()]


# def write_csv(df: pd.DataFrame, path: Path) -> None:
#     df.to_csv(path, index=False)


# def _now_isa_date_time() -> Tuple[str, str]:
#     now = datetime.now()
#     return now.strftime("%y%m%d"), now.strftime("%H%M")


# def _fmt_yyyymmdd_from_iso(iso_dt: str) -> str:
#     try:
#         dt = datetime.fromisoformat(str(iso_dt))
#         return dt.strftime("%Y%m%d")
#     except Exception:
#         try:
#             return str(iso_dt)[:10].replace("-", "")
#         except Exception:
#             return datetime.now().strftime("%Y%m%d")


# def _ctrl9(n: int) -> str:
#     return str(int(n) % 1000000000).zfill(9)


# def _ctrl_st(n: int) -> str:
#     return str(int(n) % 10000).zfill(4)


# _INTERCHANGE_COUNTER = 0


# def _stable_seed_from_str(s: str) -> int:
#     try:
#         b = (s or "").encode("utf-8", errors="ignore")
#     except Exception:
#         b = b""
#     return int(zlib.crc32(b) & 0xFFFFFFFF)


# def _make_interchange_ids(seed: Optional[int] = None) -> Tuple[str, str, str]:
#     global _INTERCHANGE_COUNTER
#     _INTERCHANGE_COUNTER += 1
#     base = int(time.time() * 1000)
#     if seed is not None:
#         base += int(seed)
#     base += (_INTERCHANGE_COUNTER % 1000000)
#     isa_ctrl = _ctrl9(base)
#     gs_ctrl = str((base // 10) % 100000)
#     st_ctrl = _ctrl_st(base // 100)
#     return isa_ctrl, gs_ctrl, st_ctrl


# class X12Parser:
#     def __init__(self):
#         self.segment_terminator = DEFAULT_SEGMENT_TERMINATOR
#         self.element_separator = DEFAULT_ELEMENT_SEPARATOR

#     def parse_file(self, filepath: str) -> List[Dict]:
#         with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
#             content = f.read()

#         seg_term, elem_sep = detect_terminator_and_separator(content)
#         self.segment_terminator = seg_term
#         self.element_separator = elem_sep

#         segments: List[Dict] = []
#         for raw_seg in split_segments(content):
#             elements = raw_seg.split(self.element_separator)
#             tag = elements[0].strip() if elements else None
#             if tag:
#                 segments.append({"tag": tag, "elements": elements, "raw": raw_seg})
#         return segments

#     def detect_transaction_type(self, segments: List[Dict]) -> Optional[str]:
#         for seg in segments:
#             if seg["tag"] == "ST" and len(seg["elements"]) > 1:
#                 return seg["elements"][1].strip()
#         return None

#     def extract_po_data(self, segments: List[Dict]) -> Dict:
#         po_data = {
#             "po_id": str(uuid.uuid4()),
#             "po_number": None,
#             "buyer_code": None,
#             "supplier_code": None,
#             "order_date": None,
#             "line_items": [],
#         }

#         for seg in segments:
#             if seg["tag"] == "BEG":
#                 if len(seg["elements"]) > 3:
#                     po_data["po_number"] = seg["elements"][3].strip()
#                 if len(seg["elements"]) > 4:
#                     po_data["order_date"] = seg["elements"][4].strip()

#             elif seg["tag"] == "N1":
#                 if len(seg["elements"]) > 2:
#                     role = seg["elements"][1].strip()
#                     code = seg["elements"][2].strip()
#                     if role == "BY":
#                         po_data["buyer_code"] = code
#                     elif role == "SU":
#                         po_data["supplier_code"] = code

#             elif seg["tag"] == "PO1":
#                 line_item = {
#                     "line_number": seg["elements"][1].strip() if len(seg["elements"]) > 1 else None,
#                     "quantity": seg["elements"][2].strip() if len(seg["elements"]) > 2 else None,
#                     "unit_of_measure": seg["elements"][3].strip() if len(seg["elements"]) > 3 else None,
#                     "unit_price": seg["elements"][4].strip() if len(seg["elements"]) > 4 else None,
#                     "sku": None,
#                 }
#                 if len(seg["elements"]) > 8 and seg["elements"][8].strip():
#                     line_item["sku"] = seg["elements"][8].strip()
#                 elif len(seg["elements"]) > 7 and seg["elements"][7].strip():
#                     line_item["sku"] = seg["elements"][7].strip()

#                 po_data["line_items"].append(line_item)

#         return po_data


# class SyntheticDataGenerator:
#     def __init__(self, golden_pos: Optional[List[Dict]] = None, seed_val: int = 42):
#         self.fake = Faker() if Faker is not None else None
#         random.seed(seed_val)
#         np.random.seed(seed_val)
#         self.golden_pos = golden_pos or []
#         self._extract_distributions()
#         self.master_data = self._generate_master_data()

#         self._supplier_lookup = {s["supplier_code"]: s for s in self.master_data["supplier_master"]}
#         self._buyer_lookup = {b["buyer_code"]: b for b in self.master_data["buyer_master"]}
#         self._pricing_lookup = {(p["supplier_code"], p["sku"]): p for p in self.master_data["pricing_contracts"]}

#     def _extract_distributions(self):
#         if not self.golden_pos:
#             self.avg_line_items = 5
#             self.avg_quantity = 100.0
#             self.std_quantity = 50.0
#             self.avg_unit_price = 50.0
#             self.std_unit_price = 20.0
#             return

#         line_counts, qtys, prices = [], [], []
#         for po in self.golden_pos:
#             items = po.get("line_items", []) or []
#             line_counts.append(len(items))
#             for it in items:
#                 try:
#                     if it.get("quantity") is not None:
#                         qtys.append(float(it["quantity"]))
#                 except Exception:
#                     pass
#                 try:
#                     if it.get("unit_price") is not None:
#                         prices.append(float(it["unit_price"]))
#                 except Exception:
#                     pass

#         self.avg_line_items = int(np.clip(np.mean(line_counts) if line_counts else 5, 1, 15))
#         q_mean = float(np.mean(qtys) if qtys else 100.0)
#         q_std = float(np.std(qtys) if qtys else 50.0)
#         p_mean = float(np.mean(prices) if prices else 50.0)
#         p_std = float(np.std(prices) if prices else 20.0)

#         self.avg_quantity = float(np.clip(q_mean, 1, 10000))
#         self.std_quantity = float(np.clip(q_std, 1, 10000))
#         self.avg_unit_price = float(np.clip(p_mean, 1, 10000))
#         self.std_unit_price = float(np.clip(p_std, 1, 10000))

#     def _generate_master_data(self) -> Dict[str, List[Dict]]:
#         supplier_master: List[Dict] = []
#         for code in SUPPLIERS:
#             supplier_master.append(
#                 {
#                     "supplier_code": code,
#                     "supplier_name": code.replace("_", " ").title(),
#                     "lead_time_days": int(random.randint(2, 14)),
#                     "sla_ship_days": int(random.randint(1, 7)),
#                     "default_payment_terms": random.choice(PAYMENT_TERMS),
#                     "preferred_carrier": random.choice(CARRIERS),
#                 }
#             )

#         buyer_master: List[Dict] = []
#         for code in BUYERS:
#             buyer_master.append(
#                 {
#                     "buyer_code": code,
#                     "buyer_name": code.replace("_", " ").title(),
#                     "default_ship_to": random.choice(LOCATIONS)["location_code"],
#                     "default_bill_to": random.choice(LOCATIONS)["location_code"],
#                 }
#             )

#         item_master: List[Dict] = []
#         for sku in SKUS:
#             item_master.append({"sku": sku, "description": sku.replace("-", " ").title()})

#         pricing_contracts: List[Dict] = []
#         today = datetime.now().date()
#         for s in SUPPLIERS:
#             for sku in SKUS:
#                 base_price = float(np.clip(np.random.normal(self.avg_unit_price, max(1.0, self.avg_unit_price * 0.20)), 1, 2000))
#                 discount_pct = float(np.clip(np.random.normal(0.03, 0.02), 0.0, 0.15))
#                 pricing_contracts.append(
#                     {
#                         "supplier_code": s,
#                         "sku": sku,
#                         "contract_unit_price": round(base_price, 2),
#                         "discount_pct": round(discount_pct, 4),
#                         "currency": "USD",
#                         "effective_start": str(today - timedelta(days=180)),
#                         "effective_end": str(today + timedelta(days=180)),
#                     }
#                 )

#         location_master = [dict(l) for l in LOCATIONS]

#         return {
#             "supplier_master": supplier_master,
#             "buyer_master": buyer_master,
#             "item_master": item_master,
#             "pricing_contracts": pricing_contracts,
#             "location_master": location_master,
#         }

#     def _base_po_shell(self, i: int, seen_po_numbers: set) -> Dict:
#         n_lines = int(np.clip(np.random.poisson(lam=self.avg_line_items), 1, 12))
#         buyer_code = random.choice(BUYERS)
#         supplier_code = random.choice(SUPPLIERS)

#         # spread across ~90 days
#         order_dt = datetime.now() - timedelta(days=random.randint(0, 90))

#         # --- MIN REALISTIC: time drift (recent orders slightly different)
#         is_recent = order_dt > (datetime.now() - timedelta(days=int(REALISM_CFG["drift_recent_days"])))
#         qty_mult = float(REALISM_CFG["drift_qty_mult_recent"]) if is_recent else 1.0
#         freight_mult = float(REALISM_CFG["drift_freight_mult_recent"]) if is_recent else 1.0

#         buyer_m = self._buyer_lookup.get(buyer_code, {})
#         supplier_m = self._supplier_lookup.get(supplier_code, {})

#         ship_to = buyer_m.get("default_ship_to") or random.choice(LOCATIONS)["location_code"]
#         bill_to = buyer_m.get("default_bill_to") or random.choice(LOCATIONS)["location_code"]
#         pay_terms = supplier_m.get("default_payment_terms") or random.choice(PAYMENT_TERMS)

#         lead = int(supplier_m.get("lead_time_days", 7))
#         jitter = int(np.clip(
#             np.random.normal(float(REALISM_CFG["expected_ship_jitter_mean"]), float(REALISM_CFG["expected_ship_jitter_std"])),
#             float(REALISM_CFG["expected_ship_jitter_min"]), float(REALISM_CFG["expected_ship_jitter_max"])
#         ))
#         expected_ship_dt = order_dt + timedelta(days=max(1, lead + jitter))

#         po_number = f"PO-{int(time.time() * 1000) % 1000000}-{i}"
#         while po_number in seen_po_numbers:
#             po_number = f"PO-{int(time.time() * 1000) % 1000000}-{i}-{random.randint(0,999)}"
#         seen_po_numbers.add(po_number)

#         po = {
#             "po_id": str(uuid.uuid4()),
#             "po_number": po_number,
#             "buyer_code": buyer_code,
#             "supplier_code": supplier_code,
#             "order_date": order_dt.isoformat(),
#             "expected_ship_date": expected_ship_dt.isoformat(),
#             "ship_to_location": ship_to,
#             "bill_to_location": bill_to,
#             "payment_terms": pay_terms,
#             "currency": random.choice(CURRENCY_CODES),
#             "line_items": [],
#             "anomaly": "NORMAL",
#             "intended_anomaly": "NORMAL",
#             "freight_amount": round(float(np.clip(np.random.normal(45.0, 15.0) * freight_mult, 0, 350)), 2),
#             "discount_amount": round(float(np.clip(np.random.normal(20.0, 12.0), 0, 200)), 2),
#             "tax_amount": round(float(np.clip(np.random.normal(10.0, 8.0), 0, 150)), 2),
#         }

#         for j in range(n_lines):
#             sku = random.choice(SKUS)
#             contract = self._pricing_lookup.get((supplier_code, sku), {})
#             contract_price = float(contract.get("contract_unit_price", self.avg_unit_price))
#             discount_pct = float(contract.get("discount_pct", 0.0))

#             qty_mean = self.avg_quantity * qty_mult
#             qty = int(np.clip(np.random.normal(qty_mean, max(5.0, self.std_quantity)), 1, 5000))

#             raw_price = float(np.clip(np.random.normal(contract_price, max(0.5, contract_price * 0.05)), 0.01, 2000))
#             unit_price = round(raw_price * (1.0 - discount_pct), 2)

#             po["line_items"].append(
#                 {
#                     "line_number": j + 1,
#                     "sku": sku,
#                     "quantity": qty,
#                     "unit_of_measure": random.choice(UNITS_OF_MEASURE),
#                     "unit_price": unit_price,
#                     "contract_unit_price": round(contract_price, 2),
#                     "discount_pct": round(discount_pct, 4),
#                 }
#             )

#         return po

#     def _sanitize_for_clean_normal(self, po: Dict) -> None:
#         # critical fields present
#         if not po.get("payment_terms"):
#             po["payment_terms"] = random.choice(PAYMENT_TERMS)
#         if not po.get("ship_to_location"):
#             po["ship_to_location"] = random.choice(LOCATIONS)["location_code"]
#         if not po.get("bill_to_location"):
#             po["bill_to_location"] = random.choice(LOCATIONS)["location_code"]

#         # valid order_date and not future
#         try:
#             dt = datetime.fromisoformat(str(po.get("order_date")))
#         except Exception:
#             dt = datetime.now() - timedelta(days=5)
#         if dt > datetime.now():
#             dt = datetime.now() - timedelta(days=1)
#         po["order_date"] = dt.isoformat()

#         # known SKUs
#         for li in po.get("line_items", []) or []:
#             li["sku"] = random.choice(SKUS)

#         # ensure contract deviation stays below threshold * frac
#         thr = float(REALISM_CFG["price_outlier_pct_min"])
#         max_dev = thr * float(REALISM_CFG["normal_price_dev_frac_of_threshold"])
#         for li in po.get("line_items", []) or []:
#             cp = float(li.get("contract_unit_price") or 0.0)
#             if cp <= 0:
#                 continue
#             li["unit_price"] = round(cp * random.uniform(1.0 - max_dev, 1.0 + max_dev), 2)

#         # qty cap to avoid accidental z outlier
#         cap = float(self.avg_quantity + float(REALISM_CFG["normal_qty_sigma_cap"]) * self.std_quantity)
#         cap = max(5.0, cap)
#         for li in po.get("line_items", []) or []:
#             q = int(li.get("quantity") or 1)
#             li["quantity"] = int(np.clip(q, 1, int(min(5000, cap))))

#         # benign missingness on non-critical only
#         if random.random() < float(REALISM_CFG["p_missing_freight_normal"]):
#             po["freight_amount"] = None
#         if random.random() < float(REALISM_CFG["p_missing_discount_normal"]):
#             po["discount_amount"] = None
#         if random.random() < float(REALISM_CFG["p_missing_tax_normal"]):
#             po["tax_amount"] = None

#     def _apply_single_anomaly(self, po: Dict, anomaly: str) -> None:
#         # start from clean normal (reduces overlap; keeps classes interpretable)
#         self._sanitize_for_clean_normal(po)

#         if anomaly == "PO_MISSING_FIELDS":
#             which = random.choice(["payment_terms", "ship_to_location", "bill_to_location"])
#             # realistic: sometimes None, sometimes blank
#             po[which] = None if random.random() < 0.7 else ""

#         elif anomaly == "PO_INVALID_DATE":
#             # realistic: some are barely future, some are corrupted
#             if random.random() < 0.75:
#                 po["order_date"] = (datetime.now() + timedelta(days=random.randint(1, 5))).isoformat()
#             else:
#                 po["order_date"] = "BAD_DATE"

#             # keep missing_fields false
#             if not po.get("payment_terms"):
#                 po["payment_terms"] = random.choice(PAYMENT_TERMS)
#             if not po.get("ship_to_location"):
#                 po["ship_to_location"] = random.choice(LOCATIONS)["location_code"]
#             if not po.get("bill_to_location"):
#                 po["bill_to_location"] = random.choice(LOCATIONS)["location_code"]

#         elif anomaly == "PO_UNKNOWN_ITEM":
#             if po.get("line_items"):
#                 k = random.randint(0, len(po["line_items"]) - 1)
#                 base_unknown = f"UNKNOWN-{random.randint(1000,9999)}"
#                 if random.random() < float(REALISM_CFG["p_unknown_sku_typo"]):
#                     # typo-like unknown
#                     real = random.choice(SKUS)
#                     base_unknown = (real[:-1] + str(random.randint(0, 9))) if len(real) > 4 else base_unknown
#                 po["line_items"][k]["sku"] = base_unknown

#         elif anomaly == "PO_PRICE_OUTLIER":
#             if po.get("line_items"):
#                 k = random.randint(0, len(po["line_items"]) - 1)
#                 li = po["line_items"][k]
#                 cp = float(li.get("contract_unit_price") or li.get("unit_price") or self.avg_unit_price)

#                 thr = float(REALISM_CFG["price_outlier_pct_min"])
#                 if random.random() < float(REALISM_CFG["p_borderline_price_outlier"]):
#                     # borderline: just over threshold
#                     over = random.uniform(float(REALISM_CFG["borderline_over_threshold_min"]), float(REALISM_CFG["borderline_over_threshold_max"]))
#                     if random.random() < 0.7:
#                         li["unit_price"] = round(max(0.01, cp * (1.0 + thr * over)), 2)
#                     else:
#                         li["unit_price"] = round(max(0.01, cp * (1.0 - thr * over)), 2)
#                 else:
#                     # strong: your original multipliers
#                     if random.random() < 0.7:
#                         mult = random.uniform(float(REALISM_CFG["price_outlier_mult_hi_min"]), float(REALISM_CFG["price_outlier_mult_hi_max"]))
#                     else:
#                         mult = random.uniform(float(REALISM_CFG["price_outlier_mult_lo_min"]), float(REALISM_CFG["price_outlier_mult_lo_max"]))
#                     li["unit_price"] = round(max(0.01, cp * mult), 2)

#         elif anomaly == "PO_QTY_OUTLIER":
#             if po.get("line_items"):
#                 k = random.randint(0, len(po["line_items"]) - 1)
#                 z_thr = float(REALISM_CFG["qty_outlier_z"])
#                 if random.random() < float(REALISM_CFG["p_borderline_qty_outlier"]):
#                     over = random.uniform(float(REALISM_CFG["borderline_over_threshold_min"]), float(REALISM_CFG["borderline_over_threshold_max"]))
#                     q = int(np.clip(self.avg_quantity + (z_thr * over) * self.std_quantity, 1, 5000))
#                     po["line_items"][k]["quantity"] = q
#                 else:
#                     big = int(np.clip(self.avg_quantity + float(REALISM_CFG["qty_outlier_sigma"]) * self.std_quantity, 50, 5000))
#                     po["line_items"][k]["quantity"] = big

#         # NORMAL already clean
#         po["anomaly"] = anomaly
#         po["intended_anomaly"] = anomaly

#     def generate_pos_with_quotas(self, quotas: Dict[str, int], start_index: int = 0) -> List[Dict]:
#         for k in quotas.keys():
#             if k not in PO_ONLY_CLASSES:
#                 raise ValueError(f"Unknown PO-only class in quotas: {k}")
#         for k in PO_ONLY_CLASSES:
#             quotas.setdefault(k, 0)

#         seen_po_numbers: set[str] = set()
#         pos: List[Dict] = []
#         i = int(start_index)

#         for label in PO_ONLY_CLASSES:
#             target = int(quotas.get(label, 0))
#             if target <= 0:
#                 continue

#             _p(f"[GEN] Target {label}: {target}")
#             for _ in range(target):
#                 po = self._base_po_shell(i=i, seen_po_numbers=seen_po_numbers)
#                 self._apply_single_anomaly(po, label)
#                 pos.append(po)
#                 i += 1

#         random.shuffle(pos)
#         return pos

#     def generate_x12_850(self, po: Dict) -> str:
#         isa_date, isa_time = _now_isa_date_time()
#         isa_ctrl, gs_ctrl, st_ctrl = _make_interchange_ids(seed=_stable_seed_from_str(str(po.get("po_number") or "")))
#         beg_date = _fmt_yyyymmdd_from_iso(po.get("order_date", ""))

#         tx: List[str] = []
#         tx.append(f"ST*850*{st_ctrl}")
#         tx.append(f"BEG*00*SA*{po['po_number']}*{beg_date}")
#         tx.append(f"N1*BY*{po['buyer_code']}")
#         tx.append(f"N1*SU*{po['supplier_code']}")

#         if po.get("payment_terms"):
#             tx.append(f"ITD*01******{po['payment_terms']}")

#         if po.get("freight_amount") is not None:
#             tx.append(f"SAC*C*FREIGHT***{float(po['freight_amount']):.2f}")
#         if po.get("discount_amount") is not None and float(po["discount_amount"]) > 0:
#             tx.append(f"SAC*A*DISCOUNT***{float(po['discount_amount']):.2f}")

#         for idx, item in enumerate(po.get("line_items", []), 1):
#             tx.append(f"PO1*{idx}*{item['quantity']}*{item['unit_of_measure']}*{item['unit_price']}****{item['sku']}")

#         tx.append(f"CTT*{len(po.get('line_items', []))}")
#         se_count = len(tx) + 1
#         se = f"SE*{se_count}*{st_ctrl}"

#         lines: List[str] = []
#         lines.append(f"ISA*00*          *00*          *ZZ*SENDER_ID       *ZZ*RECEIVER_ID     *{isa_date}*{isa_time}*U*00400*{isa_ctrl}*0*P*:")
#         lines.append(f"GS*PO*SENDER*RECEIVER*{datetime.now().strftime('%Y%m%d')}*{datetime.now().strftime('%H%M')}*{gs_ctrl}*X*004010")
#         lines.extend(tx)
#         lines.append(se)
#         lines.append(f"GE*1*{gs_ctrl}")
#         lines.append(f"IEA*1*{isa_ctrl}")
#         return "~".join(lines) + "~"


# def parse_golden_samples() -> List[Dict]:
#     golden_pos: List[Dict] = []
#     parser = X12Parser()

#     if not GOLDEN_SAMPLES_DIR.exists():
#         _p(f"[WARN] golden_schemas dir not found at: {GOLDEN_SAMPLES_DIR}")
#         return golden_pos

#     files = sorted([p for p in GOLDEN_SAMPLES_DIR.rglob("*") if p.is_file()])
#     if not files:
#         _p(f"[WARN] No files found under: {GOLDEN_SAMPLES_DIR}")
#         return golden_pos

#     for file in files:
#         try:
#             if file.suffix.lower() in [".pdf", ".png", ".jpg", ".jpeg"]:
#                 continue
#             segments = parser.parse_file(str(file))
#             tx = parser.detect_transaction_type(segments)
#             if tx == "850":
#                 po = parser.extract_po_data(segments)
#                 if po.get("po_number"):
#                     golden_pos.append(po)
#         except Exception as e:
#             _p(f"[WARN] Failed parsing {file.name}: {e}")

#     return golden_pos


# def build_oracle_labels_po_only(
#     pos: List[Dict],
#     *,
#     price_outlier_pct_min: float,
#     qty_outlier_z: float,
#     known_skus: set,
# ) -> Dict[str, Dict]:
#     all_qty: List[float] = []
#     for po in pos or []:
#         for li in po.get("line_items", []) or []:
#             try:
#                 all_qty.append(float(li.get("quantity") or 0))
#             except Exception:
#                 pass

#     qty_mean = float(np.mean(all_qty) if all_qty else 100.0)
#     qty_std = float(np.std(all_qty) if all_qty else 1.0)
#     qty_std = max(1.0, qty_std)

#     def _parse_iso_dt(val: Optional[str]) -> Optional[datetime]:
#         try:
#             if not val:
#                 return None
#             return datetime.fromisoformat(str(val))
#         except Exception:
#             return None

#     oracle: Dict[str, Dict] = {}
#     for po in pos or []:
#         po_id = str(po.get("po_id") or "")
#         if not po_id:
#             continue

#         flags = {"missing_fields": False, "invalid_date": False, "unknown_item": False, "price_outlier": False, "qty_outlier": False}

#         if (po.get("payment_terms") in (None, "")) or (po.get("ship_to_location") in (None, "")) or (po.get("bill_to_location") in (None, "")):
#             flags["missing_fields"] = True

#         dt = _parse_iso_dt(str(po.get("order_date") or ""))
#         if dt is None or dt > (datetime.now() + timedelta(days=1)):
#             flags["invalid_date"] = True

#         for li in po.get("line_items", []) or []:
#             sku = str(li.get("sku") or "")
#             if sku and (sku not in known_skus):
#                 flags["unknown_item"] = True

#             try:
#                 up = float(li.get("unit_price") or 0.0)
#             except Exception:
#                 up = 0.0
#             try:
#                 cp = float(li.get("contract_unit_price") or 0.0)
#             except Exception:
#                 cp = 0.0

#             if up > 0 and cp > 0:
#                 pct = abs(up - cp) / max(0.01, cp)
#                 if pct >= float(price_outlier_pct_min):
#                     flags["price_outlier"] = True

#             try:
#                 q = float(li.get("quantity") or 0.0)
#             except Exception:
#                 q = 0.0
#             z = abs(q - qty_mean) / qty_std
#             if z >= float(qty_outlier_z):
#                 flags["qty_outlier"] = True

#         if flags["unknown_item"]:
#             label = "PO_UNKNOWN_ITEM"
#         elif flags["invalid_date"]:
#             label = "PO_INVALID_DATE"
#         elif flags["missing_fields"]:
#             label = "PO_MISSING_FIELDS"
#         elif flags["price_outlier"]:
#             label = "PO_PRICE_OUTLIER"
#         elif flags["qty_outlier"]:
#             label = "PO_QTY_OUTLIER"
#         else:
#             label = "NORMAL"

#         oracle[po_id] = {
#             "oracle_anomaly_type": label,
#             "oracle_is_anomaly": label != "NORMAL",
#             "oracle_flags": flags,
#             "oracle_label_version": "po_only_v4_min_realistic",
#             "po_number": str(po.get("po_number") or ""),
#         }

#     return oracle


# def create_anomaly_labels(pos: List[Dict], oracle_labels: Dict[str, Dict]) -> Dict:
#     labels: Dict[str, Dict] = {}
#     for po in pos or []:
#         po_id = str(po.get("po_id") or "")
#         if not po_id:
#             continue
#         o = oracle_labels.get(po_id, {})
#         labels[po_id] = {
#             "anomaly_type": str(po.get("anomaly") or "NORMAL"),
#             "is_anomaly": str(po.get("anomaly") or "NORMAL") != "NORMAL",
#             "intended_anomaly_type": str(po.get("intended_anomaly") or po.get("anomaly") or "NORMAL"),
#             "oracle_anomaly_type": str(o.get("oracle_anomaly_type") or "UNKNOWN"),
#             "oracle_is_anomaly": bool(o.get("oracle_is_anomaly")),
#             "oracle_flags": o.get("oracle_flags") or {},
#             "oracle_label_version": str(o.get("oracle_label_version") or ""),
#         }
#     return labels


# def _apply_label_noise_in_place(pos: List[Dict]) -> None:
#     """
#     Label noise that does NOT change the underlying data.
#     This makes metrics more believable for demos.
#     """
#     p_a2n = float(REALISM_CFG["p_label_noise_anom_to_normal"])
#     p_n2a = float(REALISM_CFG["p_label_noise_normal_to_anom"])

#     anomaly_pool = [c for c in PO_ONLY_CLASSES if c != "NORMAL"]

#     for po in pos:
#         y = str(po.get("anomaly") or "NORMAL")
#         if y != "NORMAL":
#             if random.random() < p_a2n:
#                 po["anomaly"] = "NORMAL"
#         else:
#             if random.random() < p_n2a:
#                 po["anomaly"] = random.choice(anomaly_pool)


# def main():
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--seed", type=int, default=42)
#     ap.add_argument(
#         "--quotas",
#         type=str,
#         default="NORMAL=10000,PO_MISSING_FIELDS=2000,PO_INVALID_DATE=2000,PO_PRICE_OUTLIER=2000,PO_QTY_OUTLIER=2000,PO_UNKNOWN_ITEM=2000",
#     )
#     ap.add_argument("--label-source", choices=["intended", "oracle"], default="oracle")
#     ap.add_argument("--bronze-mode", choices=["all", "sample", "none"], default="sample")
#     ap.add_argument("--bronze-sample-size", type=int, default=2000)

#     # MIN REALISTIC: allow turning label noise off quickly
#     ap.add_argument("--no-label-noise", action="store_true", help="Disable label noise for debugging.")

#     args = ap.parse_args()

#     def _parse_quota_arg(s: str) -> Dict[str, int]:
#         out: Dict[str, int] = {}
#         parts = [p.strip() for p in (s or "").split(",") if p.strip()]
#         for p in parts:
#             if "=" not in p:
#                 raise ValueError(f"Bad quota entry: {p} (expected KEY=NUM)")
#             k, v = p.split("=", 1)
#             k, v = k.strip(), v.strip()
#             if k not in PO_ONLY_CLASSES:
#                 raise ValueError(f"Unknown class in quotas: {k}")
#             out[k] = int(v)
#         return out

#     DATA_DIR.mkdir(parents=True, exist_ok=True)
#     BRONZE_DIR.mkdir(parents=True, exist_ok=True)
#     SILVER_DIR.mkdir(parents=True, exist_ok=True)
#     GOLD_DIR.mkdir(parents=True, exist_ok=True)

#     _p("[1/4] Parsing golden samples...")
#     golden_pos = parse_golden_samples()
#     _p(f"[INFO] Golden POs: {len(golden_pos)}")

#     quotas = _parse_quota_arg(str(args.quotas))
#     _p("[2/4] Generating quota-driven POs...")
#     gen = SyntheticDataGenerator(golden_pos=golden_pos, seed_val=int(args.seed))
#     pos = gen.generate_pos_with_quotas(quotas=quotas)

#     _p("[3/4] Building oracle labels...")
#     oracle_labels = build_oracle_labels_po_only(
#         pos,
#         price_outlier_pct_min=float(REALISM_CFG["price_outlier_pct_min"]),
#         qty_outlier_z=float(REALISM_CFG["qty_outlier_z"]),
#         known_skus=set(SKUS),
#     )

#     if str(args.label_source) == "oracle":
#         for po in pos:
#             oid = str(po.get("po_id") or "")
#             po["anomaly"] = str(oracle_labels.get(oid, {}).get("oracle_anomaly_type") or po.get("anomaly") or "NORMAL")

#     # MIN REALISTIC: label noise to mimic ops/human labeling
#     if not bool(args.no_label_noise):
#         _p("[INFO] Applying label noise (min realistic)...")
#         _apply_label_noise_in_place(pos)

#     labels = create_anomaly_labels(pos, oracle_labels)

#     # Save Silver
#     write_csv(pd.DataFrame(pos), SILVER_DIR / "pos.csv")

#     # Save Gold
#     training_data = {
#         "pos": pos,
#         "asns": [],
#         "invoices": [],
#         "anomaly_labels": labels,
#         "master_data": gen.master_data,
#         "oracle_labels": oracle_labels,
#         "mode": "option_a_po_only",
#         "generator_version": "po_only_v4_min_realistic",
#         "realism_cfg": REALISM_CFG,
#     }
#     (GOLD_DIR / "training_dataset.json").write_text(json.dumps(training_data, indent=2, default=str), encoding="utf-8")
#     _p(f"[OK] Wrote: {GOLD_DIR / 'training_dataset.json'}")

#     # Bronze (optional)
#     if str(args.bronze_mode) != "none":
#         if str(args.bronze_mode) == "sample":
#             rng = np.random.default_rng(int(args.seed))
#             n = len(pos)
#             k = max(1, min(int(args.bronze_sample_size), n))
#             idxs = set(rng.choice(np.arange(n), size=k, replace=False).tolist())
#             subset = [po for i, po in enumerate(pos) if i in idxs]
#         else:
#             subset = pos

#         for po in subset:
#             (BRONZE_DIR / f"PO_{po['po_number']}.850").write_text(gen.generate_x12_850(po), encoding="utf-8")
#         _p(f"[OK] Bronze wrote {len(subset)} files to: {BRONZE_DIR}")

#     _p("[DONE]")


# if __name__ == "__main__":
#     main()



import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import os

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    accuracy_score,
    precision_recall_fscore_support,
)
import joblib

import mlflow
import mlflow.sklearn

# -----------------------------
# Leakage protection (keep)
# -----------------------------
STRICT_LEAK_PATTERNS = [
    r"^oracle_",
    r"^generated_",
    r"^label",
    r"^is_anomaly$",
    r"anomaly[_\- ]?type",
    r"intended[_\- ]?anomaly",

    # split helpers / grouping (never train on)
    r"dup[_\- ]?group",
    r"signature",
    r"group[_\- ]?id",
]

DEFAULT_LABELS = [
    "NORMAL",
    "QTY_ANOMALY",
    "PRICE_ANOMALY",
    "FREIGHT_DISCOUNT_TAX_ANOMALY",
    "DUPLICATE_OR_NEAR_DUPLICATE",
]

DROP_ALWAYS = {"po_id", "po_number", "order_date", "expected_ship_date"}  # raw strings / identifiers


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _p(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def import_lightgbm_or_exit():
    try:
        import lightgbm as lgb  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
        return lgb, LGBMClassifier
    except Exception as e:
        _p("ERROR: Failed importing LightGBM.")
        _p(str(e))
        _p("Fix: pip install lightgbm")
        sys.exit(1)


def set_global_seeds(seed: int) -> None:
    np.random.seed(seed)
    try:
        import random
        random.seed(seed)
    except Exception:
        pass


def load_features(features_path: Path) -> pd.DataFrame:
    if not features_path.exists():
        raise FileNotFoundError(f"Features file not found: {features_path}")
    if features_path.suffix.lower() == ".csv":
        return pd.read_csv(features_path)
    if features_path.suffix.lower() == ".parquet":
        return pd.read_parquet(features_path)
    raise ValueError(f"Unsupported feature file type: {features_path}")


def load_splits_required(splits_path: Path, id_col: str) -> pd.DataFrame:
    if not splits_path.exists():
        raise FileNotFoundError(
            f"Splits file not found: {splits_path}. "
            "Generate realistic splits first (dup-safe grouped time split)."
        )
    sdf = pd.read_csv(splits_path)
    if id_col not in sdf.columns or "split" not in sdf.columns:
        raise ValueError(f"Splits file must contain columns: {id_col}, split")
    sdf[id_col] = sdf[id_col].astype(str)
    sdf["split"] = sdf["split"].astype(str)
    # keep any audit cols too (like dup_group_id), but split is required
    return sdf


def summarize_split_distributions(df: pd.DataFrame, *, label_col: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"splits": {}}
    for sp in ["train", "val", "test"]:
        sub = df[df["split"] == sp]
        out["splits"][sp] = {
            "rows": int(len(sub)),
            "class_counts": sub[label_col].astype(str).value_counts().to_dict(),
        }
    return out


def drop_by_regex(df: pd.DataFrame, *, label_col: str, id_col: str, patterns: List[str]) -> pd.DataFrame:
    drops = []
    for c in df.columns:
        if c in (label_col, id_col, "split"):
            continue
        cl = str(c).lower()
        for pat in patterns:
            if re.search(pat, cl):
                drops.append(c)
                break
    if drops:
        _p(f"[INFO] Dropping {len(drops)} columns by leak patterns (sample): {drops[:25]}")
        df = df.drop(columns=sorted(set(drops)))
    return df


def compute_sample_weights(y: np.ndarray) -> np.ndarray:
    s = pd.Series(y.astype(str))
    counts = s.value_counts()
    n = float(len(s))
    k = float(len(counts))
    w = {cls: n / (k * float(cnt)) for cls, cnt in counts.items()}
    return np.array([w[str(v)] for v in s.values], dtype=float)


def anomaly_binary(y: np.ndarray, normal_label: str = "NORMAL") -> np.ndarray:
    y = pd.Series(np.asarray(y).astype(str))
    return (y != normal_label).astype(int).values


# -----------------------------
# Leakage-safe duplicate signals
# -----------------------------
def compute_dup_group_id(df: pd.DataFrame) -> pd.Series:
    buyer = df.get("buyer_code", "").astype(str).fillna("")
    supp = df.get("supplier_code", "").astype(str).fillna("")
    n_lines = pd.to_numeric(df.get("n_lines", 0), errors="coerce").fillna(0).astype(int)
    dsku = pd.to_numeric(df.get("distinct_sku_count", 0), errors="coerce").fillna(0).astype(int)
    qty = pd.to_numeric(df.get("po_total_qty", 0.0), errors="coerce").fillna(0.0)
    sub = pd.to_numeric(df.get("po_subtotal", 0.0), errors="coerce").fillna(0.0)

    qty_b = (qty / 10.0).round(0).astype(int)
    sub_b = (sub / 50.0).round(0).astype(int)

    def _pct_bucket(col, step=0.01):
        x = pd.to_numeric(df.get(col, np.nan), errors="coerce")
        out = (x / step).round(0).astype("Int64").astype(str)
        return out.fillna("NA")

    fr_b = _pct_bucket("freight_pct_subtotal", 0.01)
    dc_b = _pct_bucket("discount_pct_subtotal", 0.01)
    tx_b = _pct_bucket("tax_pct_subtotal", 0.01)

    return (
        buyer + "||" + supp + "||" +
        n_lines.astype(str) + "||" + dsku.astype(str) + "||" +
        qty_b.astype(str) + "||" + sub_b.astype(str) + "||" +
        fr_b + "||" + dc_b + "||" + tx_b
    )


def add_leakage_safe_duplicate_features(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    """
    Recompute duplicate signals using TRAIN ONLY (no future leakage).
    Produces:
      - dup_group_seen_in_train (0/1)
      - dup_group_count_in_train (count)
    Drops any old global-count columns if present.
    """
    df = df.copy()
    df["dup_group_id"] = compute_dup_group_id(df)

    train_mask = df["split"].astype(str) == "train"
    train_groups = df.loc[train_mask, "dup_group_id"].astype(str)

    counts = train_groups.value_counts()
    df["dup_group_count_in_train"] = df["dup_group_id"].astype(str).map(counts).fillna(0).astype(int)
    df["dup_group_seen_in_train"] = (df["dup_group_count_in_train"] > 0).astype(int)

    # Remove any previously-leaky global duplicate count columns, if you had them
    for col in ["exact_po_signature_count", "sku_set_signature_count"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # NOTE: dup_group_id should NOT be used for training; it'll be dropped by regex anyway.
    return df


def infer_feature_columns(df: pd.DataFrame, label_col: str, id_col: str, extra_drop: List[str]) -> Tuple[List[str], List[str]]:
    drop_cols = {label_col, id_col, "split"} | DROP_ALWAYS
    for c in extra_drop or []:
        if c:
            drop_cols.add(c)

    feat_cols = [c for c in df.columns if c not in drop_cols]

    cat_cols, num_cols = [], []
    for c in feat_cols:
        if pd.api.types.is_numeric_dtype(df[c]):
            num_cols.append(c)
        else:
            cat_cols.append(c)

    return num_cols, cat_cols


def split_sets(df: pd.DataFrame):
    train_df = df[df["split"] == "train"].copy()
    val_df = df[df["split"] == "val"].copy()
    test_df = df[df["split"] == "test"].copy()
    if train_df.empty or val_df.empty or test_df.empty:
        raise ValueError("Missing split rows. Ensure splits contain train/val/test.")
    return train_df, val_df, test_df


def log_feature_importance(pre: ColumnTransformer, model, outdir: Path) -> Optional[Path]:
    try:
        if not hasattr(model, "feature_importances_"):
            return None
        try:
            feat_names = pre.get_feature_names_out()
        except Exception:
            feat_names = None

        importances = np.asarray(model.feature_importances_, dtype=float)
        if feat_names is None or len(feat_names) != len(importances):
            df_imp = pd.DataFrame({
                "feature": [f"f_{i}" for i in range(len(importances))],
                "importance": importances,
            }).sort_values("importance", ascending=False)
        else:
            df_imp = pd.DataFrame({
                "feature": [str(x) for x in feat_names],
                "importance": importances,
            }).sort_values("importance", ascending=False)

        out_path = outdir / "feature_importance.csv"
        df_imp.head(300).to_csv(out_path, index=False)
        return out_path
    except Exception as e:
        _p(f"[WARN] Could not write feature importance: {e}")
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default="../data_gen/data/gold/po_features_optionA.csv")
    ap.add_argument("--splits", default="../data_gen/data/gold/po_splits.csv")
    ap.add_argument("--label-col", default="anomaly_type")
    ap.add_argument("--id-col", default="po_id")
    ap.add_argument("--outdir", default="artifacts/lightgbm_po_only")

    ap.add_argument("--n-estimators", type=int, default=4000)
    ap.add_argument("--learning-rate", type=float, default=0.03)
    ap.add_argument("--num-leaves", type=int, default=127)
    ap.add_argument("--max-depth", type=int, default=-1)
    ap.add_argument("--subsample", type=float, default=0.9)
    ap.add_argument("--colsample-bytree", type=float, default=0.9)
    ap.add_argument("--min-child-samples", type=int, default=20)
    ap.add_argument("--reg-lambda", type=float, default=1.0)
    ap.add_argument("--early-stopping-rounds", type=int, default=100)

    ap.add_argument("--balanced", action="store_true", help="Use inverse-frequency sample weights (recommended).")
    ap.add_argument("--drop-cols", default="", help="Extra comma-separated columns to drop (optional).")

    ap.add_argument("--labels", default=",".join(DEFAULT_LABELS), help="Comma-separated fixed label order.")
    ap.add_argument("--seed", type=int, default=42)

    args = ap.parse_args()
    set_global_seeds(int(args.seed))

    labels_fixed = [x.strip() for x in str(args.labels).split(",") if x.strip()]

    # MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "https://dagshub.com/youl1/supplylens_ml.mlflow"))
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME", "po_only_lightgbm"))

    run_name = f"lgbm_po_only_realistic_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params({
            "features_path": args.features,
            "splits_path": args.splits,
            "label_col": args.label_col,
            "seed": int(args.seed),
            "balanced": bool(args.balanced),
            "labels_fixed": ",".join(labels_fixed),

            "n_estimators": int(args.n_estimators),
            "learning_rate": float(args.learning_rate),
            "num_leaves": int(args.num_leaves),
            "max_depth": int(args.max_depth),
            "subsample": float(args.subsample),
            "colsample_bytree": float(args.colsample_bytree),
            "min_child_samples": int(args.min_child_samples),
            "reg_lambda": float(args.reg_lambda),
            "early_stopping_rounds": int(args.early_stopping_rounds),

            "strict_leak_patterns_count": int(len(STRICT_LEAK_PATTERNS)),
        })

        mlflow.set_tags({
            "track": "po_only",
            "option": "A",
            "model_family": "lightgbm",
            "split_strategy": "dup_safe_grouped_time",
            "leakage_safe_duplicate_features": "train_only_counts",
        })

        _p("Step 0/8: Importing LightGBM...")
        lgb, LGBMClassifier = import_lightgbm_or_exit()

        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        _p("Step 1/8: Loading features...")
        df = load_features(Path(args.features))
        if args.id_col not in df.columns:
            raise ValueError(f"Features missing id column: {args.id_col}")
        if args.label_col not in df.columns:
            raise ValueError(f"Features missing label column: {args.label_col}")

        df[args.id_col] = df[args.id_col].astype(str)
        df[args.label_col] = df[args.label_col].astype(str)

        if df[args.id_col].duplicated().any():
            dup_n = int(df[args.id_col].duplicated().sum())
            _p(f"[WARN] Features has {dup_n} duplicated {args.id_col}. Keeping first.")
            df = df.drop_duplicates(args.id_col, keep="first")

        _p("Step 2/8: Loading/attaching splits (required)...")
        splits_df = load_splits_required(Path(args.splits), args.id_col)
        df = df.merge(splits_df, on=args.id_col, how="inner")
        if df.empty:
            raise RuntimeError("No overlap between features and splits. Check id_col alignment.")

        dist = summarize_split_distributions(df, label_col=args.label_col)
        (outdir / "split_distributions.json").write_text(json.dumps(dist, indent=2), encoding="utf-8")
        mlflow.log_dict(dist, "split_distributions.json")

        _p("Step 3/8: Add leakage-safe duplicate signals (train-only)...")
        df = add_leakage_safe_duplicate_features(df, label_col=args.label_col)

        _p("Step 4/8: Strict leakage dropping (regex)...")
        df = drop_by_regex(df, label_col=args.label_col, id_col=args.id_col, patterns=STRICT_LEAK_PATTERNS)

        extra_drop = [c.strip() for c in str(args.drop_cols).split(",") if c.strip()]
        num_cols, cat_cols = infer_feature_columns(df, args.label_col, args.id_col, extra_drop)
        _p(f"Feature columns: num={len(num_cols)}, cat={len(cat_cols)}")

        train_df, val_df, test_df = split_sets(df)
        _p(f"Rows: total={len(df)}, train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")

        X_train = train_df[num_cols + cat_cols]
        X_val = val_df[num_cols + cat_cols]
        X_test = test_df[num_cols + cat_cols]

        y_train = train_df[args.label_col].astype(str).values
        y_val = val_df[args.label_col].astype(str).values
        y_test = test_df[args.label_col].astype(str).values

        _p("Step 5/8: Preprocessing...")
        pre = ColumnTransformer(
            transformers=[
                ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), num_cols),
                ("cat", Pipeline([
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("ohe", OneHotEncoder(handle_unknown="ignore")),
                ]), cat_cols),
            ],
            remainder="drop",
            sparse_threshold=0.3,
        )

        X_train_t = pre.fit_transform(X_train)
        X_val_t = pre.transform(X_val)
        X_test_t = pre.transform(X_test)

        sample_weight = compute_sample_weights(y_train) if bool(args.balanced) else None
        _p("[OK] Using inverse-frequency sample weights." if bool(args.balanced) else "[INFO] --balanced not set.")

        _p("Step 6/8: Training...")
        model = LGBMClassifier(
            objective="multiclass",
            n_estimators=int(args.n_estimators),
            learning_rate=float(args.learning_rate),
            num_leaves=int(args.num_leaves),
            max_depth=int(args.max_depth),
            subsample=float(args.subsample),
            colsample_bytree=float(args.colsample_bytree),
            min_child_samples=int(args.min_child_samples),
            reg_lambda=float(args.reg_lambda),
            n_jobs=-1,
            random_state=int(args.seed),
        )

        fit_kwargs = {
            "X": X_train_t,
            "y": y_train,
            "sample_weight": sample_weight,
            "eval_set": [(X_val_t, y_val)],
            "eval_metric": "multi_logloss",
        }

        callbacks = []
        if int(args.early_stopping_rounds) > 0:
            try:
                callbacks.append(lgb.early_stopping(int(args.early_stopping_rounds), verbose=False))
            except Exception:
                callbacks = []

        if callbacks:
            try:
                model.fit(**fit_kwargs, callbacks=callbacks)
            except TypeError:
                model.fit(**fit_kwargs)
        else:
            model.fit(**fit_kwargs)

        _p("Step 7/8: Evaluating (stable label order)...")
        val_pred = model.predict(X_val_t)
        test_pred = model.predict(X_test_t)

        # use fixed label order for consistent MLflow metrics across runs
        labels_eval = [lab for lab in labels_fixed if lab in set(y_train) | set(y_val) | set(y_test)]
        if not labels_eval:
            labels_eval = sorted(list(set(y_train) | set(y_val) | set(y_test)))

        val_macro_f1 = f1_score(y_val, val_pred, labels=labels_eval, average="macro")
        test_macro_f1 = f1_score(y_test, test_pred, labels=labels_eval, average="macro")
        test_acc = accuracy_score(y_test, test_pred)

        cm = confusion_matrix(y_test, test_pred, labels=labels_eval).tolist()
        report = classification_report(y_test, test_pred, labels=labels_eval, output_dict=True, zero_division=0)

        yb_val = anomaly_binary(y_val)
        yb_val_pred = anomaly_binary(val_pred)
        yb_test = anomaly_binary(y_test)
        yb_test_pred = anomaly_binary(test_pred)

        val_bin_f1 = f1_score(yb_val, yb_val_pred, average="binary")
        test_bin_f1 = f1_score(yb_test, yb_test_pred, average="binary")

        mlflow.log_metrics({
            "val_macro_f1": float(val_macro_f1),
            "test_macro_f1": float(test_macro_f1),
            "test_accuracy": float(test_acc),
            "val_anomaly_binary_f1": float(val_bin_f1),
            "test_anomaly_binary_f1": float(test_bin_f1),
            "train_rows": int(len(train_df)),
            "val_rows": int(len(val_df)),
            "test_rows": int(len(test_df)),
            "num_cols": int(len(num_cols)),
            "cat_cols": int(len(cat_cols)),
        })

        try:
            pr, rc, f1s, sup = precision_recall_fscore_support(
                y_test, test_pred, labels=labels_eval, zero_division=0
            )
            per_class = {}
            for i, lab in enumerate(labels_eval):
                per_class[f"test_f1__{lab}"] = float(f1s[i])
                per_class[f"test_precision__{lab}"] = float(pr[i])
                per_class[f"test_recall__{lab}"] = float(rc[i])
            mlflow.log_metrics(per_class)
        except Exception as e:
            _p(f"[WARN] Could not log per-class metrics to MLflow: {e}")

        metrics = {
            "model": "lightgbm_po_only_realistic",
            "val_macro_f1": float(val_macro_f1),
            "test_macro_f1": float(test_macro_f1),
            "test_accuracy": float(test_acc),
            "val_anomaly_binary_f1": float(val_bin_f1),
            "test_anomaly_binary_f1": float(test_bin_f1),
            "labels_eval": labels_eval,
            "confusion_matrix": cm,
            "classification_report": report,
            "num_cols": num_cols,
            "cat_cols": cat_cols,
            "strict_leak_patterns": STRICT_LEAK_PATTERNS,
            "notes": "Duplicate signals recomputed from TRAIN ONLY to avoid future leakage.",
        }

        mlflow.log_dict(metrics, "metrics.json")

        _p("Step 8/8: Saving artifacts...")
        pipeline_obj = {"preprocessor": pre, "model": model}
        joblib.dump(pipeline_obj, outdir / "model.joblib")
        (outdir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        fi_path = log_feature_importance(pre, model, outdir)
        if fi_path is not None:
            mlflow.log_artifact(str(fi_path))

        mlflow.log_artifact(str(outdir / "model.joblib"))
        mlflow.log_artifact(str(outdir / "metrics.json"))
        mlflow.log_artifact(str(outdir / "split_distributions.json"))

        inference_pipeline = Pipeline(steps=[
            ("preprocessor", pre),
            ("model", model),
        ])
        mlflow.sklearn.log_model(inference_pipeline, artifact_path="model")

        _p(json.dumps(
            {
                "val_macro_f1": float(val_macro_f1),
                "test_macro_f1": float(test_macro_f1),
                "test_accuracy": float(test_acc),
                "test_anomaly_binary_f1": float(test_bin_f1),
            },
            indent=2
        ))
        _p(f"Saved: {outdir / 'model.joblib'} and {outdir / 'metrics.json'}")


if __name__ == "__main__":
    main()