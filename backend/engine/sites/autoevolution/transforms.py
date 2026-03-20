"""
sites/autoevolution/transforms.py — Engine name resolution chain.

Builds human-readable engine names from three cascading strategies:
1. Anchor ID decode (e.g. aeng_bmw-3er-20l-4cyl → "2.0L 4 6MT RWD (184 HP)")
2. Spec-derived name (from Displacement, Cylinders, Power, Gearbox, Drive Type)
3. li title parsing from sidebar

Extracted from extract_l4_fixed.py L782-1053.
"""

import re
from typing import List, Optional


def _find_first_value(spec_sections: List[dict], label_list: List[str]) -> str:
    """Find the first spec value matching any of the given labels (case-insensitive)."""
    targets = {l.lower() for l in label_list}
    for sec in spec_sections or []:
        for item in sec.get("items", []):
            if str(item.get("label", "")).lower().strip() in targets:
                return str(item.get("value", "")).strip()
    return ""


def _parse_liters_from_displacement(disp_value: str) -> Optional[str]:
    """Extract liters from displacement string (e.g. '1998 cc' → '2.0L')."""
    if not disp_value:
        return None
    m = re.search(r"(\d[\d,]*)\s*cc", disp_value, re.IGNORECASE)
    if m:
        cc = float(m.group(1).replace(",", ""))
        liters = cc / 1000.0
        return f"{liters:.1f}L"
    m = re.search(r"(\d+(?:\.\d+)?)\s*[Ll]", disp_value)
    if m:
        liters = float(m.group(1))
        return f"{liters:.1f}L"
    return None


def _parse_hp(val: str) -> Optional[int]:
    """Extract horsepower from a power string."""
    if not val:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*HP\b", val.upper())
    if not m:
        return None
    try:
        return int(round(float(m.group(1))))
    except Exception:
        return None


def _gearbox_code(val: str) -> str:
    """Convert gearbox string to compact code (e.g. '6-Speed Manual' → '6MT')."""
    if not val:
        return ""
    u = val.upper()
    m = re.search(r"(\d+)\s*[- ]*SPEED", u)
    n = m.group(1) if m else ""
    if "MANUAL" in u:
        return f"{n}MT" if n else "MT"
    if "AUTOMATIC" in u:
        return f"{n}AT" if n else "AT"
    return n or val.strip()


def _drive_code(val: str) -> str:
    """Convert drive type string to compact code."""
    if not val:
        return ""
    u = val.upper()
    if "REAR WHEEL" in u or "RWD" in u:
        return "RWD"
    if "FRONT WHEEL" in u or "FWD" in u:
        return "FWD"
    if "ALL WHEEL" in u or "AWD" in u or "4WD" in u:
        return "AWD"
    return val.strip()


def _parse_kwh_from_sections(spec_sections: List[dict]) -> Optional[str]:
    """Extract battery capacity in KWH from spec sections."""
    best = None
    for sec in spec_sections or []:
        for item in sec.get("items", []):
            lbl = str(item.get("label", "")).upper()
            val = str(item.get("value", "")).upper()
            if "CAPACITY" in lbl and "KWH" in val:
                m = re.search(r"(\d+(?:\.\d+)?)\s*KWH", val)
                if m:
                    v = float(m.group(1))
                    if best is None or v > best:
                        best = v
    if best is None:
        return None
    if best.is_integer():
        return f"{int(best)} KWH"
    return f"{best:.1f} KWH"


# ── Strategy 1: Build name from anchor ID ──────────────────────────────────

def build_engine_name_from_anchor(anchor: str, spec_sections: List[dict]) -> str:
    """
    Decode engine name from anchor ID.

    Examples:
        aeng_maserati-bora-1971-49-v8-5mt-280-hp → 4.9L V8 5MT (280 HP)
        aeng_bmw-3er-20l-4cyl-6mt-rwd-184-hp → 2.0L 4 6MT RWD (184 HP)
    """
    if not anchor:
        return ""

    tail = str(anchor).strip().lower()
    if tail.startswith("aeng_"):
        tail = tail[5:]
    parts = [p for p in tail.split("-") if p]
    if not parts:
        return ""

    # Find start of engine descriptor tokens (skip model name/year prefix)
    start = None
    for i, tok in enumerate(parts):
        if re.fullmatch(r"\d{4}", tok):
            continue
        if re.fullmatch(r"\d+l", tok) or tok in {"ev", "bev"}:
            start = i
            break
        if re.fullmatch(r"\d+[k]?wh", tok):
            start = i
            break
    if start is None:
        for i, tok in enumerate(parts):
            if re.fullmatch(r"\d{2,3}", tok):
                start = i
                break
    if start is None:
        return ""

    toks = parts[start:]
    liters = kwh = cyl = gearbox = drive = hp = ""

    i = 0
    while i < len(toks):
        tok = toks[i]

        if not liters and re.fullmatch(r"\d+l", tok):
            num = tok[:-1]
            if len(num) == 1:
                liters = f"{num}.0L"
            elif len(num) == 2:
                liters = f"{num[0]}.{num[1]}L"
            elif len(num) == 3:
                liters = f"{num[:-1]}.{num[-1]}L"
            else:
                liters = f"{num}L"
            i += 1
            continue
        if not kwh and re.fullmatch(r"\d+kwh", tok):
            kwh = f"{tok[:-3]} KWH"
            i += 1
            continue
        # Handle split KWH: "925-kwh" → two tokens
        if not kwh and re.fullmatch(r"\d{2,4}", tok) and i + 1 < len(toks) and toks[i + 1] == "kwh":
            num = tok
            if len(num) == 2:
                kwh = f"{num[0]}.{num[1]} KWH"
            elif len(num) == 3:
                kwh = f"{num[:-1]}.{num[-1]} KWH"
            elif len(num) == 4:
                kwh = f"{num[:-1]}.{num[-1]} KWH"
            else:
                kwh = f"{num} KWH"
            i += 2
            continue
        if not liters and not kwh and re.fullmatch(r"\d{2}", tok):
            liters = f"{tok[0]}.{tok[1]}L"
            i += 1
            continue
        if not liters and not kwh and re.fullmatch(r"\d{3}", tok):
            liters = f"{tok[:-1]}.{tok[-1]}L"
            i += 1
            continue
        if not cyl and re.fullmatch(r"v\d+", tok):
            cyl = tok.upper()
            i += 1
            continue
        if not gearbox and re.fullmatch(r"\d+[am]t", tok):
            gearbox = tok.upper()
            i += 1
            continue
        if not drive and tok in {"rwd", "fwd", "awd", "4wd"}:
            drive = "AWD" if tok == "4wd" else tok.upper()
            i += 1
            continue
        if not hp and re.fullmatch(r"\d+", tok) and i + 1 < len(toks) and toks[i + 1] == "hp":
            hp = tok
            i += 2
            continue
        i += 1

    # Fill gaps from spec sections
    if not liters:
        disp = _find_first_value(spec_sections, ["Displacement"])
        liters = _parse_liters_from_displacement(disp) or ""
    if not cyl:
        cyl = _find_first_value(spec_sections, ["Cylinders"]) or ""
    if not gearbox:
        gearbox = _gearbox_code(_find_first_value(spec_sections, ["Gearbox"])) or ""
    if not drive:
        drive = _drive_code(_find_first_value(spec_sections, ["Drive Type"])) or ""
    if not hp:
        parsed = _parse_hp(_find_first_value(spec_sections, ["Power", "Total maximum power", "Electrical motor power"]))
        hp = str(parsed) if parsed else ""
    if not kwh:
        kwh = _parse_kwh_from_sections(spec_sections) or ""

    core_parts = []
    if kwh:
        core_parts.append(kwh)
    elif liters:
        core_parts.append(liters)
    if cyl:
        core_parts.append(cyl)
    if gearbox:
        core_parts.append(gearbox)
    if drive:
        core_parts.append(drive)

    core = " ".join(p for p in core_parts if p).strip()
    if hp and core:
        return f"{core} ({hp} HP)"
    if hp:
        return f"{hp} HP"
    return core


# ── Strategy 2: Build name from spec values ────────────────────────────────

def build_engine_name_from_specs(spec_sections: List[dict]) -> str:
    """
    Derive canonical engine name from structured spec sections.
    ICE: "<liters>L <cyl> <gearbox> <drive> (<hp> HP)"
    EV:  "<kwh> <drive> (<hp> HP)"
    """
    fuel = _find_first_value(spec_sections, ["Fuel"]).upper()

    # EV-style name
    if "ELECTRIC" in fuel:
        kwh = _parse_kwh_from_sections(spec_sections)
        drive = _drive_code(_find_first_value(spec_sections, ["Drive Type"]))
        power_val = _find_first_value(spec_sections, ["Total maximum power", "Electrical motor power", "Power"])
        hp = _parse_hp(power_val)

        parts = []
        if kwh:
            parts.append(kwh)
        if drive:
            parts.append(drive)
        if hp:
            parts.append(f"({hp} HP)")
        return " ".join(parts).strip()

    # ICE-style name
    cyl = _find_first_value(spec_sections, ["Cylinders"])
    disp = _find_first_value(spec_sections, ["Displacement"])
    liters = _parse_liters_from_displacement(disp)
    power_val = _find_first_value(spec_sections, ["Power"])
    hp = _parse_hp(power_val)
    gb = _gearbox_code(_find_first_value(spec_sections, ["Gearbox"]))
    drive = _drive_code(_find_first_value(spec_sections, ["Drive Type"]))

    name_parts = []
    if liters:
        name_parts.append(liters)
    if cyl:
        name_parts.append(cyl)
    tail = " ".join(p for p in [gb, drive] if p)
    if tail:
        name_parts.append(tail)

    core = " ".join(name_parts).strip()
    if hp:
        return f"{core} ({hp} HP)" if core else f"{hp} HP"
    return core or ""
