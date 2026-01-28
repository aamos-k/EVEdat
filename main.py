import os
import json
import time
import sqlite3
import requests
import pandas as pd
from collections import defaultdict
from functools import lru_cache

# ============================================================================
# CONFIGURATION
# ============================================================================
DB_PATH = "sqlite-latest.sqlite"
REGION_ID = 10000002  # Jita
UNITS_TO_COMPARE = 10

ISK_BUDGET = 500_000_000

ENABLE_SELF_SUFFICIENT = True        # Mining + PI mode
INCLUDE_BLUEPRINT_COST = True
BLUEPRINT_RUNS = 4000                # BPO amortization runs

MIN_DAILY_VOLUME = 5                 # volume filter
EXCLUDE_TECH_2 = True

REPROCESSING_EFFICIENCY = 0.72
STRIP_MINER_YIELD = 540       # m3
STRIP_MINER_CYCLE = 180       # seconds

VOLUME_CACHE_FILE = "cached_volumes.json"
VOLUME_DAYS = 30              # Your chosen 30-day window

# ============================================================================
# DATABASE LOAD — FULL SDE INTO MEMORY
# ============================================================================
print("Loading SDE…")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# invTypes table
types = {}
for tid, name, vol, groupID in cur.execute("""
    SELECT typeID, typeName, volume, groupID FROM invTypes
"""):
    types[tid] = {
        "name": name,
        "volume": float(vol),
        "groupID": groupID
    }

# invGroups → category lookup
groups = {}
for gid, cid in cur.execute("SELECT groupID, categoryID FROM invGroups"):
    groups[gid] = cid

# All blueprint → product → output quantity
product_to_bp = {}
product_output_qty = {}

for bp_typeID, productTypeID, activityID, qty in cur.execute("""
    SELECT typeID, productTypeID, activityID, quantity
    FROM industryActivityProducts
    WHERE activityID = 1
"""):
    product_to_bp[productTypeID] = bp_typeID
    product_output_qty[productTypeID] = qty

# Materials table
bp_materials = defaultdict(list)
for bp_typeID, matTypeID, qty in cur.execute("""
    SELECT typeID, materialTypeID, quantity
    FROM industryActivityMaterials
    WHERE activityID = 1
"""):
    bp_materials[bp_typeID].append((matTypeID, qty))

print("SDE loaded.")

# ============================================================================
# PRICE LOADING — BATCH FROM FUZZWORK
# ============================================================================
def preload_prices(type_ids, chunk_size=200):
    """Batch-query prices in chunks to avoid Fuzzwork JSON errors.
    Returns a dict: { typeID: price }"""
    
    print(f"Loading prices for {len(type_ids)} items in chunks of {chunk_size}…")

    prices = {}

    # break into chunks
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    for i, chunk in enumerate(chunks(type_ids, chunk_size)):
        chunk_str = ",".join(map(str, chunk))
        url = f"https://market.fuzzwork.co.uk/aggregates/?region={REGION_ID}&types={chunk_str}"

        try:
            r = requests.get(url, timeout=20)
            try:
                data = r.json()
            except:
                print(f"[WARN] JSON decode failed on chunk {i}. Retrying in 2 seconds…")
                time.sleep(2)
                r = requests.get(url, timeout=20)
                data = r.json()  # if this fails, let it crash

        except Exception as e:
            print(f"[ERROR] Failed chunk {i}: {e}")
            continue

        for tid in chunk:
            try:
                prices[tid] = float(data[str(tid)]["sell"]["min"])
            except:
                prices[tid] = 0.0  # missing → treat as zero

        time.sleep(0.2)  # Prevent rate limits

    print("Price load complete.")
    return prices


# ============================================================================
# VOLUME LOADING — CACHED 30-DAY AVERAGES
# ============================================================================
def generate_volume_cache(type_ids):
    """Calls ESI once per item (slow) then produces cached_volumes.json"""

    print(f"Generating cached {VOLUME_DAYS}-day volume file…")
    volumes = {}

    for i, tid in enumerate(type_ids):
        if i % 50 == 0:
            print(f"  {i}/{len(type_ids)}")

        url = (
            f"https://esi.evetech.net/latest/markets/{REGION_ID}/history/"
            f"?datasource=tranquility&type_id={tid}"
        )
        try:
            r = requests.get(url, timeout=20)
            hist = r.json()

            if not hist:
                volumes[tid] = 0
                continue

            window = hist[-VOLUME_DAYS:] if len(hist) >= VOLUME_DAYS else hist

            avg_vol = sum(day.get("volume", 0) for day in window) / len(window)
            volumes[tid] = avg_vol

        except:
            volumes[tid] = 0

        time.sleep(0.2)  # avoid rate limits

    with open(VOLUME_CACHE_FILE, "w") as f:
        json.dump(volumes, f, indent=2)

    print("Volume cache generated.")
    return volumes


def load_volume_cache(type_ids):
    """Load cached volumes or generate if missing."""
    if os.path.exists(VOLUME_CACHE_FILE):
        print("Loading cached volume data…")
        with open(VOLUME_CACHE_FILE) as f:
            return json.load(f)

    return generate_volume_cache(type_ids)

# ============================================================================
# BASIC LOOKUPS
# ============================================================================
def get_price(tid):
    return prices.get(tid, 0.0)

def get_volume(tid):
    return volumes.get(str(tid), volumes.get(tid, 999999))

def get_materials(product_tid):
    bp_tid = product_to_bp.get(product_tid)
    if not bp_tid:
        return []
    return bp_materials.get(bp_tid, [])

def get_output_qty(product_tid):
    return product_output_qty.get(product_tid, 1)

def is_mineral(tid):
    return groups.get(types[tid]["groupID"]) == 18

def is_pi(tid):
    return groups.get(types[tid]["groupID"]) == 43

# ============================================================================
# MINING TIME — simplified (same as your script)
# ============================================================================
@lru_cache(None)
def mining_time_for(mineral_tid, qty):
    # minimal model because reprocessing table would require extra join
    # You can expand later if needed
    cycles = qty / (REPROCESSING_EFFICIENCY * 1)   # assume 1 unit per ore
    seconds = cycles * STRIP_MINER_CYCLE
    return seconds / 3600

# ============================================================================
# RECURSIVE COST CALCULATION (OPTIMIZED)
# ============================================================================
@lru_cache(None)
def cost_to_build(tid):
    """Return (isk_cost, mining_hours, pi_hours, bp_cost, bp_price_one_time)."""

    # No blueprint = raw material
    if tid not in product_to_bp:
        if ENABLE_SELF_SUFFICIENT:
            if is_mineral(tid):
                return (0, 0, 0, 0, 0)
            if is_pi(tid):
                return (0, 0, 0.01, 0, 0)
        return (get_price(tid), 0, 0, 0, 0)

    materials = get_materials(tid)

    total_isk = 0
    mine_hours = 0
    pi_hours = 0
    bp_cost_total = 0

    for mat_id, qty in materials:

        # Self-sufficient logic
        if ENABLE_SELF_SUFFICIENT:
            if is_mineral(mat_id):
                mine_hours += mining_time_for(mat_id, qty)
                continue
            if is_pi(mat_id):
                pi_hours += qty * 0.01
                continue

        sub_isk, sub_mine, sub_pi, sub_bp_cost, sub_bp_price = cost_to_build(mat_id)

        total_isk += sub_isk * qty
        mine_hours += sub_mine * qty
        pi_hours += sub_pi * qty
        bp_cost_total += sub_bp_cost * qty

    # Add blueprint amortization
    bp_tid = product_to_bp.get(tid)
    bp_price_full = get_price(bp_tid)

    amortized = (bp_price_full / BLUEPRINT_RUNS) if INCLUDE_BLUEPRINT_COST and BLUEPRINT_RUNS > 0 else 0
    bp_cost_total += amortized

    # divide by output quantity
    out_qty = get_output_qty(tid)
    if out_qty > 1:
        total_isk /= out_qty
        mine_hours /= out_qty
        pi_hours /= out_qty
        bp_cost_total /= out_qty

    return (total_isk, mine_hours, pi_hours, bp_cost_total, bp_price_full)

# ============================================================================
# ALL PRODUCT FILTERING (same as your script)
# ============================================================================
def get_all_products():
    excluded_market_groups = set()
    exclude_names = ['Special Edition Ships', 'Faction & Storyline', 'Faction Warfare']

    for name in exclude_names:
        try:
            rows = cur.execute("""
                WITH RECURSIVE mg AS (
                    SELECT marketGroupID
                    FROM invMarketGroups
                    WHERE marketGroupName LIKE ?
                    UNION ALL
                    SELECT m.marketGroupID
                    FROM invMarketGroups m
                    JOIN mg ON m.parentGroupID = mg.marketGroupID
                )
                SELECT marketGroupID FROM mg
            """, (f"%{name}%",)).fetchall()
            excluded_market_groups.update(int(r[0]) for r in rows)
        except:
            pass

    group_filter = ""
    if excluded_market_groups:
        group_filter = "AND t.marketGroupID NOT IN (" + ",".join(map(str, excluded_market_groups)) + ")"

    tech2_filter = "AND t.typeName NOT LIKE '% II'" if EXCLUDE_TECH_2 else ""

    query = f"""
        SELECT DISTINCT p.productTypeID
        FROM industryActivityProducts p
        JOIN industryActivity a ON p.typeID = a.typeID
        JOIN invTypes t ON p.productTypeID = t.typeID
        JOIN invGroups g ON t.groupID = g.groupID
        WHERE a.activityID = 1
          AND g.categoryID NOT IN (29, 2)
          AND t.published = 1
          AND t.marketGroupID IS NOT NULL
          {tech2_filter}
          {group_filter}
    """

    rows = cur.execute(query).fetchall()
    return [int(r[0]) for r in rows]

# ============================================================================
# MAIN ANALYSIS
# ============================================================================
def analyze():
    product_ids = get_all_products()
    print(f"Found {len(product_ids)} manufacturable products.")

    # preload prices
    global prices
    prices = preload_prices(product_ids + list(product_to_bp.values()))

    # preload volumes
    global volumes
    volumes = load_volume_cache(product_ids)

    results = []

    for i, tid in enumerate(product_ids):
        if i % 100 == 0:
            print(f"{i}/{len(product_ids)}")

        volume = get_volume(tid)
        if volume < MIN_DAILY_VOLUME:
            continue

        build_isk, mine_h, pi_h, bp_cost, bp_price_full = cost_to_build(tid)

        build_isk *= UNITS_TO_COMPARE
        mine_h *= UNITS_TO_COMPARE
        pi_h *= UNITS_TO_COMPARE
        bp_cost *= UNITS_TO_COMPARE

        sell_value = get_price(tid) * UNITS_TO_COMPARE
        total_cost = build_isk + bp_cost
        profit = sell_value - total_cost

        entry = {
            "type_id": tid,
            "name": types[tid]["name"],
            "build_cost": build_isk,
            "blueprint_cost": bp_cost,
            "blueprint_price": bp_price_full,
            "total_cost": total_cost,
            "sell_value": sell_value,
            "profit": profit,
            "daily_volume": volume,
            "mining_hours": mine_h,
            "pi_hours": pi_h,
            "total_hours": mine_h + pi_h,
        }

        entry["isk_per_hour"] = (
            profit / (entry["total_hours"]) if entry["total_hours"] > 0 else 0
        )

        results.append(entry)

    df = pd.DataFrame(results)
    df = df.sort_values("isk_per_hour", ascending=False)
    return df

# ============================================================================
# RUN SCRIPT
# ============================================================================
if __name__ == "__main__":
    df = analyze()

    if len(df) == 0:
        print("No valid items found after filtering.")
    else:
        print("\n=== TOP 20 BY ISK/HOUR ===\n")
        print(df[[
            "name", "profit", "blueprint_cost", "blueprint_price",
            "daily_volume", "mining_hours", "pi_hours",
            "total_hours", "isk_per_hour"
        ]].head(20))

        print("\n=== WORST 20 ===\n")
        print(df.tail(20))

        df.to_csv("industry_profit_report_full_optimized.csv", index=False)
        print("\nSaved to industry_profit_report_full_optimized.csv")
