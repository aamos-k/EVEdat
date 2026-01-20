import sqlite3
import requests
import pandas as pd
from functools import lru_cache

# ----------------------------
# CONFIGURATION
# ----------------------------
DB_PATH = "sqlite-latest.sqlite"
REGION_ID = 10000002     # Jita region
UNITS_TO_COMPARE = 10
ISK_BUDGET = 500_000_000   # 500 million ISK

# NEW: Enable self-sufficiency mode (mining + PI)
ENABLE_SELF_SUFFICIENT = True  # Set to False to use market prices for everything

# Market volume filtering (exclude low-demand items)
MIN_DAILY_VOLUME = 5  # Minimum average daily volume traded (set to 0 to disable filtering)

# Tech level filtering
EXCLUDE_TECH_2 = True  # Set to False to include Tech 2 items

# Blueprint configuration
INCLUDE_BLUEPRINT_COST = True  # Set to False to ignore blueprint costs
BLUEPRINT_RUNS = 10  # Number of runs to amortize blueprint cost over
# Note: Set to a high number (e.g., 1000) for BPOs, lower (e.g., 10) for BPCs

# Mining configuration (Modulated Strip Miner II)
STRIP_MINER_CYCLE_TIME = 180  # seconds per cycle
STRIP_MINER_YIELD = 540       # m3 per cycle (base yield for Modulated Strip Miner II)

# Reprocessing efficiency (adjust based on your setup)
REPROCESSING_EFFICIENCY = 0.72  # 72% (can be higher with skills/implants/station bonuses)

# ----------------------------
# DATABASE CONNECTION
# ----------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ----------------------------
# ITEM NAME LOOKUP
# ----------------------------
def get_item_name(type_id):
    row = cur.execute(
        "SELECT typeName FROM invTypes WHERE typeID = ?",
        (type_id,)
    ).fetchone()
    return row[0] if row else f"Unknown({type_id})"

# ----------------------------
# ITEM VOLUME LOOKUP
# ----------------------------
def get_item_volume(type_id):
    row = cur.execute(
        "SELECT volume FROM invTypes WHERE typeID = ?",
        (type_id,)
    ).fetchone()
    return float(row[0]) if row else 0.0

# ----------------------------
# CHECK IF ITEM IS A MINERAL
# ----------------------------
MINERAL_GROUP_ID = 18  # Mineral group in EVE
ICE_PRODUCT_GROUP_ID = 423  # Ice Products

def is_mineral(type_id):
    row = cur.execute(
        "SELECT groupID FROM invTypes WHERE typeID = ?",
        (type_id,)
    ).fetchone()
    if row:
        return row[0] in [MINERAL_GROUP_ID, ICE_PRODUCT_GROUP_ID]
    return False

# ----------------------------
# CHECK IF ITEM IS PI MATERIAL
# ----------------------------
PI_CATEGORY_ID = 43  # Planetary Commodities category

def is_pi_material(type_id):
    row = cur.execute(
        """SELECT g.categoryID 
           FROM invTypes i 
           JOIN invGroups g ON i.groupID = g.groupID 
           WHERE i.typeID = ?""",
        (type_id,)
    ).fetchone()
    if row:
        return row[0] == PI_CATEGORY_ID
    return False

# ----------------------------
# GET ORE THAT REPROCESSES TO MINERAL
# ----------------------------
def get_ore_for_mineral(mineral_type_id):
    """Find an ore that reprocesses into this mineral"""
    query = """
    SELECT tr.typeID, tr.quantity, t.volume
    FROM invTypeMaterials tr
    JOIN invTypes t ON tr.typeID = t.typeID
    WHERE tr.materialTypeID = ?
    ORDER BY tr.quantity DESC
    LIMIT 1
    """
    row = cur.execute(query, (mineral_type_id,)).fetchone()
    if row:
        ore_id, mineral_yield, ore_volume = row
        return ore_id, int(mineral_yield), float(ore_volume)
    return None, 0, 0.0

# ----------------------------
# CALCULATE MINING TIME
# ----------------------------
def calculate_mining_time(mineral_type_id, quantity_needed):
    """Calculate time to mine ore for required minerals"""
    ore_id, mineral_per_ore, ore_volume = get_ore_for_mineral(mineral_type_id)
    
    if not ore_id:
        return 0.0
    
    # Calculate how much ore we need
    minerals_per_cycle = mineral_per_ore * REPROCESSING_EFFICIENCY
    cycles_needed = quantity_needed / minerals_per_cycle
    
    # Total time in seconds
    total_time_seconds = cycles_needed * STRIP_MINER_CYCLE_TIME
    
    return total_time_seconds / 3600  # Convert to hours

# ----------------------------
# MARKET PRICE CACHE
# ----------------------------
_price_cache = {}
_volume_cache = {}

def get_price(type_id):
    if type_id in _price_cache:
        return _price_cache[type_id]

    url = f"https://market.fuzzwork.co.uk/aggregates/?region={REGION_ID}&types={type_id}"

    try:
        data = requests.get(url, timeout=10).json()
        price = float(data[str(type_id)]["sell"]["min"])
    except:
        price = 0.0

    _price_cache[type_id] = price
    return price

def get_market_volume(type_id):
    """Get average daily market volume for an item"""
    if type_id in _volume_cache:
        return _volume_cache[type_id]
    
    # Use ESI API for volume data instead
    url = f"https://esi.evetech.net/latest/markets/{REGION_ID}/history/?datasource=tranquility&type_id={type_id}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Calculate average volume from last 30 days
                recent_data = data[-30:] if len(data) >= 30 else data
                avg_volume = sum(day.get('volume', 0) for day in recent_data) / len(recent_data)
                _volume_cache[type_id] = avg_volume
                return avg_volume
    except:
        pass
    
    # If API fails, return high volume to not filter out
    _volume_cache[type_id] = 999999
    return 999999

# ----------------------------
# BLUEPRINT OPERATIONS
# ----------------------------
def get_blueprint_type_id(product_type_id):
    """Get the blueprint type ID for a product"""
    query = """
    SELECT DISTINCT typeID
    FROM industryActivityProducts
    WHERE productTypeID = ?
      AND activityID = 1
    LIMIT 1
    """
    row = cur.execute(query, (product_type_id,)).fetchone()
    return int(row[0]) if row else None

def get_blueprint_cost(product_type_id):
    """Calculate the amortized blueprint cost per production run"""
    blueprint_id = get_blueprint_type_id(product_type_id)
    if not blueprint_id:
        return 0.0, 0.0
    
    # Get blueprint price from market
    bp_price = get_price(blueprint_id)
    
    # Amortized cost over number of runs
    amortized_cost = (bp_price / BLUEPRINT_RUNS) if BLUEPRINT_RUNS > 0 and INCLUDE_BLUEPRINT_COST else 0.0
    
    return amortized_cost, bp_price

# ----------------------------
# BLUEPRINT CHECK
# ----------------------------
def has_blueprint(type_id):
    query = """
    SELECT COUNT(*)
    FROM industryActivityProducts p
    JOIN industryActivity a ON p.typeID = a.typeID
    WHERE p.productTypeID = ?
      AND a.activityID = 1
    """
    count = cur.execute(query, (type_id,)).fetchone()[0]
    return count > 0

# ----------------------------
# GET MATERIALS
# ----------------------------
def get_materials(product_type_id):
    query = """
    SELECT m.materialTypeID, m.quantity
    FROM industryActivityMaterials m
    JOIN industryActivityProducts p ON m.typeID = p.typeID
    JOIN industryActivity a ON m.typeID = a.typeID
    WHERE p.productTypeID = ?
      AND a.activityID = 1
      AND m.activityID = 1
    """
    rows = cur.execute(query, (product_type_id,)).fetchall()
    return [(int(mid), int(q)) for mid, q in rows]

# ----------------------------
# SAFE RECURSION WRAPPER
# ----------------------------
visited = set()

def safe_cost_to_build(type_id):
    if type_id in visited:
        return get_price(type_id), 0.0, 0.0, 0.0, 0.0
    
    visited.add(type_id)
    value = _cost_to_build(type_id)
    visited.remove(type_id)
    return value

# ----------------------------
# RECURSIVE COST FUNCTION
# ----------------------------
@lru_cache(maxsize=None)
def _cost_to_build(type_id):
    """Returns (cost, mining_hours, pi_hours, blueprint_cost, full_bp_price)"""
    
    if not has_blueprint(type_id):
        # Base material - check if it's mineral or PI
        if ENABLE_SELF_SUFFICIENT:
            if is_mineral(type_id):
                return (0.0, 0.0, 0.0, 0.0, 0.0)  # Will calculate mining time at usage point
            elif is_pi_material(type_id):
                return (0.0, 0.0, 0.0, 0.0, 0.0)  # PI is free but takes time
        
        return (float(get_price(type_id)), 0.0, 0.0, 0.0, 0.0)
    
    total_cost = 0.0
    total_mining_hours = 0.0
    total_pi_hours = 0.0
    total_bp_cost = 0.0
    total_full_bp_price = 0.0
    
    materials = get_materials(type_id)
    
    for mat_id, qty in materials:
        if ENABLE_SELF_SUFFICIENT:
            if is_mineral(mat_id):
                # Calculate mining time instead of cost
                mining_time = calculate_mining_time(mat_id, qty)
                total_mining_hours += mining_time
                continue
            elif is_pi_material(mat_id):
                # PI materials are free but we could track extraction time
                # For now, we'll estimate based on material tier
                # This is a simplified model - adjust as needed
                total_pi_hours += qty * 0.01  # Rough estimate
                continue
        
        sub_cost, sub_mining, sub_pi, sub_bp, sub_full_bp = safe_cost_to_build(mat_id)
        total_cost += float(sub_cost) * float(qty)
        total_mining_hours += sub_mining * float(qty)
        total_pi_hours += sub_pi * float(qty)
        total_bp_cost += sub_bp * float(qty)
        total_full_bp_price += sub_full_bp * float(qty)
    
    # Add blueprint cost for this item
    amortized_bp, full_bp_price = get_blueprint_cost(type_id)
    total_bp_cost += amortized_bp
    total_full_bp_price += full_bp_price
    
    return (float(total_cost), float(total_mining_hours), float(total_pi_hours), float(total_bp_cost), float(total_full_bp_price))

# ----------------------------
# GET ALL MANUFACTURABLE ITEMS
# ----------------------------
def get_all_products():
    # First, get the market group IDs to exclude
    excluded_market_groups = set()
    
    # Find "Special Edition Ships" and "Faction & Storyline" market groups recursively
    exclude_names = ['Special Edition Ships', 'Faction & Storyline', 'Faction Warfare']
    
    for name in exclude_names:
        try:
            query = """
            WITH RECURSIVE market_tree AS (
              SELECT marketGroupID FROM invMarketGroups WHERE marketGroupName LIKE ?
              UNION ALL
              SELECT mg2.marketGroupID 
              FROM invMarketGroups mg2
              JOIN market_tree mt ON mg2.parentGroupID = mt.marketGroupID
            )
            SELECT marketGroupID FROM market_tree
            """
            rows = cur.execute(query, (f'%{name}%',)).fetchall()
            excluded_market_groups.update(int(r[0]) for r in rows)
        except:
            pass
    
    # Build the exclusion list
    exclusion_clause = ""
    if excluded_market_groups:
        group_list = ','.join(str(g) for g in excluded_market_groups)
        exclusion_clause = f"AND t.marketGroupID NOT IN ({group_list})"
    tech2_filter = ""
    if EXCLUDE_TECH_2:
        tech2_filter = "AND t.typeName NOT LIKE '% II'"
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
      -- Exclude by group name
      AND g.groupName NOT LIKE '%Special Edition%'
      AND g.groupName NOT LIKE '%Faction%'
      AND g.groupName NOT LIKE '%Storyline%'
      AND g.groupName NOT LIKE '%Officer%'
      AND g.groupName NOT LIKE '%Deadspace%'
      AND g.groupName NOT LIKE '%Pirate%'
      -- Exclude by item name patterns
      AND t.typeName NOT LIKE '%Navy%'
      AND t.typeName NOT LIKE '%Fleet%'
      AND t.typeName NOT LIKE '%Shadow%'
      AND t.typeName NOT LIKE '%Caldari Navy%'
      AND t.typeName NOT LIKE '%Federation Navy%'
      AND t.typeName NOT LIKE '%Republic Fleet%'
      AND t.typeName NOT LIKE '%Imperial Navy%'
      AND t.typeName NOT LIKE '%Civilian%'
      AND t.typeName NOT LIKE '%Integrated%'
      AND t.typeName NOT LIKE '%Augmented%'
      AND t.typeName NOT LIKE '%Abyssal%'
      AND t.typeName NOT LIKE '%Mutated%'
      AND t.typeName NOT LIKE '%Mordus%'
      AND t.typeName NOT LIKE '%Thukker%'
      AND t.typeName NOT LIKE '%Sansha%'
      AND t.typeName NOT LIKE '%Blood Raider%'
      AND t.typeName NOT LIKE '%Guristas%'
      AND t.typeName NOT LIKE '%Serpentis%'
      AND t.typeName NOT LIKE '%Angel%'
      AND t.typeName NOT LIKE '%Domination%'
      AND t.typeName NOT LIKE '%Gist%'
      AND t.typeName NOT LIKE '%Estamel%'
      AND t.typeName NOT LIKE '%Vepas%'
      AND t.typeName NOT LIKE '%Thon%'
      AND t.typeName NOT LIKE '%Kaikka%'
      AND t.typeName NOT LIKE '%Setele%'
      AND t.typeName NOT LIKE '%Brynn%'
      AND t.typeName NOT LIKE '%Tairei%'
      AND t.typeName NOT LIKE '%Hakim%'
      AND t.typeName NOT LIKE '%Chelm%'
      AND t.typeName NOT LIKE '%Cormack%'
      AND t.typeName NOT LIKE '%Draclira%'
      AND t.typeName NOT LIKE '%Mizuro%'
      AND t.typeName NOT LIKE '%Khanid%'
      AND t.typeName NOT LIKE '%Ammatar%'
      AND t.typeName NOT LIKE '%ORE%'
      AND t.typeName NOT LIKE '%Concord%'
      AND t.typeName NOT LIKE '%CONCORD%'
      {tech2_filter}
      {exclusion_clause}
    """
    
    rows = cur.execute(query).fetchall()
    return [int(r[0]) for r in rows]

# ----------------------------
# MAIN ANALYSIS
# ----------------------------
def analyze_profits():
    results = []
    
    product_ids = get_all_products()
    
    mode_str = "SELF-SUFFICIENT (Mining + PI)" if ENABLE_SELF_SUFFICIENT else "MARKET PURCHASE"
    print(f"Found {len(product_ids)} manufacturable items... calculating ({mode_str})...\n")
    
    for i, type_id in enumerate(product_ids):
        if i % 100 == 0:
            print(f"Processing {i}/{len(product_ids)}...")
        
        # Check market volume first to skip low-demand items
        volume = get_market_volume(type_id)
        if volume < MIN_DAILY_VOLUME:
            continue
        
        build_cost, mining_hours, pi_hours, bp_cost, full_bp_price = safe_cost_to_build(type_id)
        
        build_cost *= UNITS_TO_COMPARE
        mining_hours *= UNITS_TO_COMPARE
        pi_hours *= UNITS_TO_COMPARE
        bp_cost *= UNITS_TO_COMPARE
        # Don't multiply full_bp_price - it's the one-time purchase cost
        
        if full_bp_price == 0:
            continue
        
        sell_value = get_price(type_id) * UNITS_TO_COMPARE
        total_cost = build_cost + bp_cost
        profit = sell_value - total_cost
        
        result = {
            "type_id": type_id,
            "name": get_item_name(type_id),
            "build_cost": build_cost,
            "blueprint_cost": bp_cost,
            "blueprint_price": full_bp_price,
            "total_cost": total_cost,
            "sell_value": sell_value,
            "profit": profit,
            "daily_volume": volume
        }
        
        if ENABLE_SELF_SUFFICIENT:
            result["mining_hours"] = mining_hours
            result["pi_hours"] = pi_hours
            result["total_hours"] = mining_hours + pi_hours
            result["isk_per_hour"] = profit / result["total_hours"] if result["total_hours"] > 0 else 0
        
        results.append(result)
    
    df = pd.DataFrame(results)
    
    print(f"\nFiltered to {len(df)} items with volume >= {MIN_DAILY_VOLUME}")
    
    if len(df) == 0:
        print(f"WARNING: No items found with volume >= {MIN_DAILY_VOLUME}. Try lowering MIN_DAILY_VOLUME.")
        return df
    
    if ENABLE_SELF_SUFFICIENT:
        df = df.sort_values("isk_per_hour", ascending=False)
    else:
        df = df.sort_values("profit", ascending=False)
    
    return df

# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    df = analyze_profits()
    
    if len(df) == 0:
        print("\nNo items to display. Exiting.")
    else:
        if ENABLE_SELF_SUFFICIENT:
            print("\n=== TOP 20 BY ISK/HOUR (Self-Sufficient Mode) ===\n")
            print(df[["name", "profit", "blueprint_cost", "blueprint_price", "daily_volume", "mining_hours", "pi_hours", "total_hours", "isk_per_hour"]].head(20))
        else:
            print("\n=== TOP 20 PROFITS (Market Purchase Mode) ===\n")
            print(df[["name", "build_cost", "blueprint_cost", "blueprint_price", "total_cost", "sell_value", "profit", "daily_volume"]].head(20))
        
        print("\n=== WORST 20 (LOSS) ===\n")
        print(df.tail(20))
        
        output_file = "industry_profit_report_selfsufficient.csv" if ENABLE_SELF_SUFFICIENT else "industry_profit_report.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved full results to {output_file}")
