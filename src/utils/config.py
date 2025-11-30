from pathlib import Path

# ROOT PROJECT (folder LMD)
ROOT = Path(__file__).resolve().parents[2]

# --------- THƯ MỤC DỮ LIỆU ---------
DATA_RAW = ROOT / "data_raw"
DATA_PROCESSED = ROOT / "data_processed"

# --------- FILE RAW (GIAI ĐOẠN 1) ---------
CUSTOMERS_RAW = DATA_RAW / "customers_vietnam.xlsx"
DEPOTS_RAW = DATA_RAW / "depots_vietnam.xlsx"
VEHICLES_RAW = DATA_RAW / "vehicles_vietnam.xlsx"

# --------- FILE CLEAN (GIAI ĐOẠN 1) ---------
CUSTOMERS_CLEAN = DATA_PROCESSED / "customers_clean.csv"
DEPOTS_CLEAN = DATA_PROCESSED / "depots_clean.csv"
VEHICLES_CLEAN = DATA_PROCESSED / "vehicles_clean.csv"

# --------- FILE GIAI ĐOẠN 2+ ---------
NODES_MASTER = DATA_PROCESSED / "nodes_master.csv"
EDGES_MASTER = DATA_PROCESSED / "edges_master.csv"  

# --------- REGION CONFIG ---------
# Map tên city -> Region_ID (tuỳ ý chỉnh)
# -------- REGION CONFIG --------
# Gom các city nhỏ vào 4 region lớn: HCM, HAN, DAN, CTO
REGION_MAP = {
    # Miền Nam – dùng roads HCM
    "Ho Chi Minh City": "HCM",
    "Bien Hoa": "HCM",
    "Vung Tau": "HCM",

    # Miền Bắc – dùng roads HAN
    "Hanoi": "HAN",
    "Hai Phong": "HAN",

    # Miền Trung – dùng roads DAN
    "Da Nang": "DAN",
    "Hue": "DAN",
    "Nha Trang": "DAN",

    # Tây Nam Bộ / Tây Nguyên – dùng roads CTO
    "Can Tho": "CTO",
    "Buon Ma Thuot": "CTO",
}


def city_to_region(city: str):
    """
    Chuẩn hoá tên city thành Region_ID (HCM, HAN, ...).
    Nếu city không nằm trong REGION_MAP, sẽ fallback: viết hoa & thay khoảng trắng bằng '_'.
    """
    if not isinstance(city, str):
        return None
    city = city.strip()
    if city == "":
        return None

    if city in REGION_MAP:
        return REGION_MAP[city]
    # fallback: ví dụ 'Vung Tau' -> 'VUNG_TAU'
    return city.upper().replace(" ", "_")
# -------- SAMPLING CONFIG (giới hạn số khách mỗi region) --------
# Nếu = None  -> dùng toàn bộ khách
# Nếu là số   -> mỗi region chỉ giữ tối đa bấy nhiêu khách (sample ngẫu nhiên có kiểm soát)
MAX_CUSTOMERS_PER_REGION = 2000
RANDOM_SEED = 42
