import pandas as pd
import re
from src.utils.config import DEPOTS_RAW, DEPOTS_CLEAN, DATA_PROCESSED, city_to_region

def to_snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"_+", "_", s)
    return s.lower()

def parse_number(x):
    try:
        return float(str(x).strip().replace(",", "."))
    except:
        return None

def hhmm_to_min(x):
    if pd.isna(x):
        return None
    s = str(x)
    m = re.match(r"(\d{1,2}):(\d{2})", s)
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))

def load_depots():
    # 1. đọc file
    df = pd.read_excel(DEPOTS_RAW)
    df.columns = [to_snake(c) for c in df.columns]

    # 2. chuẩn tên cột
    df = df.rename(columns={
        "depot_id": "depot_id",
        "city": "city",
        "latitude": "lat",
        "longitude": "lon",
        "capacity_storage": "capacity_storage",
        "operating_hours": "operating_hours",
    })

    # 3. ép kiểu số
    df["lat"] = df["lat"].apply(parse_number)
    df["lon"] = df["lon"].apply(parse_number)
    df["capacity_storage"] = df["capacity_storage"].apply(parse_number)

    # 4. tạo region_id từ city
    df["region_id"] = df["city"].apply(city_to_region)

    # 5. tách operating_hours "06:00-22:00" → open / close (phút)
    open_list, close_list = [], []
    for val in df["operating_hours"]:
        if isinstance(val, str) and "-" in val:
            o, c = val.split("-")
            open_list.append(hhmm_to_min(o))
            close_list.append(hhmm_to_min(c))
        else:
            open_list.append(None)
            close_list.append(None)

    df["open_time_min"] = open_list
    df["close_time_min"] = close_list

    # 6. loại depot thiếu toạ độ
    df = df.dropna(subset=["depot_id", "lat", "lon"])

    DATA_PROCESSED.mkdir(exist_ok=True)
    df.to_csv(DEPOTS_CLEAN, index=False)
    print(f"✔ Saved depots_clean.csv → {DEPOTS_CLEAN}")


if __name__ == "__main__":
    load_depots()
