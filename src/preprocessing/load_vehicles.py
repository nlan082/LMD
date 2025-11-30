import pandas as pd
import re
from src.utils.config import (
    VEHICLES_RAW, VEHICLES_CLEAN, DATA_PROCESSED,
    DEPOTS_CLEAN, city_to_region
)

def to_snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"_+", "_", s)
    return s.lower()

def parse_number(x):
    if pd.isna(x):
        return None
    x = str(x).strip().replace(",", ".")
    try:
        return float(x)
    except:
        return None

def extract_leading_number(x):
    if pd.isna(x):
        return None
    m = re.match(r"(\d+(\.\d+)?)", str(x).strip())
    return float(m.group(1)) if m else None

def load_vehicles():
    df = pd.read_excel(VEHICLES_RAW)
    df.columns = [to_snake(c) for c in df.columns]

    df = df.rename(columns={
        "vehicle_id": "vehicle_id",
        "vehicle_type": "vehicle_type",
        "capacity_weight": "capacity_weight",
        "capacity_volume": "capacity_volume",
        "fixed_cost": "fixed_cost",
        "variable_cost": "variable_cost",
        "max_distance": "max_distance_km",
        "max_working_hours": "max_working_hours",
        "start_depot_id": "start_depot_id",
        "end_depot_id": "end_depot_id"
    })

    # số
    for col in ["capacity_weight", "capacity_volume",
                "fixed_cost", "variable_cost", "max_distance_km"]:
        df[col] = df[col].apply(parse_number)

    df["max_working_hours"] = df["max_working_hours"].apply(extract_leading_number)

    # region_id cho vehicle = region của start_depot
    depots = pd.read_csv(DEPOTS_CLEAN)
    depot_region = (depots
                    .set_index("depot_id")["city"]
                    .map(city_to_region)
                    .to_dict())
    df["region_id"] = df["start_depot_id"].map(depot_region)

    df = df.dropna(subset=["vehicle_id", "capacity_weight"])
    df = df[df["capacity_weight"] > 0]

    DATA_PROCESSED.mkdir(exist_ok=True)
    df.to_csv(VEHICLES_CLEAN, index=False)
    print(f"✔ Saved vehicles_clean.csv → {VEHICLES_CLEAN}")


if __name__ == "__main__":
    load_vehicles()
