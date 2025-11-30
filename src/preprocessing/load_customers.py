import pandas as pd
import re
from src.utils.config import (
    CUSTOMERS_RAW,
    CUSTOMERS_CLEAN,
    DATA_PROCESSED,
    city_to_region,
    MAX_CUSTOMERS_PER_REGION,
    RANDOM_SEED,
)

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

def load_customers():
    df = pd.read_excel(CUSTOMERS_RAW)
    df.columns = [to_snake(c) for c in df.columns]

    df = df.rename(columns={
        "customer_id": "customer_id",
        "latitude": "lat",
        "longitude": "lon",
        "city": "city",
        "order_weight": "demand_weight",
        "order_volume": "demand_volume",
        "time_window_start": "ready_time",
        "time_window_end": "due_time",
        "service_time": "service_time",
        "priority_level": "priority",
        "delivery_type": "delivery_type",
        "return_flag": "is_return"
    })

    # số
    for col in ["demand_weight", "demand_volume", "service_time"]:
        df[col] = df[col].apply(parse_number)

    # thời gian → phút
    df["ready_time_min"] = df["ready_time"].apply(hhmm_to_min)
    df["due_time_min"] = df["due_time"].apply(hhmm_to_min)

    # region
    df["region_id"] = df["city"].apply(city_to_region)

    # Return TRUE/FALSE → 0/1
    df["is_return"] = df["is_return"].map(
        {True: 1, False: 0, "TRUE": 1, "FALSE": 0}
    ).fillna(0).astype(int)

    # loại khách thiếu info tối thiểu
    df = df.dropna(subset=["customer_id", "lat", "lon", "demand_weight"])
    df = df[df["demand_weight"] > 0]

    DATA_PROCESSED.mkdir(exist_ok=True)
        # ... đoạn code clean dữ liệu, gán region_id ... ở trên ...

    # ===============================
    #  GIỚI HẠN SỐ KHÁCH MỖI REGION
    # ===============================
    if MAX_CUSTOMERS_PER_REGION is not None:
        if "region_id" not in df.columns:
            print("⚠ Không có cột region_id, bỏ qua bước sample theo region.")
        else:
            def _sample_region(group):
                # group: tất cả khách trong 1 region
                if len(group) <= MAX_CUSTOMERS_PER_REGION:
                    return group
                return group.sample(
                    n=MAX_CUSTOMERS_PER_REGION,
                    random_state=RANDOM_SEED
                )

            before = len(df)
            df = df.groupby("region_id", group_keys=False).apply(_sample_region)
            after = len(df)
            print(
                f"✔ Áp dụng giới hạn {MAX_CUSTOMERS_PER_REGION} khách/region: "
                f"{before} → {after} customers"
            )

    # ===============================
    #  Lưu file sạch
    # ===============================
    DATA_PROCESSED.mkdir(exist_ok=True)
    df.to_csv(CUSTOMERS_CLEAN, index=False, encoding="utf-8-sig")
    print(f"✓ Saved customers_clean.csv → {CUSTOMERS_CLEAN}")

    df.to_csv(CUSTOMERS_CLEAN, index=False)
    print(f"✔ Saved customers_clean.csv → {CUSTOMERS_CLEAN}")


if __name__ == "__main__":
    load_customers()
