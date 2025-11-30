# src/preprocessing/build_road_graph.py

import pandas as pd
from pathlib import Path

from src.utils.config import (
    DATA_RAW,
    CUSTOMERS_CLEAN,
    DEPOTS_CLEAN,
    NODES_MASTER,
    EDGES_MASTER,
    DATA_PROCESSED,
)


# =======================
#  HÀM HỖ TRỢ
# =======================

def parse_number(x):
    """Chuyển về float, xử lý luôn trường hợp có dấu phẩy."""
    if pd.isna(x):
        return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# =======================
#  GIAI ĐOẠN 2: BUILD NODES
# =======================

def build_nodes():
    """
    GIAI ĐOẠN 2 – Biến toàn bộ depot + customer thành một bảng NODE duy nhất.

    - Đọc customers_clean.csv, depots_clean.csv (đã có region_id từ Stage 1)
    - Tạo node_id:
        + depot:   node_id = depot_id
        + customer: node_id = customer_id
    - Thêm cột node_type: 'depot' / 'customer'
    - Giữ lại các thông tin quan trọng: lat, lon, city, region_id
    - Gộp thành nodes_master.csv
    """

    customers = pd.read_csv(CUSTOMERS_CLEAN)
    depots = pd.read_csv(DEPOTS_CLEAN)

    # tạo node cho depot
    depots_nodes = depots.copy()
    depots_nodes["node_id"] = depots_nodes["depot_id"]
    depots_nodes["node_type"] = "depot"

    # tạo node cho customer
    customers_nodes = customers.copy()
    customers_nodes["node_id"] = customers_nodes["customer_id"]
    customers_nodes["node_type"] = "customer"

    base_cols = ["node_id", "node_type", "lat", "lon", "city", "region_id"]

    for df in (depots_nodes, customers_nodes):
        for col in base_cols:
            if col not in df.columns:
                df[col] = None

    nodes = pd.concat(
        [depots_nodes[base_cols], customers_nodes[base_cols]],
        ignore_index=True
    )

    # cảnh báo node_id trùng
    dup_mask = nodes["node_id"].duplicated()
    if dup_mask.any():
        print("⚠ Cảnh báo: có node_id trùng nhau:")
        print(nodes.loc[dup_mask, "node_id"].unique())

    # cảnh báo toạ độ bất thường
    invalid = nodes[
        (nodes["lat"].isna())
        | (nodes["lon"].isna())
        | (nodes["lat"] < -90)
        | (nodes["lat"] > 90)
        | (nodes["lon"] < -180)
        | (nodes["lon"] > 180)
    ]
    if not invalid.empty:
        print(f"⚠ Có {len(invalid)} node có toạ độ bất thường (lat/lon).")

    DATA_PROCESSED.mkdir(exist_ok=True)
    nodes.to_csv(NODES_MASTER, index=False, encoding="utf-8-sig")

    print("✔ GIAI ĐOẠN 2: Build nodes_master.csv DONE")
    print(f"  - Tổng node: {len(nodes)}")
    print(f"  - Region list: {nodes['region_id'].dropna().unique().tolist()}")
    print(f"  → Đã lưu tại: {NODES_MASTER}")


# =======================
#  GIAI ĐOẠN 3: BUILD EDGES (ROADS)
# =======================

def _load_single_roads_file(path: Path, valid_nodes: set) -> pd.DataFrame:
    """
    Đọc một file roads_*.csv, chuẩn hoá cột và làm sạch cơ bản.
    Chỉ giữ những dòng có origin & destination nằm trong tập valid_nodes.
    """
    df = pd.read_csv(path)

    # chuẩn tên cột, bỏ khoảng trắng dư
    df.columns = [c.strip() for c in df.columns]

    # đổi tên cột về chuẩn
    df = df.rename(columns={
        "Origin_Node_ID": "origin_id",
        "Destination_Node_ID": "destination_id",
        "Distance_km": "distance_km",
        "Travel_Time_min": "travel_time_min",
        "Traffic_Level": "traffic_level",
        "Road_Restrictions": "road_restrictions",
    })

    # giữ các cột quan trọng
    keep_cols = [
        "origin_id", "destination_id",
        "distance_km", "travel_time_min",
        "traffic_level", "road_restrictions",
    ]
    df = df[keep_cols]

    # ép kiểu số
    df["distance_km"] = df["distance_km"].apply(parse_number)
    df["travel_time_min"] = df["travel_time_min"].apply(parse_number)

    # bỏ dòng thiếu id hoặc số <= 0
    df = df.dropna(subset=["origin_id", "destination_id",
                           "distance_km", "travel_time_min"])
    df = df[(df["distance_km"] > 0) & (df["travel_time_min"] > 0)]

    # chỉ giữ edge nối giữa các node hợp lệ
    df = df[df["origin_id"].isin(valid_nodes)
            & df["destination_id"].isin(valid_nodes)]

    # fill NA cho text
    df["traffic_level"] = df["traffic_level"].fillna("Unknown")
    df["road_restrictions"] = df["road_restrictions"].fillna("None")

    # thêm cột ghi lại nguồn gốc file (để debug nếu cần)
    df["source_file"] = path.name

    return df


def build_edges():
    """
    GIAI ĐOẠN 3 – Tiền xử lý toàn bộ mạng lưới đường.

    - Duyệt qua tất cả folder roads_* trong data_raw/
    - Đọc từng file .csv, chuẩn hoá cột, làm sạch distance/time
    - Giữ lại chỉ những edge nối giữa các node có trong nodes_master.csv
    - Nếu đường KHÔNG phải One-Way -> tạo thêm edge ngược lại (hai chiều)
    - Loại bỏ edge trùng (origin, destination giống nhau) – giữ edge ngắn nhất
    - Ghi ra edges_master.csv
    """

    # 0. Đọc danh sách node hợp lệ
    nodes = pd.read_csv(NODES_MASTER)
    valid_nodes = set(nodes["node_id"].unique())

    # 1. Tìm tất cả file roads_*.csv trong data_raw/
    road_files = []
    for path in DATA_RAW.iterdir():
        if path.is_dir() and path.name.startswith("roads_"):
            road_files.extend(path.glob("*.csv"))

    if not road_files:
        print("⚠ Không tìm thấy file roads_*.csv trong data_raw/")
        return

    print(f"👉 Tìm thấy {len(road_files)} file roads .csv")

    # 2. Đọc và làm sạch từng file
    all_edges = []
    for fp in road_files:
        df = _load_single_roads_file(fp, valid_nodes)
        if not df.empty:
            all_edges.append(df)
        else:
            print(f"  - File {fp.name}: không có edge hợp lệ, bỏ qua.")

    if not all_edges:
        print("⚠ Không có edge nào sau khi làm sạch.")
        return

    edges = pd.concat(all_edges, ignore_index=True)

    # 3. Tạo thêm edge ngược lại cho đường hai chiều
    #    Quy ước: nếu road_restrictions chứa 'One-Way' (không phân biệt hoa thường)
    #    thì xem là một chiều, ngược lại là hai chiều.
    mask_two_way = ~edges["road_restrictions"].str.contains(
        "one-way", case=False, na=False
    )
    edges_two_way = edges[mask_two_way].copy()
    # đảo origin <-> destination
    edges_two_way = edges_two_way.rename(
        columns={"origin_id": "destination_id",
                 "destination_id": "origin_id"}
    )

    # gộp edge gốc + edge đảo chiều
    edges_full = pd.concat([edges, edges_two_way], ignore_index=True)

    # 4. Loại bỏ trùng: nếu (origin_id, destination_id) trùng nhau
    #    giữ edge có distance_km nhỏ nhất
    edges_full.sort_values(
        by=["origin_id", "destination_id", "distance_km", "travel_time_min"],
        inplace=True,
    )
    edges_full = edges_full.drop_duplicates(
        subset=["origin_id", "destination_id"],
        keep="first",
    )

    # 5. Lưu edges_master.csv
    DATA_PROCESSED.mkdir(exist_ok=True)
    edges_full.to_csv(EDGES_MASTER, index=False)

    print("✔ GIAI ĐOẠN 3: Build edges_master.csv DONE")
    print(f"  - Số edges (sau khi nhân hai chiều + loại trùng): {len(edges_full)}")
    print(f"  → Đã lưu tại: {EDGES_MASTER}")


if __name__ == "__main__":
    # Cho phép chạy riêng file này để test
    build_nodes()
    build_edges()
