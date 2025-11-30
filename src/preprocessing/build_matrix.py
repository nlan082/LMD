# src/preprocessing/build_matrix.py

import pandas as pd
import heapq
from math import inf

from src.utils.config import (
    NODES_MASTER,
    EDGES_MASTER,
    DATA_PROCESSED,
    VEHICLES_CLEAN,
)

# ============================================================
#  GIAI ĐOẠN 4 – BUILD GRAPH THEO REGION (edges_{region}.csv)
# ============================================================

def build_graphs_by_region():
    """
    GIAI ĐOẠN 4 – BUILD GRAPH THEO REGION

    - Đọc nodes_master.csv để lấy danh sách node + region_id.
    - Đọc edges_master.csv (mạng đường toàn quốc).
    - Với mỗi region:
        + Lấy danh sách node thuộc region đó.
        + Lọc edges sao cho origin & destination đều thuộc tập node này.
        + Ghi ra edges_{region}.csv để dùng cho Dijkstra & ma trận ở Giai đoạn 5.
    """

    nodes = pd.read_csv(NODES_MASTER)
    edges = pd.read_csv(EDGES_MASTER)

    regions = nodes["region_id"].dropna().unique().tolist()
    print(f"✔ GIAI ĐOẠN 4 – Tìm thấy các region: {regions}")

    if not regions:
        print("⚠ Không có region_id trong nodes_master.csv, kiểm tra lại Stage 1–2.")
        return

    summary = []
    for region in regions:
        print(f"\n=== Region: {region} ===")

        # node thuộc region này
        nodes_region = nodes[nodes["region_id"] == region]["node_id"].tolist()
        num_nodes = len(nodes_region)
        print(f"  - Số node trong region: {num_nodes}")

        if num_nodes == 0:
            print("  ⚠ Không có node, bỏ qua region này.")
            continue

        # edges kết nối các node trong region
        edges_region = edges[
            edges["origin_id"].isin(nodes_region)
            & edges["destination_id"].isin(nodes_region)
        ].copy()

        num_edges = len(edges_region)
        if num_edges == 0:
            print("  ⚠ Không có edge nối các node trong region, kiểm tra lại dữ liệu roads.")
        else:
            out_path = DATA_PROCESSED / f"edges_{region}.csv"
            edges_region.to_csv(out_path, index=False)
            print(f"  - Số edges: {num_edges}")
            print(f"  → Đã lưu edges cho region tại: {out_path}")

        summary.append((region, num_nodes, num_edges))

    print("\n===== TỔNG KẾT GIAI ĐOẠN 4 – BUILD GRAPH THEO REGION =====")
    for region, n_nodes, n_edges in summary:
        print(f"Region {region}: {n_nodes} nodes, {n_edges} edges")


# ============================================================
#  GIAI ĐOẠN 5 – DIJKSTRA & MA TRẬN (BẢN TỐI ƯU)
# ============================================================

def _dijkstra_source_to_all(adjacency: dict, source: str) -> dict:
    """
    Dijkstra từ 1 source đến tất cả node trong graph (adjacency).
    adjacency: dict[node] = list[(neighbor, weight)]
    Trả về: dict[node] = distance (float hoặc inf)
    """
    dist = {n: inf for n in adjacency.keys()}
    dist[source] = 0.0
    pq = [(0.0, source)]

    while pq:
        d, u = heapq.heappop(pq)
        if d > dist[u]:
            continue
        for v, w in adjacency.get(u, []):
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                heapq.heappush(pq, (nd, v))
    return dist


def build_matrices_by_region():
    """
    GIAI ĐOẠN 5 (BẢN TỐI ƯU)
    - Chỉ tính ma trận cho DEPOT + CUSTOMER (active nodes).
    - Graph vẫn dùng tất cả node/edge trong region, nhưng chỉ lưu
      khoảng cách giữa các node thực sự tham gia bài toán (depot+customer).
    """

    nodes = pd.read_csv(NODES_MASTER)
    regions = nodes["region_id"].dropna().unique().tolist()
    print(f"✔ GIAI ĐOẠN 5 (tối ưu) – regions: {regions}")

    for region in regions:
        print(f"\n=== REGION {region} ===")

        # 1. Node trong region
        nodes_region_df = nodes[nodes["region_id"] == region].copy()
        if nodes_region_df.empty:
            print("  ⚠ Không có node, skip.")
            continue

        # 2. Active nodes = depot + customer
        if "node_type" in nodes_region_df.columns:
            active_df = nodes_region_df[
                nodes_region_df["node_type"].isin(["depot", "customer"])
            ].copy()
        else:
            # fallback: nếu thiếu cột node_type thì xem mọi node đều là active
            active_df = nodes_region_df.copy()

        active_nodes = active_df["node_id"].tolist()
        if len(active_nodes) == 0:
            print("  ⚠ Region không có depot/customer, skip.")
            continue

        print(f"  - Tổng node trong region: {len(nodes_region_df)}")
        print(f"  - Active nodes (depot + customer): {len(active_nodes)}")

        # 3. Đọc edges_{region}.csv để build graph
        edges_path = DATA_PROCESSED / f"edges_{region}.csv"
        if not edges_path.exists():
            print(f"  ⚠ Không có {edges_path}, skip.")
            continue

        edges_region = pd.read_csv(edges_path)
        if edges_region.empty:
            print("  ⚠ edges rỗng, skip.")
            continue

        # 4. Build adjacency list cho distance & time
        all_nodes_in_edges = set(edges_region["origin_id"]).union(
            set(edges_region["destination_id"])
        )
        adjacency_dist = {n: [] for n in all_nodes_in_edges}
        adjacency_time = {n: [] for n in all_nodes_in_edges}

        for _, row in edges_region.iterrows():
            u = row["origin_id"]
            v = row["destination_id"]
            d = row.get("distance_km", None)
            t = row.get("travel_time_min", None)

            if pd.isna(d) or pd.isna(t):
                continue

            # đảm bảo node nằm trong graph
            if u not in adjacency_dist or v not in adjacency_dist:
                continue

            adjacency_dist[u].append((v, float(d)))
            adjacency_time[u].append((v, float(t)))

        # 5. Chuẩn bị ma trận cho active_nodes
        dist_matrix = pd.DataFrame(
            index=active_nodes, columns=active_nodes, dtype=float
        )
        time_matrix = pd.DataFrame(
            index=active_nodes, columns=active_nodes, dtype=float
        )

        # 6. Chạy Dijkstra CHỈ TỪ active_nodes
        total_sources = len(active_nodes)
        for idx, src in enumerate(active_nodes, start=1):
            if idx % 50 == 1 or idx == total_sources:
                print(f"  · Dijkstra {idx}/{total_sources} từ node {src}")

            dist_res = _dijkstra_source_to_all(adjacency_dist, src)
            time_res = _dijkstra_source_to_all(adjacency_time, src)

            for dst in active_nodes:
                d_val = dist_res.get(dst, inf)
                t_val = time_res.get(dst, inf)
                dist_matrix.loc[src, dst] = None if d_val == inf else d_val
                time_matrix.loc[src, dst] = None if t_val == inf else t_val

        # 7. Lưu ma trận theo region
        dist_path = DATA_PROCESSED / f"distance_matrix_{region}.csv"
        time_path = DATA_PROCESSED / f"time_matrix_{region}.csv"

        dist_matrix.to_csv(dist_path)
        time_matrix.to_csv(time_path)

        print(f"  → Saved distance_matrix_{region}.csv tại {dist_path}")
        print(f"  → Saved time_matrix_{region}.csv tại {time_path}")

    print("\n===== HOÀN TẤT GIAI ĐOẠN 5 (TỐI ƯU) =====")


# ============================================================
#  GIAI ĐOẠN 6 – EXPORT GA-READY DATA
# ============================================================

def export_ga_ready_data():
    """
    GIAI ĐOẠN 6 – ĐÓNG GÓI DỮ LIỆU CHO GA

    Đầu vào:
        - nodes_master.csv
        - vehicles_clean.csv
        - edges_{region}.csv
        - distance_matrix_{region}.csv
        - time_matrix_{region}.csv

    Đầu ra cho mỗi region (có ma trận):
        - nodes_final_{region}.csv
            + node_index (0..N-1)
            + node_id
            + node_type
            + lat, lon, city, region_id
        - edges_final_{region}.csv
            + origin_index, destination_index
            + origin_id, destination_id
            + distance_km, travel_time_min
            + traffic_level, road_restrictions
        - vehicles_{region}.csv
            + các xe có region_id = region
        - giữ nguyên:
            + distance_matrix_{region}.csv
            + time_matrix_{region}.csv
    """

    nodes = pd.read_csv(NODES_MASTER)
    vehicles = pd.read_csv(VEHICLES_CLEAN)

    regions = nodes["region_id"].dropna().unique().tolist()
    print(f"✔ GIAI ĐOẠN 6 – Regions: {regions}")

    for region in regions:
        print(f"\n=== REGION {region} (GA-READY) ===")

        dist_path = DATA_PROCESSED / f"distance_matrix_{region}.csv"
        time_path = DATA_PROCESSED / f"time_matrix_{region}.csv"
        edges_path = DATA_PROCESSED / f"edges_{region}.csv"

        if not dist_path.exists() or not time_path.exists():
            print("  ⚠ Không thấy distance/time matrix, region này chưa được tính Stage 5. Skip.")
            continue

        if not edges_path.exists():
            print(f"  ⚠ Không có edges_{region}.csv, skip.")
            continue

        # 1. Đọc ma trận để lấy THỨ TỰ node dùng cho GA
        dist_mat = pd.read_csv(dist_path, index_col=0)
        node_ids_order = list(dist_mat.index)  # thứ tự node trong matrix

        # 2. Mapping node_id -> node_index
        id_to_index = {nid: idx for idx, nid in enumerate(node_ids_order)}

        # 3. nodes_final_{region}.csv
        nodes_region = nodes[nodes["node_id"].isin(node_ids_order)].copy()
        nodes_region["node_index"] = nodes_region["node_id"].map(id_to_index)
        nodes_region = nodes_region.sort_values("node_index")

        node_cols = [
            "node_index",
            "node_id",
            "node_type",
            "lat",
            "lon",
            "city",
            "region_id",
        ]
        for col in node_cols:
            if col not in nodes_region.columns:
                nodes_region[col] = None

        nodes_final_path = DATA_PROCESSED / f"nodes_final_{region}.csv"
        nodes_region[node_cols].to_csv(nodes_final_path, index=False)
        print(f"  → nodes_final_{region}.csv: {nodes_final_path}")

        # 4. edges_final_{region}.csv
        edges_region = pd.read_csv(edges_path)
        edges_region["origin_index"] = edges_region["origin_id"].map(id_to_index)
        edges_region["destination_index"] = edges_region["destination_id"].map(id_to_index)

        edges_region = edges_region.dropna(subset=["origin_index", "destination_index"])
        edges_region["origin_index"] = edges_region["origin_index"].astype(int)
        edges_region["destination_index"] = edges_region["destination_index"].astype(int)

        edges_final_cols = [
            "origin_index",
            "destination_index",
            "origin_id",
            "destination_id",
            "distance_km",
            "travel_time_min",
            "traffic_level",
            "road_restrictions",
        ]
        for col in edges_final_cols:
            if col not in edges_region.columns:
                edges_region[col] = None

        edges_final_path = DATA_PROCESSED / f"edges_final_{region}.csv"
        edges_region[edges_final_cols].to_csv(edges_final_path, index=False)
        print(f"  → edges_final_{region}.csv: {edges_final_path}")

        # 5. vehicles_{region}.csv
        if "region_id" in vehicles.columns:
            vehicles_region = vehicles[vehicles["region_id"] == region].copy()
        else:
            vehicles_region = vehicles.copy()
            vehicles_region["region_id"] = None

        vehicles_path = DATA_PROCESSED / f"vehicles_{region}.csv"
        vehicles_region.to_csv(vehicles_path, index=False)
        print(f"  → vehicles_{region}.csv: {vehicles_path}")

    print("\n===== HOÀN TẤT GIAI ĐOẠN 6 – EXPORT GA-READY DATA =====")


# ============================================================
#  CHO PHÉP CHẠY TRỰC TIẾP FILE NÀY ĐỂ TEST
# ============================================================

if __name__ == "__main__":
    print(">>> RUN: Giai đoạn 4 + 5 + 6 (test)")
    build_graphs_by_region()
    build_matrices_by_region()
    export_ga_ready_data()
