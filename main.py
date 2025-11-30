from src.preprocessing.load_customers import load_customers
from src.preprocessing.load_depots import load_depots
from src.preprocessing.load_vehicles import load_vehicles
from src.preprocessing.build_road_graph import build_nodes, build_edges
from src.preprocessing.build_matrix import (
    build_graphs_by_region,
    build_matrices_by_region,
    export_ga_ready_data,
)


def run_stage1():
    print("=== GIAI ĐOẠN 1: Clean raw data + gán Region_ID ===")
    load_customers()
    load_depots()
    load_vehicles()
    print("=== HOÀN TẤT GIAI ĐOẠN 1 ===\n")


def run_stage2():
    print("=== GIAI ĐOẠN 2: Build nodes_master (customers + depots) ===")
    build_nodes()
    print("=== HOÀN TẤT GIAI ĐOẠN 2 ===\n")


def run_stage3():
    print("=== GIAI ĐOẠN 3: Build edges_master từ tất cả roads_* ===")
    build_edges()
    print("=== HOÀN TẤT GIAI ĐOẠN 3 ===\n")


def run_stage4():
    print("=== GIAI ĐOẠN 4: Build graphs theo từng region (edges_{region}) ===")
    build_graphs_by_region()
    print("=== HOÀN TẤT GIAI ĐOẠN 4 ===\n")


def run_stage5():
    print("=== GIAI ĐOẠN 5: Dijkstra + Matrix theo từng region (tối ưu) ===")
    build_matrices_by_region()
    print("=== HOÀN TẤT GIAI ĐOẠN 5 ===\n")


def run_stage6():
    print("=== GIAI ĐOẠN 6: Export GA-ready data ===")
    export_ga_ready_data()
    print("=== HOÀN TẤT GIAI ĐOẠN 6 ===\n")


if __name__ == "__main__":
    run_stage6()
