"""
This script imports hiking data from a GPX file into a Neo4j database,
solves a problem using the imported data, and outputs the solution.
"""

from model.graph_data import GraphData
from model.problem_solver import ProblemSolver


# import data
graph_data = GraphData(
    neo4j_uri = "bolt://localhost:7687",
    neo4j_user = "neo4j",
    neo4j_password = "12345678",
    neo4j_database = "neo4j")
graph_data.import_data(
    stamp_point_gpx_filename = "HWN2024.gpx",
    max_section_length_m = 5000,
    force_update = False,
    log = False)

# solve problem
solver = ProblemSolver(graph_data)
solution = solver.solve(
    days = 3,
    maximum_daily_distance = 15000,
    min_stamps = 6,
    home_address = "Torfhaus 1, 38667 Torfhaus",
    max_bus_days = 1,
    max_parking_days = 1,
    ignore_stamp_ids = {137,138},
    prioritized_solver_str = "GUROBI") # e.g. GLPK_CMD, CPLEX_CMD, GUROBI, PULP_CBC_CMD

# output solution
print(solution)
solution.visualize_html("map.html")
