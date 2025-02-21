"""
This module contains the ProblemSolver class which is used to solve hiking route problems.

Classes
ProblemSolver
"""
from pulp import LpVariable, LpProblem, LpBinary, LpContinuous, LpMinimize, lpSum, listSolvers, getSolver, PULP_CBC_CMD, LpStatus
from typing import Optional, Set
from model.graph_data import GraphData
from model.solution import Solution


class ProblemSolver:
    """
    A class to represent a problem solver for hiking routes.
    """

    def __init__(self, data: GraphData):
        """
        Constructs all the necessary attributes for the ProblemSolver object.
        """
        self.data = data

    def solve(self,
              days: int,
              maximum_daily_distance: float,
              min_stamps: int,
              home_address: str,
              max_bus_days: int = 0,
              max_parking_days: int = 0,
              ignore_stamp_ids: Set[int] = set(),
              prioritized_solver_str: Optional[str] = None) -> Solution:
        """
        Solves the hiking route problem with the given constraints.
        """
        home_stamp_distances = self.data.get_home_stamp_distances(home_address)
        home_start = self.data.get_home_node(home_address, "home_start")
        home_end = self.data.get_home_node(home_address, "home_end")
        def get_node(node_id):
            if node_id == "home_start":
                return home_start
            if node_id == "home_end":
                return home_end
            return self.data.node(node_id)

        ignore_stamp_point_ids = set()
        for stamp_point_id in self.data.stamp_points:
            if self.data.node(stamp_point_id).stamp_id in ignore_stamp_ids:
                ignore_stamp_point_ids.add(stamp_point_id)

        # Is the node (origin, bus_stop, parking_lot, stamp_point) visited on day?
        x = {}
        # Position of the stamp_point on the tour.
        z = {}
        for day in range(days):
            x[day, home_start.neo4j_id] = LpVariable(f"x[{day},{home_start.neo4j_id}]", cat=LpBinary)
            x[day, home_end.neo4j_id] = LpVariable(f"x[{day},{home_end.neo4j_id}]", cat=LpBinary)
            for stamp_point_id in self.data.stamp_points:
                if stamp_point_id not in ignore_stamp_point_ids:
                    x[day, stamp_point_id] = LpVariable(f"x[{day},{stamp_point_id}]", cat=LpBinary)
                    z[day, stamp_point_id] = LpVariable(f"z[{day},{stamp_point_id}]", 0, len(self.data.stamp_points), LpContinuous)
            for bus_stop_id in self.data.bus_stops:
                x[day, bus_stop_id] = LpVariable(f"x[{day},{bus_stop_id}]", cat=LpBinary)
            for parking_lot_id in self.data.parking_lots:
                x[day, parking_lot_id] = LpVariable(f"x[{day},{parking_lot_id}]", cat=LpBinary)
        # Is the arc (from_id, to_id) used on day?
        y = {}
        for day in range(days):
            for from_id in self.data.distances:
                for to_id in self.data.distances[from_id]:
                    if from_id not in ignore_stamp_point_ids and to_id not in ignore_stamp_point_ids:
                        y[day, from_id, to_id] = LpVariable(f"y[{day},{from_id},{to_id}]", cat=LpBinary)
            for stamp_point_id in home_stamp_distances:
                if stamp_point_id not in ignore_stamp_point_ids:
                    y[day, home_start.neo4j_id, stamp_point_id] = LpVariable(f"y[{day},{home_start.neo4j_id},{stamp_point_id}]", cat=LpBinary)
                    y[day, stamp_point_id, home_end.neo4j_id] = LpVariable(f"y[{day},{stamp_point_id},{home_end.neo4j_id}]", cat=LpBinary)
        # auxiliary variable for daily distance
        d = []
        for day in range(days):
            d.append(LpVariable(f"d[{day}]", 0, maximum_daily_distance, LpContinuous))

        # Create the model
        prob = LpProblem("Harz-Hiking", LpMinimize)

        # objective function
        prob += lpSum(d[day] for day in range(days))
        
        # daily distance (includes maximum from ub of d[day])
        for day in range(days):
            prob += lpSum(self.data.distances[from_id][to_id] * y[day, from_id, to_id]
                        for from_id in self.data.distances if from_id not in ignore_stamp_point_ids
                        for to_id in self.data.distances[from_id] if to_id not in ignore_stamp_point_ids) + \
                    lpSum(home_stamp_distances[stamp_point_id] * y[day, home_start.neo4j_id, stamp_point_id]
                        for stamp_point_id in home_stamp_distances if stamp_point_id not in ignore_stamp_point_ids) + \
                    lpSum(home_stamp_distances[stamp_point_id] * y[day, stamp_point_id, home_end.neo4j_id]
                        for stamp_point_id in home_stamp_distances if stamp_point_id not in ignore_stamp_point_ids) == d[day]

        # visit exactly one starting node each day (bus, parking or home)
        for day in range(days):
            prob += lpSum(x[day, bus_stop] for bus_stop in self.data.bus_stops) + \
                    lpSum(x[day, parking_lot] for parking_lot in self.data.parking_lots) + \
                    x[day, home_start.neo4j_id] == 1
            
        # visit each stamp_point at most once
        for stamp_point_id in self.data.stamp_points:
            if stamp_point_id not in ignore_stamp_point_ids:
                prob += lpSum(x[day, stamp_point_id] for day in range(days)) <= 1
            
        # link arcs to nodes (flow conservation)
        for day in range(days):
            for from_id in self.data.distances:
                # only if there are outgoing arcs (every node except if no other point is within max_section_length_m)
                if len(self.data.distances[from_id]) > 0 and from_id not in ignore_stamp_point_ids:
                    prob += lpSum(y[day, from_id, to_id] for to_id in self.data.distances[from_id] if to_id not in ignore_stamp_point_ids) + \
                         (y[day, from_id, home_end.neo4j_id] if from_id in home_stamp_distances else 0) == x[day, from_id]
            for to_id in self.data.distances_reverse:
                # only if there are incoming arcs (parking lots and stamp points except if no other point is within max_section_length_)
                if len(self.data.distances_reverse[to_id]) > 0 and to_id not in ignore_stamp_point_ids:
                    prob += lpSum(y[day, from_id, to_id] for from_id in self.data.distances_reverse[to_id] if from_id not in ignore_stamp_point_ids) + \
                        (y[day, home_start.neo4j_id, to_id] if to_id in home_stamp_distances else 0) == x[day, to_id]
            # home_start and home_end
            prob += lpSum(y[day, from_id, home_end.neo4j_id] for from_id in home_stamp_distances if from_id not in ignore_stamp_point_ids) == x[day, home_end.neo4j_id]
            prob += lpSum(y[day, home_start.neo4j_id, to_id] for to_id in home_stamp_distances if to_id not in ignore_stamp_point_ids) == x[day, home_start.neo4j_id]

        # no subtour consisting of stamp_points
        for day in range(days):
            for from_id in self.data.stamp_points:
                if from_id not in ignore_stamp_point_ids:
                    for to_id in self.data.stamp_points:
                        if to_id not in ignore_stamp_point_ids:
                            if from_id in self.data.distances and to_id in self.data.distances[from_id]:
                                prob += z[day, from_id] + 1 <= z[day, to_id] + len(self.data.stamp_points) * (1 - y[day, from_id, to_id])

        # visit at least min_stamps stamp_points
        prob += lpSum(x[day, stamp_point_id] for day in range(days)
                      for stamp_point_id in self.data.stamp_points if stamp_point_id not in ignore_stamp_point_ids) >= min_stamps
            
        # maximum number of bus days
        prob += lpSum(x[day, bus_stop] for day in range(days) for bus_stop in self.data.bus_stops) <= max_bus_days

        # maximum number of parking days
        prob += lpSum(x[day, parking_lot] for day in range(days) for parking_lot in self.data.parking_lots) <= max_parking_days

        # solve the model
        # check for solvers
        selected_solver = PULP_CBC_CMD(msg=0)
        if prioritized_solver_str is not None and prioritized_solver_str in listSolvers():
            prioritized_solver = getSolver(prioritized_solver_str, msg=0)
            if prioritized_solver.available:
                selected_solver = prioritized_solver
        status = prob.solve(selected_solver)
        if LpStatus[status] == "Infeasible":
            raise ValueError("Problem configuration is infeasible.")
        elif status != 1:
            raise RuntimeError(f"An unexpected error occured while trying to solve the problem. ({LpStatus[status]})")

        # create the solution
        tours = []
        for day in range(days):
            # build prev dict for each day
            next_dict = {}
            for from_id in self.data.distances:
                if from_id not in ignore_stamp_point_ids:
                    for to_id in self.data.distances[from_id]:
                        if to_id not in ignore_stamp_point_ids:
                            if y[day, from_id, to_id].value() > 0.5:
                                next_dict[from_id] = to_id
            for stamp_id in home_stamp_distances:
                if stamp_id not in ignore_stamp_point_ids:
                    if y[day, home_start.neo4j_id, stamp_id].value() > 0.5:
                        next_dict[home_start.neo4j_id] = stamp_id
                    if y[day, stamp_id, home_end.neo4j_id].value() > 0.5:
                        next_dict[stamp_id] = home_end.neo4j_id

            # find the destination node (not a stamp_point)
            start_id = None
            for next_id in next_dict:
                if next_id not in self.data.stamp_points:
                    start_id = next_id
                    break
            if start_id is None:
                raise RuntimeError("No start node found")
            # build the tour
            tour = []
            next_id = start_id
            while next_id is not None:
                tour.append(get_node(next_id))
                next_id = next_dict.get(next_id)
                if next_id == start_id:
                    tour.append(get_node(next_id))
                    break
            tours.append(tour)
        return Solution(tours)
