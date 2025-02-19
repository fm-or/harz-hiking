"""
This module contains the ProblemSolver class which is used to solve hiking route problems.

Classes
ProblemSolver
"""
from pulp import LpVariable, LpProblem, LpBinary, LpContinuous, LpMinimize, lpSum, listSolvers, getSolver, PULP_CBC_CMD
from typing import Optional
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
              max_bus_days: int = 0,
              max_parking_days: int = 0,
              prioritized_solver_str: Optional[str] = None) -> Solution:
        """
        Solves the hiking route problem with the given constraints.
        """
        # Is the node (origin, bus_stop, parking_lot, stamp_point) visited on day?
        x = {}
        # Position of the stamp_point on the tour.
        z = {}
        for day in range(days):
            #TODO: Implement origin
            for stamp_point in self.data.stamp_points:
                x[day, stamp_point] = LpVariable(f"x[{day},{stamp_point}]", 0, 1, LpBinary)
                z[day, stamp_point] = LpVariable(f"z[{day},{stamp_point}]", 0, len(self.data.stamp_points), LpContinuous)
            for bus_stop in self.data.bus_stops:
                x[day, bus_stop] = LpVariable(f"x[{day},{bus_stop}]", 0, 1, LpBinary)
            for parking_lot in self.data.parking_lots:
                x[day, parking_lot] = LpVariable(f"x[{day},{parking_lot}]", 0, 1, LpBinary)
        y = {}
        for day in range(days):
            for from_id in self.data.distances:
                for to_id in self.data.distances[from_id]:
                    y[day, from_id, to_id] = LpVariable(f"y[{day},{from_id},{to_id}]", 0, 1, LpBinary)

        # Create the model
        prob = LpProblem("Harz-Hiking", LpMinimize)

        # objective function
        from_id = list(self.data.distances.keys())[0]
        to_id = list(self.data.distances[from_id].keys())[0]
        prob += lpSum(self.data.distances[from_id][to_id] * y[day, from_id, to_id]
                      for day in range(days)
                      for from_id in self.data.distances
                      for to_id in self.data.distances[from_id])

        # visit one starting node each day
        for day in range(days):
            prob += lpSum(x[day, bus_stop] for bus_stop in self.data.bus_stops) + \
                    lpSum(x[day, parking_lot] for parking_lot in self.data.parking_lots) == 1
            
        # link arcs to nodes (flow conservation)
        for day in range(days):
            for from_id in self.data.distances:
                # only if there are outgoing arcs
                if len(self.data.distances[from_id]) > 0:
                    prob += lpSum(y[day, from_id, to_id] for to_id in self.data.distances[from_id]) == x[day, from_id]
            for to_id in self.data.distances_reverse:
                # only if there are incoming arcs
                if len(self.data.distances_reverse[to_id]) > 0:
                    prob += lpSum(y[day, from_id, to_id] for from_id in self.data.distances_reverse[to_id]) == x[day, to_id]

        # no subtour consisting of stamp_points
        for day in range(days):
            for from_id in self.data.stamp_points:
                for to_id in self.data.stamp_points:
                    if from_id in self.data.distances and to_id in self.data.distances[from_id]:
                        prob += z[day, from_id] + 1 <= z[day, to_id] + len(self.data.stamp_points) * (1 - y[day, from_id, to_id])

        # visit at least min_stamps stamp_points
        prob += lpSum(x[day, stamp_point] for day in range(days) for stamp_point in self.data.stamp_points) >= min_stamps

        # maximum daily distance
        for day in range(days):
            prob += lpSum(self.data.distances[from_id][to_id] * y[day, from_id, to_id]
                          for from_id in self.data.distances
                          for to_id in self.data.distances[from_id]) <= maximum_daily_distance
            
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

        # create the solution
        tours = []
        for day in range(days):
            # build prev dict for each day
            prev = {}
            for from_id in self.data.distances:
                for to_id in self.data.distances[from_id]:
                    if y[day, from_id, to_id].value() > 0.5:
                        prev[to_id] = from_id
            # find the start node (not a stamp_point)
            start_id = None
            for node_id in prev:
                if node_id not in self.data.stamp_points:
                    start_id = node_id
                    break
            # build the tour
            tour = []
            node_id = start_id
            while node_id is not None:
                tour.append(self.data.node(node_id))
                node_id = prev.get(node_id)
            tours.append(tour)
        return Solution(tours)
