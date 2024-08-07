import os
import unittest
from collections import deque

from amalgamation.amalgamation import (
    retrieve_source_set,
    combine_c_standard_lib,
    create_directed_graph,
    dfs,
    topsort,
)
from amalgamation.tests.test_helper_functions import suppress_output


class TestDirectedGraph(unittest.TestCase):
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    EMBED_DB = os.path.join(PROJECT_ROOT, "src", "embedDB")
    QUERY_INTERFACE = os.path.join(PROJECT_ROOT, "src", "query-interface")
    SPLINE = os.path.join(PROJECT_ROOT, "src", "spline")
    UTILITY_FUNCTIONS = os.path.join(PROJECT_ROOT, "lib", "EmbedDB-Utility")

    # Test specific directories
    source_code_directories = [EMBED_DB, QUERY_INTERFACE, SPLINE, UTILITY_FUNCTIONS]

    c_stand_w_arduino = {
        "#include <assert.h>",
        "#include <complex.h>",
        "#include <ctype.h>",
        "#include <errno.h>",
        "#include <fenv.h>",
        "#include <float.h>",
        "#include <inttypes.h>",
        "#include <iso646.h>",
        "#include <limits.h>",
        "#include <locale.h>",
        "#include <math.h>",
        "#include <setjmp.h>",
        "#include <signal.h>",
        "#include <stdalign.h>",
        "#include <stdarg.h>",
        "#include <stdatomic.h>",
        "#include <stdbool.h>",
        "#include <stddef.h>",
        "#include <stdint.h>",
        "#include <stdio.h>",
        "#include <stdlib.h>",
        "#include <stdnoreturn.h>",
        "#include <string.h>",
        "#include <tgmath.h>",
        "#include <threads.h>",
        "#include <time.h>",
        "#include <uchar.h>",
        "#include <wchar.h>",
        "#include <wctype.h>",
        "#include <Arduino.h>",
    }

    complex_dir_graph = {
        "A": ["B", "C"],
        "B": ["D", "E", "F"],
        "C": ["G"],
        "D": ["H", "I"],
        "E": ["J", "K"],
        "F": ["L", "M"],
        "G": ["N"],
        "H": [],
        "I": ["O", "P"],
        "J": ["Q", "R"],
        "K": ["S"],
        "L": ["T"],
        "M": [],
        "N": ["U", "V"],
        "O": [],
        "P": ["W"],
        "Q": ["X"],
        "R": ["Y"],
        "S": ["Z"],
        "T": [],
        "U": [],
        "V": [],
        "W": [],
        "X": [],
        "Y": [],
        "Z": [],
    }

    """
    This creates a cycle (A -> B -> C -> A)
    """
    cyclic_dir_graph = {"A": {"B"}, "B": {"C"}, "C": {"A"}, "D": {"A", "C"}}

    simple_cyclic_dir_graph = {"A": {"B"}, "B": {"A"}}

    self_ref_cyclic_graph = {"A": {"A"}}

    def test_retrieve_source_set(self):
        """
        This test ensures that the retrieve_source_set function returns a set all the header and .c files
        """

        master_h_test = retrieve_source_set(self.source_code_directories, "h")
        retrieved_header_file_names = {file.file_name for file in master_h_test}

        expected_header_file_names = {
            "advancedQueries.h",
            "embedDB.h",
            "schema.h",
            "spline.h",
            "embedDBUtility.h",
        }
        self.assertEqual(expected_header_file_names, retrieved_header_file_names)

        master_c_test = retrieve_source_set(self.source_code_directories, "c")
        retrieved_c_file_names = {file.file_name for file in master_c_test}

        expected_c_file_names = {
            "advancedQueries.c",
            "embedDBUtility.c",
            "embedDB.c",
            "schema.c",
            "spline.c",
        }
        self.assertEqual(expected_c_file_names, retrieved_c_file_names)

    def test_combine_c_standard_lib_embedDB(self):
        """
        Test ensures that the correct C stand lib is included given a directory
        """

        expected_results = {
            "#include <assert.h>",
            "#include <math.h>",
            "#include <stdbool.h>",
            "#include <stddef.h>",
            "#include <stdint.h>",
            "#include <stdio.h>",
            "#include <stdlib.h>",
            "#include <string.h>",
            "#include <time.h>",
        }

        # set of objects containing source files (fileNode)
        master_h = retrieve_source_set(self.source_code_directories, "h")
        master_c = retrieve_source_set(self.source_code_directories, "c")

        # set of c-standard library that the amaglamation requires
        c_standard_dep = combine_c_standard_lib([master_h, master_c])

        # test
        self.assertEqual(expected_results, c_standard_dep)

    def test_create_dir_graph(self):
        expected_graph = {
            "embedDB.h": {"spline.h"},
            "embedDBUtility.h": set(),
            "advancedQueries.h": {"embedDB.h", "schema.h"},
            "schema.h": set(),
            "spline.h": set(),
        }

        # get headers
        master_h = retrieve_source_set(self.source_code_directories, "h")

        # create directed graph
        directed_graph = create_directed_graph(master_h)

        # test
        self.assertEqual(expected_graph, directed_graph)

    def test_dfs_at_node_embedDB(self):
        """
        Test uses embedDB source code as an example, finds children of EmbedDB node using DFS
        """

        # get headers
        master_h = retrieve_source_set(self.source_code_directories, "h")

        # create directed graph
        directed_graph = create_directed_graph(master_h)

        starting_node = "embedDB.h"
        visited = set()
        stack = deque()
        visiting = []

        dfs(directed_graph, starting_node, visited, stack, visiting)

        # Since the stack represents the reverse order of DFS completion,
        # convert it to a list and reverse it to match the expected DFS order
        actual_res = list(stack)[::-1]

        expected_res = ["embedDB.h", "spline.h"]

        # Check if the actual result matches the expected result
        self.assertEqual(actual_res, expected_res)

        # For 'visited', we need to check if it contains the same elements as 'expected_res', regardless of order
        self.assertEqual(visited, set(expected_res))

    def test_dfs_with_grandchildren(self):
        """
        Test ensures that DFS finds all children and grandchildren of a starting node A.
        """

        directed_graph = {"A": {"B", "C"}, "B": {"D"}, "C": set(), "D": set()}

        starting_node = "A"
        visited = set()
        stack = deque()
        visiting = []

        dfs(directed_graph, starting_node, visited, stack, visiting)

        stack_list = list(stack)

        expected_res = ["A", "B", "C", "D"]

        self.assertEqual(set(stack_list) == set(expected_res), True)

    def test_cyclic_graph_dfs(self):
        """
        Test ensures that the DFS algorithm is able to detect if there are any cycles in a graph then prints an error and exits
        """

        starting_node = "A"
        visited = set()
        stack = deque()
        visiting = []

        with suppress_output():
            with self.assertRaises(SystemExit):
                dfs(self.cyclic_dir_graph, starting_node, visited, stack, visiting)

            with self.assertRaises(SystemExit):
                dfs(
                    self.simple_cyclic_dir_graph,
                    starting_node,
                    visited,
                    stack,
                    visiting,
                )

            with self.assertRaises(SystemExit):
                dfs(self.self_ref_cyclic_graph, starting_node, visited, stack, visiting)

    def test_topological_sort_embedDB(self):
        """
        Test ensures that top sort works for EmbedDB. Results are not unique.
        """

        # Get headers
        master_h = retrieve_source_set(self.source_code_directories, "h")

        # Create directed graph
        directed_graph = create_directed_graph(master_h)

        # Perform top sort
        sorted_graph = topsort(directed_graph)

        # Check if every node in the directed graph is in the sorted result
        self.assertEqual(set(sorted_graph) == set(directed_graph.keys()), True)

    def test_cyclic_graph_topological(self):
        """
        Tests topological sort on a cyclic graph. check_topological_sort_order, should return an error
        """

        with suppress_output():
            with self.assertRaises(SystemExit):
                sorted_graph = topsort(self.cyclic_dir_graph)

            with self.assertRaises(SystemExit):
                sorted_graph = topsort(self.simple_cyclic_dir_graph)

            with self.assertRaises(SystemExit):
                sorted_graph = topsort(self.self_ref_cyclic_graph)
