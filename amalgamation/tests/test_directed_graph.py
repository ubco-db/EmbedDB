from collections import deque
import unittest
import sys
import os

# fun way to get module :)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import amalgamation as source


class TestDirectedGraph(unittest.TestCase):
    # TODO: Need to fix all tests in this file, they were not ported properly
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    EMBEDDB = os.path.join(PROJECT_ROOT, "src", "embedDB")
    QUERY_INTERFCE = os.path.join(PROJECT_ROOT, "src", "query-interface")
    SPLINE = os.path.join(PROJECT_ROOT, "src", "spline")

    # Test specific directories
    # TODO: add utility functions directory to this list
    source_code_directories = [EMBEDDB, QUERY_INTERFCE, SPLINE]

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

        # TODO: fix test, it is not actually checking that all the needed files names are here
        master_h_test = source.retrieve_source_set(self.source_code_directories, "h")
        retrieved_file_names = [file.file_name for file in master_h_test]

        test_h_result = {
            "advancedQueries.h",
            "embedDB.h",
            "radixspline.h",
            "schema.h",
            "spline.h",
            "utilityFunctions.h",
        }

        for h in master_h_test:
            file = h.file_name
            self.assertEqual(file in test_h_result, True)


        master_c_test = source.retrieve_source_set(self.source_code_directories, "c")

        test_c_result = {
            "advancedQueries.c",
            "utilityFunctions.c",
            "embedDB.c",
            "radixspline.c",
            "schema.c",
            "spline.c",
        }

        for c in master_c_test:
            file = c.file_name
            self.assertEqual(file in test_c_result, True)

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
        master_h = source.retrieve_source_set(self.source_code_directories, "h")
        master_c = source.retrieve_source_set(self.source_code_directories, "c")

        # set of c-standard library that the amaglamation requires
        c_standard_dep = source.combine_c_standard_lib([master_h, master_c])

        # test
        self.assertTrue(expected_results == c_standard_dep)

    def test_create_dir_graph(self):
        expected_graph = {
            "embedDB.h": {"radixspline.h", "spline.h"},
            "utilityFunctions.h": {"embedDB.h"},
            "advancedQueries.h": {"embedDB.h", "schema.h"},
            "schema.h": set(),
            "radixspline.h": {"spline.h"},
            "spline.h": set(),
        }

        # get headers
        master_h = source.retrieve_source_set(self.source_code_directories, "h")

        # create directed graph
        directed_graph = source.create_directed_graph(master_h)

        # test
        self.assertEqual(expected_graph == directed_graph, True)

    def test_dfs_at_node_embedDB(self):
        """
        Test uses embedDB source code as an example, finds children of EmbedDB node using DFS
        """

        # get headers
        master_h = source.retrieve_source_set(self.source_code_directories, "h")

        # create directed graph
        directed_graph = source.create_directed_graph(master_h)

        starting_node = "embedDB.h"
        visited = set()
        stack = deque()
        visiting = []

        source.dfs(directed_graph, starting_node, visited, stack, visiting)

        # Since the stack represents the reverse order of DFS completion,
        # convert it to a list and reverse it to match the expected DFS order
        actual_res = list(stack)[::-1]

        expected_res = ["embedDB.h", "radixspline.h", "spline.h"]

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

        source.dfs(directed_graph, starting_node, visited, stack, visiting)

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

        with self.assertRaises(SystemExit):
            source.dfs(self.cyclic_dir_graph, starting_node, visited, stack, visiting)

        with self.assertRaises(SystemExit):
            source.dfs(
                self.simple_cyclic_dir_graph, starting_node, visited, stack, visiting
            )

        with self.assertRaises(SystemExit):
            source.dfs(
                self.self_ref_cyclic_graph, starting_node, visited, stack, visiting
            )

    def test_topological_sort_embedDB(self):
        """
        Test ensures that top sort works for EmbedDB. Results are not unique.
        """

        # Get headers
        master_h = source.retrieve_source_set(self.source_code_directories, "h")

        # Create directed graph
        directed_graph = source.create_directed_graph(master_h)

        # Perform top sort
        sorted_graph = source.topsort(directed_graph)

        # Check if every node in the directed graph is in the sorted result
        self.assertEqual(set(sorted_graph) == set(directed_graph.keys()), True)

    def test_cyclic_graph_topological(self):
        """
        Tests topological sort on a cyclic graph. check_topological_sort_order, should return an error
        """

        with self.assertRaises(SystemExit):
            sorted_graph = source.topsort(self.cyclic_dir_graph)

        with self.assertRaises(SystemExit):
            sorted_graph = source.topsort(self.simple_cyclic_dir_graph)

        with self.assertRaises(SystemExit):
            sorted_graph = source.topsort(self.self_ref_cyclic_graph)


if __name__ == "__main__":
    unittest.main()
