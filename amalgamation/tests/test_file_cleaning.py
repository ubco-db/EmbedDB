import unittest
import os

from amalgamation.amalgamation import (
    read_file,
    retrieve_pattern_from_source,
    remove_pattern_from_source,
    check_against_standard_library,
    format_external_lib,
)


class TestRead(unittest.TestCase):
    """
    This test suite is responsible for testing the cleaning functionality of the amalgamation
    """

    REGEX_INCLUDE = r'\s*#include ((<[^>]+>)|("[^"]+"))'
    REGEX_DEFINE = "r'(?m)^#define (?:.*\\\r?\n)*.*$'"
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    EMBED_DB = os.path.join(PROJECT_ROOT, "src", "embedDB")
    QUERY_INTERFACE = os.path.join(PROJECT_ROOT, "src", "query-interface")
    SPLINE = os.path.join(PROJECT_ROOT, "src", "spline")
    UTILITY_FUNCTIONS = os.path.join(PROJECT_ROOT, "lib", "EmbedDB-Utility")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = os.path.join(current_dir, "test_files")
    source_code_directories = os.path.join(current_dir, "test_files", "EmbedDB")

    c_stand = {
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
    }

    def test_primitive_retrieve_include(self):
        """
        Test ensures that a file with only includes is returned
        """

        # read file
        c_file_directoy = os.path.join(self.test_files, "only-include.c")
        read_c_file = read_file(c_file_directoy)

        result = retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {
            "#include <time.h>",
            "#include <stdio.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "#include <math.h>",
            "#include <stdint.h>",
            "#include <stdlib.h>",
        }

        self.assertEqual(expected_result, result)

    def test_primitive_retrieve_include_comments(self):
        """
        Test ensures that a file with includes with inline comments is returned
        """

        # read file
        c_file_directoy = os.path.join(
            self.test_files, "only-include-inline-comments.c"
        )
        read_c_file = read_file(c_file_directoy)

        result = retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {
            "#include <time.h>",
            "#include <stdio.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "#include <math.h>",
            "#include <stdint.h>",
            "#include <stdlib.h>",
        }

        self.assertEqual(expected_result, result)

    def test_primitive_includes_no_duplicates(self):
        """
        Test ensures that no duplicate includes are retrieved
        """
        # read file
        c_file_directoy = os.path.join(self.test_files, "only-include-duplicate.c")
        read_c_file = read_file(c_file_directoy)

        result = retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {
            "#include <time.h>",
            "#include <stdio.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "#include <math.h>",
            "#include <stdint.h>",
            "#include <stdlib.h>",
        }

        self.assertEqual(expected_result, result)

    def test_retrieve_pattern_from_source(self):
        """
        Test retrieves includes from an actual source file
        """

        c_file_directoy = os.path.join(self.EMBED_DB, "embedDB.c")
        read_c_file = read_file(c_file_directoy)

        result = retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {
            '#include "embedDB.h"',
            "#include <math.h>",
            '#include "serial_c_iface.h"',
            "#include <stdbool.h>",
            "#include <stddef.h>",
            "#include <stdint.h>",
            "#include <stdio.h>",
            "#include <stdlib.h>",
            "#include <string.h>",
            "#include <time.h>",
        }

        self.assertEqual(expected_result, result)

    def test_primitive_remove_include(self):
        """
        Test ensures that a file with only includes is removed
        """

        # read file
        c_file_directoy = os.path.join(self.test_files, "only-include.c")
        read_c_file = read_file(c_file_directoy)

        removed = remove_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        self.assertNotEqual(read_c_file, removed)

    def test_check_default_libraries_includes_primitive(self):
        """
        Test checks extracted included statements are part of the standard C library
        """

        # set for all includes
        total = set()

        # read file
        c_file_directoy = os.path.join(self.test_files, "only-include.c")
        read_c_file = read_file(c_file_directoy)

        # take result and put
        extracted_headers = retrieve_pattern_from_source(
            read_c_file, self.REGEX_INCLUDE
        )
        result = check_against_standard_library(self.c_stand, extracted_headers, total)

        # test that it returns libraries that are NOT standard
        self.assertEqual(0, len(result))

        expected_total = {
            "#include <time.h>",
            "#include <stdio.h>",
            "#include <stdbool.h>",
            "#include <string.h>",
            "#include <math.h>",
            "#include <stdint.h>",
            "#include <stdlib.h>",
        }

        self.assertEqual(expected_total, total)

    def test_check_default_libraries_source_file(self):
        """
        Test checks which libraries are not default libraries in source code
        """

        # set for all includes
        total = set()

        # read file
        c_file_directoy = os.path.join(self.EMBED_DB, "embedDB.c")
        read_c_file = read_file(c_file_directoy)

        # extract
        extracted_headers = retrieve_pattern_from_source(
            read_c_file, self.REGEX_INCLUDE
        )
        result = check_against_standard_library(self.c_stand, extracted_headers, total)

        # test
        expected_internal_header_files = {
            '#include "embedDB.h"',
            '#include "serial_c_iface.h"',
        }

        self.assertEqual(expected_internal_header_files, result)

        expected_external_header_files = {
            "#include <math.h>",
            "#include <stdbool.h>",
            "#include <stdint.h>",
            "#include <stddef.h>",
            "#include <stdio.h>",
            "#include <stdlib.h>",
            "#include <string.h>",
            "#include <time.h>",
        }

        self.assertEqual(expected_external_header_files, total)

    def test_check_format_external_lib(self):
        """
        Comprehensive test for the format_external_lib function
        """

        ## test input_1

        input_1 = {
            '#include "embedDB.h"',
            '#include "../spline/spline.h"',
        }

        expected_header_file_names = {"embedDB.h", "spline.h"}

        actual_header_file_names = format_external_lib(input_1)
        self.assertEqual(expected_header_file_names, actual_header_file_names)

        ## test input_2

        input_2 = {
            '#include "embedDB.h"',
            '#include "../../../../../spline/spline.h"',
            '#include "../../../../../../spline-and-then/cats/foo/file.h"',
        }

        expected_header_file_names_2 = {
            "embedDB.h",
            "spline.h",
            "file.h",
        }
        actual_header_file_names_2 = format_external_lib(input_2)
        self.assertEqual(expected_header_file_names_2, actual_header_file_names_2)

        ## test input_3

        input_3 = {
            '#include "embedDB.h"',
            '#include "../spline/spline.h"',
            '#include "embedDB.h"',
            '#include "../spline/spline.h"',
        }

        expected_header_file_names_3 = {"embedDB.h", "spline.h"}
        actual_header_file_names_3 = format_external_lib(input_3)
        self.assertEqual(expected_header_file_names_3, actual_header_file_names_3)
