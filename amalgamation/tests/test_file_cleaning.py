import unittest
import sys
import os

# fun way to get sourceule :) 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# TODO: rename source as source
import amalgamation as source

class TestRead(unittest.TestCase):
    '''
    This test suite is responsible for testing the cleaning functionality of the amalgamation
    '''

    REGEX_INCLUDE = '\s*#include ((<[^>]+>)|("[^"]+"))'
    REGEX_DEFINE = "r'(?m)^#define (?:.*\\\r?\n)*.*$'" 
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = os.path.join(current_dir, 'test_files')
    embedDB_files = os.path.join(current_dir, 'test_files', 'EmbedDB')

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
        "#include <wctype.h>"
    }

    def test_primitive_retrieve_include(self):
        ''' 
        Test ensures that a file with only includes is returned
        '''

        # read file 
        c_file_directoy = os.path.join(self.test_files, 'only-include.c')
        read_c_file = source.read_file(c_file_directoy)

        result = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {'#include <time.h>', '#include <stdio.h>', '#include <stdbool.h>', '#include <string.h>', '#include <math.h>', '#include <stdint.h>', '#include <stdlib.h>'}

        self.assertEqual(result, expected_result)

    def test_primitive_retrieve_include_comments(self):
        ''' 
        Test ensures that a file with includes with inline comments is returned
        '''

        # read file 
        c_file_directoy = os.path.join(self.test_files, 'only-include-inline-comments.c')
        read_c_file = source.read_file(c_file_directoy)

        result = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)
    
        expected_result = {'#include <time.h>', '#include <stdio.h>', '#include <stdbool.h>', '#include <string.h>', '#include <math.h>', '#include <stdint.h>', '#include <stdlib.h>'}

        self.assertEqual(result, expected_result)

    def test_primitive_includes_no_duplicates(self):
        '''
        Test ensures that no duplicate includes are retrieved
        '''
        # read file 
        c_file_directoy = os.path.join(self.test_files, 'only-include-duplicate.c')
        read_c_file = source.read_file(c_file_directoy)

        result = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {'#include <time.h>', '#include <stdio.h>', '#include <stdbool.h>', '#include <string.h>', '#include <math.h>', '#include <stdint.h>', '#include <stdlib.h>'}

        self.assertEqual(result, expected_result)

    def test_retrieve_pattern_from_source(self):
        '''
        Test retrieves includes from an actual source file
        '''

        c_file_directoy = os.path.join(self.embedDB_files, 'embedDB.c')
        read_c_file = source.read_file(c_file_directoy)

        result = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        expected_result = {
            '#include "embedDB.h"',
            '#include <math.h>',
            '#include <stdbool.h>',
            '#include <stdint.h>',
            '#include <stdio.h>',
            '#include <stdlib.h>',
            '#include <string.h>',
            '#include <time.h>',
            '#include "../spline/radixspline.h"',
            '#include "../spline/spline.h"',
        }


        self.assertEqual(result, expected_result)

      
    def test_primitive_remove_include(self):
        '''
        Test ensures that a file with only includes is removed
        '''

        # read file 
        c_file_directoy = os.path.join(self.test_files, 'only-include.c')
        read_c_file = source.read_file(c_file_directoy)

        removed = source.remove_pattern_from_source(read_c_file, self.REGEX_INCLUDE)

        self.assertEqual(removed == read_c_file, False)

    def test_check_default_libraries_includes_primitive(self):
        '''
        Test checks extracted included statements are part of the standard C libarray
        '''

        # set for all includes
        total = set()

        # read file 
        c_file_directoy = os.path.join(self.test_files, 'only-include.c')
        read_c_file = source.read_file(c_file_directoy)
        
        # take result and put 
        extracted_headers = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)
        result = source.check_against_standard_library(self.c_stand, extracted_headers, total)

        # test that it returns libraries that are NOT standard
        self.assertEqual(len(result), 0)

        expected_total = {
            '#include <time.h>', 
            '#include <stdio.h>', 
            '#include <stdbool.h>', 
            '#include <string.h>', 
            '#include <math.h>', 
            '#include <stdint.h>', 
            '#include <stdlib.h>'
        }

        self.assertEqual(total, expected_total)

    def test_check_default_libraries_source_file(self):
        '''
        Test checks which libraries are not default libraries in source code
        '''

        # set for all includes
        total = set()

        # read file
        c_file_directoy = os.path.join(self.embedDB_files, 'embedDB.c')
        read_c_file = source.read_file(c_file_directoy)

        # extract
        extracted_headers = source.retrieve_pattern_from_source(read_c_file, self.REGEX_INCLUDE)
        result = source.check_against_standard_library(self.c_stand, extracted_headers, total)

        # test
        correct_result = {
        '#include "embedDB.h"', 
        '#include "../spline/spline.h"', 
        '#include "../spline/radixspline.h"'
        }

        self.assertEqual(result, correct_result)

        expected_total = { 
            '#include <math.h>',
            '#include <stdbool.h>',
            '#include <stdint.h>',
            '#include <stdio.h>',
            '#include <stdlib.h>',
            '#include <string.h>',
            '#include <time.h>',
        }

        self.assertEqual(expected_total, total)

    def test_check_format_external_lib(self):
        '''
        Comphrensive test for the format_external_lib function
        '''

        ## test input_1 

        input_1 = {
            '#include "embedDB.h"', 
            '#include "../spline/spline.h"', 
            '#include "../spline/radixspline.h"'
        }

        correct_result_1 = {
            "embedDB.h",
            "spline.h",
            "radixspline.h"
        }

        result_1 = source.format_external_lib(input_1)
        self.assertEqual(result_1, correct_result_1)

        ## test input_2 

        input_2 = {
            '#include "embedDB.h"', 
            '#include "../../../../../spline/spline.h"', 
            '#include "../spline/cats/foo/radixspline.h"',
            '#include "../../../../../../spline-and-then/cats/foo/file.h"'
        }

        correct_result_2 = {
            "embedDB.h",
            "spline.h",
            "radixspline.h",
            "file.h"
        }

        result_2 = source.format_external_lib(input_2)
        self.assertEqual(result_2, correct_result_2)

        ## test input_3 

        input_3 = {
            '#include "embedDB.h"', 
            '#include "../spline/spline.h"', 
            '#include "../spline/radixspline.h"',
            '#include "embedDB.h"', 
            '#include "../spline/spline.h"', 
            '#include "../spline/radixspline.h"'
        }

        correct_result_3 = {
            "embedDB.h",
            "spline.h",
            "radixspline.h"
        }

        result_3 = source.format_external_lib(input_3)

        self.assertEqual(result_3, correct_result_3)

        

    
if __name__ == '__main__':
    unittest.main()

        

        
      

