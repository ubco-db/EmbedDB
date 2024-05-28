import unittest
import sys
import os

# fun way to get source :)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import amalgamation as source

"""
Purpose of this test suite is to test the functionality of opening, searching, combining, and saving files. 
Since the glob is part of the default Python library: opening and saving files are not tested extensively.
"""


class TestFileHandlers(unittest.TestCase):
    # source file directory, makes it easy
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = os.path.join(current_dir, "test_files")
    embedDB_files = os.path.join(PROJECT_ROOT, "src", "embedDB")
    query_interface_files = os.path.join(PROJECT_ROOT, "src", "query-interface")
    spline_files = os.path.join(PROJECT_ROOT, "src", "spline")
    utility_functions_files = os.path.join(PROJECT_ROOT, "lib", "EmbedDB-Utility")

    def test_read(self):
        """
        Test ensures that file is read correctly
        """
        test_file = os.path.join(self.test_files, "hello-world.txt")
        result = source.read_file(test_file)
        self.assertEqual(result, "Hello World")

    def test_read_no_file(self):
        """
        Tests that an exception is raised if trying to access a file that does not exist
        """
        with self.assertRaises(SystemExit):
            source.read_file("foo")

    def test_save_file(self):
        """
        Tests that a file is saved, opens the file and certifies that the content is correct, and then deletes the file
        """

        content = "foo fighters!"
        file_name = "foo"
        extension = "c"

        source.save_file(content, file_name, extension)

        file_path = f"{file_name}.{extension}"

        temp = source.read_file(file_path)

        self.assertEqual(content, temp, "Content of the file is incorrect.")

        os.remove(file_path)

        self.assertFalse(os.path.exists(file_path), "File was not deleted.")

    def test_get_all_files_of_type_embedDB_files(self):
        """
        Tests that all relatives paths are returned from a directory
        """

        # test c files
        directories_to_check = [
            self.embedDB_files,
            self.spline_files,
            self.utility_functions_files,
            self.query_interface_files,
        ]
        c_files = []
        for directory in directories_to_check:
            c_files.extend(source.get_all_files_of_type(directory, "c"))

        expected_c_files = [
            os.path.join(self.query_interface_files, "advancedQueries.c"),
            os.path.join(self.embedDB_files, "embedDB.c"),
            os.path.join(self.spline_files, "radixspline.c"),
            os.path.join(self.query_interface_files, "schema.c"),
            os.path.join(self.spline_files, "spline.c"),
            os.path.join(self.utility_functions_files, "embedDBUtility.c"),
        ]

        self.assertCountEqual(expected_c_files, c_files)

        # test h files
        h_files = []
        for directory in directories_to_check:
            h_files.extend(source.get_all_files_of_type(directory, "h"))

        expected_h_files = [
            os.path.join(self.query_interface_files, "advancedQueries.h"),
            os.path.join(self.embedDB_files, "embedDB.h"),
            os.path.join(self.spline_files, "radixspline.h"),
            os.path.join(self.query_interface_files, "schema.h"),
            os.path.join(self.spline_files, "spline.h"),
            os.path.join(self.utility_functions_files, "embedDBUtility.h"),
        ]

        self.assertCountEqual(expected_h_files, h_files)

    def test_get_all_files_of_type_blank_dir(self):
        """
        Test ensures that a blank directory yields a len(0) result and is not truthy
        """

        blank_dir = os.path.join(self.embedDB_files, "blank")

        files = source.get_all_files_of_type(blank_dir, "c")

        self.assertEqual(len(files), 0)
