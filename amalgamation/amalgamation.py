from collections import deque
import glob
import os
import re
import sys

"""
GLOBAL VARIABLES: DO NOT EDIT
"""

# REGEX
REGEX_INCLUDE = '\s*#include ((<[^>]+>)|("[^"]+"))'  # https://stackoverflow.com/questions/1420017/regular-expression-to-match-c-include-file
REGEX_COMMENTS = "//.*?\n|/\*.*?\*/"

# Ask Ramon how he would like these dealt with
DEFINE_SEARCH_METHOD_REGEX = "#define SEARCH_METHOD [1-9]"
DEFINE_RADIX_BITS_REGEX = "#define RADIX_BITS [1-9]"
DEFINE_PRINT_ERRORS_REGEX = "#define PRINT_ERRORS"

# standard libraries
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

aud_stand = {
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
    # here down is arduino specific...
    "#include <Arduino.h>",
    "#include <SPI.h>",
    "#include <SdFat.h>",
}

header = """
/******************************************************************************/
/**
 * @file        EmbedDB-Amalgamation
 * @author      EmbedDB Team (See Authors.md)
 * @brief       Source code amalgamated into one file for easy distribution
 * @copyright   Copyright 2024
 *              EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 *  may be used to endorse or promote products derived from this software without
 *  specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/  
"""

"""
FILE PORTION
"""


def read_file(directory):
    """
    Reads the contents of a file and returns it as a string.

    This function attempts to open a file in read-only mode and read its entire contents into a single string, which is then returned. If the function encounters an error while trying to open or read the file (such as the file not existing, lacking the necessary permissions, or the directory not being a valid file path), it will catch the exception, print an error message, and the function will return None.

    :param directory: The path to the file that needs to be read. This should be a string representing a valid file path.

    :return: The contents of the file as a string if the file is successfully read. Returns None if an error occurs during file reading.

    :raises Exception: Prints an error message with the exception detail but does not raise it further. The function handles all exceptions internally and does not propagate them.
    """
    try:
        with open(directory, "r") as file:
            contents = file.read()
        return contents
    except Exception as e:
        sys.stderr.write(f"An error occurred opening the file: {e}\n")
        sys.exit(1)


def save_file(source, file_name, extension, directory=None):
    """
    Writes the provided source content to a file with the specified file name and extension in the specified directory or a default directory if none is specified.

    This function creates a new file or overwrites an existing file in the specified directory (or the default directory if none is provided) with the given file name and extension. It writes the source content to this file.
    The function expects the extension to be provided without a preceding dot (e.g., "txt" instead of ".txt").
    The directory should be provided as a path, which could be absolute or relative. If the directory is not provided, the file will be saved in the current working directory.
    If an error occurs during file writing (such as permission issues, an invalid file name, or issues creating the directory), the function will catch and print the exception.

    :param source: The content to be written to the file. This should be a string.
    :param file_name: The name of the file without the extension. This should be a string.
    :param extension: The extension of the file without a preceding dot. This function expects values like 'txt' for text files, 'py' for Python files, etc.
    :param directory: The path to the directory where the file will be saved. If None, the file will be saved in the current working directory.

    :return: None. The function does not return a value but prints an error message if an exception occurs.

    :raises Exception: Catches any exceptions that occur during file writing and prints them. The function does not re-raise the exceptions.
    """
    if directory is None:
        directory = (
            os.getcwd()
        )  # Use current working directory if no directory is provided

    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            sys.stderr.write(f"An error occurred creating the directory: {e}\n")
            sys.exit(1)

    file_path = os.path.join(directory, file_name + "." + extension)
    try:
        with open(file_path, "w") as file:
            file.write(source)
    except Exception as e:
        sys.stderr.write(f"An error occurred saving the file: {e}\n")
        sys.exit(1)


def get_all_files_of_type(directory, file_extension):
    """
    Searches for and returns a list of all files with a specific extension within a specified directory and its subdirectories.

    This function utilizes the `glob` module to search for files. It constructs a search pattern using the provided directory and file extension, then performs a recursive search to find all matching files.
    If the directory does not exist or there are no files matching the given extension, the function returns an empty list.

    :param directory: The base directory from which the search should start. This should be a string representing a valid directory path.
    :param file_extension: The file extension to search for. This should be provided without a preceding dot (e.g., "txt" for text files).

    :return: A list of strings, where each string is the path to a file that matches the specified extension. If no files are found or the directory does not exist, an empty list is returned.

    Note: The function returns a relative path to each file from the current working directory.
    """
    search_pattern = os.path.join(directory, "**", f"*.{file_extension}")
    files = glob.glob(search_pattern, recursive=True)
    return files


"""
CLEANING PORTION
"""


def retrieve_pattern_from_source(source, pattern):
    """
    Extracts and returns all unique #include statements from a given source code string using a regular expression.

    The function employs a regular expression to identify all occurrences of #include statements in the provided source code.
    It then formats these findings and returns them as a set, ensuring each include statement is unique in the result.
    The regular expression used is designed to be robust and has been adapted from a solution on StackOverflow.

    :param source: The source code string from which #include statements are to be extracted.

    :return: A set of strings, each representing a unique #include statement found in the source code. If no #include statements are found, an empty set is returned.

    """
    result = re.findall(pattern, source)
    return set([f"#include {match[0]}" for match in result])


def remove_pattern_from_source(source, pattern):
    """
    Removes all #include statements from a C or C++ source file content.

    This function uses a regular expression to find and replace all occurrences of #include statements in the provided source code string with a space, effectively removing them.
    The function is intended to be used with source code from .c or .h files.

    :param source: The source code string from which #include statements are to be removed. This should be the content of a .c or .h file as a string.

    :return: A string representing the source code with all #include statements removed. If no #include statements are found, the original source string is returned unchanged.
    """

    return re.sub(pattern, " ", source)


def check_against_standard_library(c_stand, incoming_lib, amalg_c_stand_lib):
    """
    Validates a set of C library headers against the Standard C Library headers and updates a set with the valid standard libraries.

    This function reads a predefined list of Standard C Library headers from a file, compares the incoming library headers against this list, and determines which of the incoming headers are part of the Standard C Library.
    It updates the provided set with these valid standard libraries. The headers that are not part of the standard library are returned as a set.

    :param incoming_lib: A set of strings representing the library headers to be checked against the standard C library.
    :param amalg_c_stand_lib: A set that is updated with the headers from incoming_lib that are part of the standard C library.

    :return: A set of strings representing the headers from incoming_lib that are not part of the standard C library. If all incoming headers are standard, an empty set is returned.
    """

    not_standard_lib = incoming_lib.difference(c_stand)
    required_standard_lib = incoming_lib - not_standard_lib

    amalg_c_stand_lib.update(required_standard_lib)

    # return the difference between the two sets
    return not_standard_lib


def format_external_lib(incoming_lib):
    """
    Transforms include statements with relative paths into just the file names.

    This function takes a set of include statements that may contain relative paths (e.g., #include "../src/EmbedDB.h") and extracts just the file names (e.g., "EmbedDB.h").
    It returns a new set containing these file names, ensuring that each name is unique within the set.

    :param incoming_lib: A set of strings, each representing an include statement possibly containing a relative path.

    :return: A set of strings where each string is a file name extracted from the include statements. If the incoming set is empty, the function returns an empty set.

    Note: The function assumes that the include statements are formatted with double quotes around the paths (e.g., #include "path/to/file").
    It does not handle angle-bracketed include statements (e.g., #include <file>), as those are assumbed to be included from c-standard libraries
    """
    temp = set()

    # extract each dependency from incoming_lib and add it to the set

    for lib in incoming_lib:
        path = lib.split('"')[1]
        file = os.path.basename(path)
        temp.add(file)

    return temp


""" 
Dealing with FileNode
"""


def retrieve_source_set(source_dir, file_type, c_lib=c_stand, ignore_empty=False):
    """
    Generates a set of FileNode objects for each file of a specified type in a source directory and its subdirectories.

    This function scans a given directory and its subdirectories for files with a specified extension. It creates a FileNode object for each file found and adds it to a set,
    ensuring that each file is represented uniquely.

    :param source_dir: The path to the directory containing the source files. This function will also search all subdirectories. Can take either a string or a list.
    :param file_type: The file extension of the files to be processed (e.g., 'h' for C header files or 'c' for C source files).

    :returns: A set of FileNode objects, each representing a unique file of the specified type found in the source directory and its subdirectories.

    Note: The FileNode class should be defined elsewhere in the code, and it should have a constructor that accepts a file path as a parameter.
    """

    if isinstance(source_dir, str):
        source_dir = [source_dir]

    file_set = set()
    dirs = []  # list of strings

    for source in source_dir:
        files = get_all_files_of_type(source, file_type)
        dirs.extend(files)

    # alphabeitcalize for deterministic results
    dirs = sorted(dirs)

    if dirs:
        for dir in dirs:
            # create a new object for each file
            file = FileNode(dir, c_lib)
            # append to the set
            file_set.add(file)
        return file_set
    else:
        if ignore_empty:
            return []
        else:
            sys.stderr.write(f"There was an issue opening files, no files found\n")
            sys.exit(1)


def combine_c_standard_lib(array_of_sets):
    """
    Aggregates C standard library dependencies from multiple FileNode objects into a single set.

    This function iterates over an array of sets, where each set contains FileNode objects.
    It extracts the C standard library dependencies (c_stand_dep) from each FileNode and combines them into a master set.
    This master set represents all unique C standard library dependencies required by the amalgamated source file.

    :param array_of_sets: An array (or list) of sets, where each set contains FileNode objects. Each FileNode object should have a 'c_stand_dep' attribute,
    which is a set of its C standard library dependencies.

    :return: A set containing all unique C standard library dependencies from the provided FileNode objects. If no dependencies are found, an empty set is returned.

    Note: This function assumes that each FileNode object in the sets has a 'c_stand_dep' attribute containing a set of strings, each representing a C standard library dependency.
    """
    master_dep = set()

    for item in array_of_sets:
        for file in item:
            dep = file.c_stand_dep
            master_dep.update(dep)

    return master_dep


"""
Directed Graph Portion 
"""


def create_directed_graph(set_of_FileNodes):
    """
    Constructs a directed graph from a set of FileNode objects.

    This function iterates through a set of FileNode objects, using each FileNode 'file_name' attribute as a node in the graph.
    The 'header_dep' attribute of each FileNode, which should be a set of strings representing the file names of dependencies, is used to create edges in the graph.
    Each node in the resulting graph points to its dependencies, establishing a directed relationship.

    :param set_of_FileNodes: A set of FileNode objects. Each FileNode should have a 'file_name' attribute representing the node's name and a 'header_dep'
    attribute representing the edges as dependencies.

    :return: A dictionary representing a directed graph. The keys are file names (nodes), and the values are sets of file names (edges) representing dependencies.

    Note: This function assumes that the 'file_name' and 'header_dep' attributes of each FileNode object are properly populated and that 'header_dep'
    contains the names of files that each file node depends on.
    """
    directed_graph = {}

    # Each FileNode's file_name becomes value of node, edges are the dependency set each obj contains
    for file_node in set_of_FileNodes:
        node = file_node.file_name
        edges = file_node.header_dep
        directed_graph[node] = edges

    return directed_graph


def dfs(graph, node, visited, result_stack, visiting):
    """
    Performs a depth-first search on a graph from a specified starting node, marking visited nodes during traversal and recording the traversal order only when the last node is found to
    return a subsection of a topological result.

    This function explores as far as possible along each branch before backtracking. It's a recursive implementation that updates the 'visited' set to keep track of
    visited nodes and appends each node to the 'result_stack' once all its descendants have been explored.

    :param graph: A dictionary representing the adjacency list of the graph. Keys are node identifiers, and values are sets or lists of connected nodes.
    :param node: The node from which to start the DFS.
    :param visited: A set that keeps track of which nodes have been visited to avoid revisiting them.
    :param result_stack: A list that records the order in which nodes are fully explored. Nodes are added to this stack once all their children have been visited.

    :return: None. The function updates the 'visited' set and 'result_stack' list in place.

    Note: The graph should be represented as an adjacency list, where each key is a node and its value is a collection of nodes to which it is connected.
    The function modifies 'visited' and 'result_stack' in place, so it does not return any value.
    """

    # add node to visited array so we do not visit it again
    visited.add(node)
    visiting.append(node)

    # for each node's decendant
    for child in graph[node]:
        # check if cylcic
        if child in visiting:
            sys.stderr.write(
                f"An error occured in program depedencies, a cycle with {child} was found\n"
            )
            sys.exit(1)
        # only traverse unvisited nodes
        if child not in visited:
            # traverse grandchildren
            dfs(graph, child, visited, result_stack, visiting)

    visiting.remove(node)
    # once at the end of dfs, add results to the stack
    result_stack.append(node)


def topsort(graph):
    """
    Performs a topological sort on a directed graph.

    This function implements a topological sort algorithm, which orders the nodes in a directed graph in a linear order, respecting the dependencies represented by the edges.
    It uses a depth-first search (DFS) to explore the graph and a stack to determine the order of nodes.

    :param graph: A dictionary representing the adjacency list of the graph. Keys are node identifiers, and values are sets or lists of connected nodes.

    :return: A deque (double-ended queue) representing the nodes in topologically sorted order.

    Note: The graph should not contain any cycles for a valid topological sort to be possible. If the graph contains cycles, the result will not represent a valid topological ordering.
    TODO use Tarjans strongly connected component algorithm to detect if a graph has cycles. StackOverFlow also suggests that we can detect a cycle in our algorithm too
    """
    # TODO try and sort alphabeitcally by filename to make it determinsitic

    visited = set()
    result_stack = deque()  # fast stack using the collections library
    visiting = []

    # for each node in the graph
    for node in graph.keys():
        # if the node is not visited, search using dfs, add nodes to visited
        if node not in visited:
            dfs(graph, node, visited, result_stack, visiting)

    return list(result_stack)


""" 
Creating Amalgamation
"""


def order_file_nodes_by_sorted_filenames(header_file_nodes, sorted_graph):
    """
    Reorders a collection of FileNode objects based on a sorted list of filenames.

    This function creates a mapping from filenames to their corresponding FileNode objects and then uses a sorted list of filenames to arrange the FileNode objects in the same order.
    The function assumes that each filename in the sorted list has a corresponding FileNode object in the input collection.

    :param header_file_nodes: A collection (e.g., list or set) of FileNode objects to be reordered. Each FileNode should have a 'file_name' attribute.
    :param sorted_graph: A list of filenames (strings) representing the desired order for the FileNode objects.
    :param isCpp:

    :return: A list of FileNode objects ordered according to the order of filenames in 'sorted_graph'.

    Note: The function assumes that 'sorted_graph' contains all filenames corresponding to the FileNode objects in 'header_file_nodes' and that the 'file_name' attribute of each FileNode is unique within
    'header_file_nodes'.
    """

    filename_to_filenode = {}

    for FileNode in header_file_nodes:
        name = FileNode.file_name
        filename_to_filenode[name] = FileNode

    ordered = []

    for node in sorted_graph:
        ordered.append(filename_to_filenode[node])

    return ordered


def create_amalgamation(files, isCpp):
    amalgamation = ""

    if isCpp:
        openExtern = """
            #ifdef __cplusplus
            extern "C" {
            #endif
        """
        closeExtern = """
            #ifdef __cplusplus
            }
            #endif
        """

    for file in files:
        if isinstance(file, FileNode):
            filename = (
                "/************************************************************"
                + file.file_name
                + "************************************************************/\n"
            )
            # .cpp compiler must treat .c source code to prevent name mangling etc.
            if isCpp == True and file.file_ext == ".c":
                amalgamation += (
                    filename + openExtern + file.contents + closeExtern + "\n"
                )
            else:
                amalgamation += filename + file.contents + "\n"

        else:
            amalgamation += file + "\n"

    return amalgamation


"""
@TODO only open up standard-c-lib.txt once! 
"""


class FileNode:
    """
    A class to represent a file node which encapsulates file information and dependencies.

    Attributes:
        c_stand_dep (set): A class variable that stores C standard library dependencies.
        header_dep (set): Stores header file dependencies for this specific file.
        file_dir (str): The directory path of the file.
        file_name (str): The name of the file.
        contents (str): The contents of the file after processing.

    Methods:
        __init__(self, file_dir): Initializes the FileNode with directory information, reads the file,
                                  processes its contents, and identifies dependencies.
    """

    c_stand_dep = set()
    header_dep = set()
    file_dir = ""
    file_ext = ""
    file_name = ""
    contents = ""

    def __init__(self, file_dir, c_lib):
        """
        Initializes the FileNode instance, reads the file, and processes its dependencies.

        The initialization process involves reading the file's content, extracting its #include statements,
        identifying local and standard library dependencies, removing include statements from the content, and
        formatting local dependencies.

        Args:
            file_dir (str): The directory path of the file to be processed.

        Steps:
            1. Assigns the file directory and extracts the file name.
            2. Reads the content of the file.
            3. Retrieves all #include statements within the file content.
            4. Identifies which of these includes are local dependencies and which are standard library dependencies.
            5. Removes all #include statements from the file content.
            6. Formats local dependencies to store only the file names.
        """
        # assign directory
        self.file_dir = file_dir
        # retrieve file name
        self.file_name = os.path.basename(file_dir)
        # retrieve file extension
        self.file_ext = os.path.splitext(file_dir)[1]
        # open file
        original = read_file(file_dir)
        # retrieve included libraries
        includes = retrieve_pattern_from_source(original, REGEX_INCLUDE)
        # find local dependencies
        self.header_dep = check_against_standard_library(
            c_lib, includes, self.c_stand_dep
        )
        # remove includes and assign source
        self.contents = remove_pattern_from_source(original, REGEX_INCLUDE)
        # format local dependencies
        self.header_dep = format_external_lib(self.header_dep)


def amalgamate(
    dir, c_lib, file_name, isCpp=False, save_directory=None, split_header_files=True
):
    """
    Amalgmates C source code similar to SQLite https://sqlite.org/amalgamation.html

    Does this by reading in all .c and .h files in a directory, creates a FileNode object for each file which reads and saves the contents, then parses through the content to find which
    libraries it uses, and saves two dependency lists. The first one, 'c_stand_dep', is a list containing the standard c-lib that the file relies on, which is also specified as an argument
    to this function so useres can modify this depending on the project. The second dependency list, 'header_dep', is all the different header files inside the directory each source file relies upon.
    A topological sort is perfomed on that dependency list to create a non-unqiue though somewhat determinsitc ordering of the file dependency. It is somewhat determinsitic because retrieve_source_set
    returns files alphabetically sorted.

    :param dir: a directory pointing to the source file you would like to amalgamate
    :param c_lib:  set of standard c libraries that are not in your project source files. These libraries are not used in topological sorting so they will not be amaglamated.
    May change depending on the nature of your project.
    :param file_name: the name of the amalgamated file
    :param isCPP: dictates whether to save the file as a .cpp file or a .c file. Does not influence if EmbedDB will read .cpp files.
    :param save_directory: the directory you would like the amalgamation files saved too. Saves to the root directory by default.
    :param split_header_files: An optional boolean argument that prevents the amalgamation from splitting the source files.


    """

    """
    Create a set of FileNode objects for each file type. FileNode objects will automatically parse the file, clean, compare with the C stand-lib defined as an argument, 
    and create a small dependency graph. Sets are used to maintain uniqueness.
    """
    header_file_nodes = retrieve_source_set(dir, "h", c_lib)
    source_file_nodes = retrieve_source_set(dir, "c", c_lib)
    if isCpp:
        cpp_files_nodes = retrieve_source_set(dir, "cpp", c_lib, True)

    """
    Since each FileNode object contains a set of which files they rely on, combining into a global set is nec to amalgamation.
    """
    c_standard_dep = combine_c_standard_lib(
        [header_file_nodes, source_file_nodes, cpp_files_nodes if isCpp else []]
    )

    """
    Next step is iterting over the set containing the header files and creating a directed graph for all the dependencies. After that, a topological sort is performed
    which will create a non-unique but somewhat deterministic sort since the files are sorted alphabetically. 
    """
    dir_graph = create_directed_graph(header_file_nodes)
    sorted_graph = topsort(dir_graph)

    """
    The sorted_graph does not sort by filename right now unfortiantely, but future iterations of this amalgamation could include that functionality, so comparing
    directly to the original source is nec. The next step is taking the topological sort and bassically extracting them into two files, one having all the .c code 
    and the other having all the .h code so it's easy to include in other projects, and finally saving the files.
    """
    sorted_h = order_file_nodes_by_sorted_filenames(header_file_nodes, sorted_graph)

    amalg_h = create_amalgamation(c_standard_dep, isCpp) + create_amalgamation(
        sorted_h, isCpp
    )
    amalg_c = create_amalgamation(source_file_nodes, isCpp)
    if isCpp:
        amalg_cpp = create_amalgamation(cpp_files_nodes, isCpp)

    # save
    if split_header_files:
        save_file(header + amalg_h, file_name, "h", save_directory)
        save_file(
            '#include "./EmbedDB.h"\n' + header + amalg_c,
            file_name,
            "cpp" if isCpp else "c",
            save_directory,
        )
    else:
        save_file(
            header + amalg_h + "\n" + amalg_c,
            file_name,
            "cpp" if isCpp else "c",
            save_directory,
        )


def main():
    # Load directories
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    EMBEDDB = os.path.join(PROJECT_ROOT, "src", "embedDB")
    QUERY_INTERFCE = os.path.join(PROJECT_ROOT, "src", "query-interface")
    SPLINE = os.path.join(PROJECT_ROOT, "src", "spline")
    OUTPUT_DIRECTORY = os.path.join(PROJECT_ROOT, "src", "distribution")
    # create standard embedDB amalgamation
    amalgamate(
        [EMBEDDB, QUERY_INTERFCE, SPLINE], aud_stand, "embedDB", False, OUTPUT_DIRECTORY
    )


if __name__ == "__main__":
    main()
