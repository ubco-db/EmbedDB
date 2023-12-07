# EmbedDB Index Structure for Time Series Data

EmbedDB is a high performance embedded data storage and index structure for time series data for embedded systems:

1. Uses the minimum of two page buffers for performing all operations. The memory usage is less than 1.5 KB for 512 byte pages.
2. Performance is several times faster than using B-trees and hash-based indexes using learned indexing. Simplifies data management without worrying about low-level details and storing data in raw files.
3. No use of dynamic memory (i.e. malloc()). All memory is pre-allocated at creation of the index.
4. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
5. Option to store time series data with or without an index. Adding an index allows for faster retrieval of records based on data value.
6. Several indexing approaches including learned indexing, radix spline, and bitmaps. 
7. Support for iterator to traverse data sequentially.
8. Works with user-defined file-interfaces and iterators. 
9. Support for variable-sized records.
10. Easy to use and include in existing projects.
11. Included library with easy to use query operators. 
12. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed for building and execution on an embedded device using Platform.io.**
## License

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Code Files

-   [advancedQueries](src/query-interface/advancedQueries.c) - An included library with easy to use query operators. 
-   [embedDBExample](src/embedDBExample.h) - An example file demonstrating how to get, put, and iterate through data in index. 
-   [embedDBVariableDataExample](src/embedDBVariableDataExample.h) - An example file demonstrating the use of records with variable-sized data. 
-   [embedDBQueryInterfaceExamples](src/advancedQueryInterfaceExample.h) - An example file demonstrating the included embedDB library. 
-   [dueMain.cpp](src/dueMain.cpp), [megaMain.cpp](src/megaMain.cpp), [memboardMain.cpp](src/memBoardMain.cpp) are main files for the Arduino due, mega, and custom hardware respectively.
-   [embedDB.h](src/embedDB/embedDB.h), [embedDB.c](src/embedDB/embedDB.c) - Core source files 
-   [spline.c](src/spline/spline.c) - Implementation of spline index structure
-   [radixSpline.c](src/spline/radixspline.c) - Implementation of radix spline index structure. 

## Documentation

A paper describing EmbedDB use for time series indexing is [available from the publisher](https://www.scitepress.org/Link.aspx?doi=10.5220/0010318800920099) and a [pre-print is also available](SBITS_time_series_index.pdf).

More detail regarding learned indexes can be also be found [from the publisher.](https://arxiv.org/abs/2302.03085)

### Additional documentation files

-   [Setup & usage](docs/usageInfo.md)
-   [Setting up a file interface](docs/fileInterface.md)

<br>University of British Columbia Okanagan
