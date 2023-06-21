# SBITS with Learned Indexing

SBITS on Raw memory (NOR, NAND) with learned indexing

1. Uses the minimum of two page buffers for performing all operations. The memory usage is less than 1.5 KB for 512 byte pages.
2. Performance is several times faster than using B-trees and hash-based indexes. Simplifies data management without worrying about low-level details and storing data in raw files.
3. No use of dynamic memory (i.e. malloc()). All memory is pre-allocated at creation of the index.
4. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
5. Option to store time series data with or without an index. Adding an index allows for faster retrieval of records based on data value.
6. Support for iterator to traverse data in sorted order.
7. Support for variable-sized records.
8. Easy to use and include in existing projects.
9. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed for building and execution on an embedded device using Platform.io.**

## License

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Code Files

-   test_sbits.h - test file demonstrating how to get, put, and iterate through data in index
-   vartest.c - test file demonstrating the use of records with variable-sized data
-   main.cpp - main Arduino code file
-   sbits.h, sbits.c - implementation of SBITS index structure supporting arbitrary key-value data items
-   spline.c - Implementation of spline index structure

## Documentation

A paper describing SBITS use for time series indexing is [available from the publisher](https://www.scitepress.org/Link.aspx?doi=10.5220/0010318800920099) and a [pre-print is also available](SBITS_time_series_index.pdf).

### Additional documentation files

-   [Setup & usage](docs/usageInfo.md)
-   [Building project](docs/buildRunInformation.md)
-   [Test suite](docs/testInfo.md)

#### Ramon Lawrence, David Ding, Ivan Carvalho<br>University of British Columbia Okanagan
