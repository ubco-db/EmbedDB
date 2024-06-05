# EmbedDB Embedded Database for Time Series Data

EmbedDB is a high performance embedded data storage and index structure optimized for time series data on embedded systems. It supports key-value and relational data and runs on a wide variety of embedded devices. EmbedDB does not require an operating system and outperforms other systems, including SQLite, on small embedded systems. Key features:

1. Minimum memory requirement is 4 KB allowing execution on the smallest devices.
2. Key-value store optimized for time series with extremely fast insert performance.
3. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
4. High-performance [learned index for keys](https://arxiv.org/abs/2302.03085) and efficient, [customizable data index](docs/SBITS_time_series_index.pdf) optimized for flash memory that outperforms B+-trees.
5. Supports any type of storage including raw NOR and NAND chips and SD cards.
6. No dependencies on libraries or need for an operating system.
7. Advanced query API for SQL queries, which can be written by hand or by using our [SQL converter](https://github.com/ubco-db/EmbedDB-SQL)
8. Easily included in C projects.
9. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed for building and execution on an embedded device using Platform.io.** A [desktop version](https://github.com/ubco-db/EmbedDB-Desktop) is also available.

## License

Smoll change

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Example Usage

```c
embedDBState* state = (embedDBState*) malloc(sizeof(embedDBState));
state->keySize = 4;  
state->dataSize = 12;

// Function pointers that can compare keys and data (user customizable)
state->compareKey = int32Comparator;
state->compareData = dataComparator;

// Storage configuration (SD card example shown)
state->pageSize = 512;
state->eraseSizeInPages = 4;
state->numDataPages = 1000;
state->numIndexPages = 48;
char dataPath[] = "dataFile.bin";
state->fileInterface = getSDInterface();
state->dataFile = setupSDFile(dataPath);

// Configure memory buffers
state->bufferSizeInBlocks = 2; // Minimum 2 buffers is required for read/write operations
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);

// Initialize
embedDBInit(state, splineMaxError);

// Store record
uint32_t key = 123;
char data[12] = "TEST DATA";
embedDBPut(state, (void*) &key, dataPtr);

// Get record
embedDBGet(state, (void*) &key, (void*) returnDataPtr);
```

## Quick Start

EmbedDB consists of a series of files located in the [source](src/) folder. For convience, these files have been combined into distribution version of the project located in the [distribution](distribution/) folder. To use EmbedDB in your project, add the two core source files listed below to your project.

Core source files needed: [embedDB.h](distribution/embedDB.h), [embedDB.c](distribution/embedDB.c)

### Running Our Code

To run our project on your machine, follow the [setup](docs/setup.md) instructions.

If you want to run the distribution version of the project, please look at the [distribution](docs/distribution.md) instructions.

Examples:

- [dueMain.cpp](src/dueMain.cpp), [megaMain.cpp](src/megaMain.cpp), [memboardMain.cpp](src/memBoardMain.cpp) are main files for the Arduino Due, Mega, and a custom Arduino board respectively.
- [embedDBExample](src/embedDBExample.h) - An example file demonstrating how to get, put, and iterate through data in index.
- [embedDBVariableDataExample](src/embedDBVariableDataExample.h) - An example file demonstrating the use of records with variable-sized data.
- [embedDBQueryInterfaceExamples](src/advancedQueryInterfaceExample.h) - An example file demonstrating the included embedDB query library.

## Documentation

- [Setup & Usage](docs/usageInfo.md)
- [Simple Query Interface](docs/advancedQueries.md)
- [Configuring a File Interface](docs/fileInterface.md)
- [Performance Benchmarks](benchmarks/README.md)
- [Time Series Index Publication](docs/SBITS_time_series_index.pdf)
- [Time Series Learned Index Publication](https://arxiv.org/abs/2302.03085)
<!-- TODO: EmbedDB publication -->
  
<br>University of British Columbia Okanagan
