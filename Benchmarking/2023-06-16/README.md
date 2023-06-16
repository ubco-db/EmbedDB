# Benchmarking June 16, 2023

## Parameters

-   Storage Device
    -   32 GB Lexar MicroSD
-   Dataset
    -   sea100K.bin
-   Key Size
    -   4 Bytes
-   Variable data size
    -   0
    -   10
    -   50
    -   100
    -   500
    -   1000

## Details

This test was run to specifically see how efficiently the system could deal with variable data. Variable data was inserted with every record in increasing amounts every test. The queries were performed by taking a randomized copy of the dataset that was inserted and reading through it sequantially and doing a key-value search for each record. This means that 100,000 records were inserted, and all 100,000 were queried and all queries were on an existing record. **_It is important to note that querying this way introduced extra I/Os so this test is not directly comparable to previous tests._**

### Note: The code used to obtain these results is located in this directory
