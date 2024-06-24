## Running EmbedDB on Desktop Platforms

The EmbedDB project can be run on both Windows and Linux. This can be achieved using either PlatformIO or the included [makefile](../makefile). GCC must be installed on your system to run EmbedDB using either of these options. For Windows, follow this [tutorial](https://code.visualstudio.com/docs/cpp/config-mingw) to install GCC.

### Running EmbedDB with PlatformIO

Install the PlatformIO extension for Visual Studio Code or the PlatformIO CMD using the [installation instructions](https://platformio.org/install/).
- If you installed PlatformIO using the extension, you can click on the PlatformIO icon on the sidebar, then select *Desktop > General > Upload*.
- If you are running from the command line, run the command `pio run -e desktop --target exec`.

This will run the example file we have included for EmbedDB. There are additional benchmarking files which can be run. Change the **WHICH_PROGRAM** macro in the desktop runner [file](../src/desktopMain.c) to select one of the benchmarking files.

Running the unit tests can also be done with PlatformIO:
- If you installed PlatformIO using the extension, click the PlatformIO icon on the sidebar, and then select *Desktop > Advanced > Test*
- Using the command line, run the command `pio test -e desktop -vv` .

### Running EmbedDB with Makefile

Make must be installed on your system in addition to GCC to run EmbedDB this way.

The  included examples and benchmark files can be run with the command `make build`. By default, the [example](../src/embedDBExample.h) file will run. This can be changed either in the runner [file](../src/desktopMain.c) by changing the **WHICH_PROGRAM** macro. It can also be changed over the command line using the command `make build CFLAGS="-DWHICH_PROGRAM=NUM", with NUM being from 0 - 3.

Unit tests for EmbedDB can also be run using the makefile.
- Make sure the Git submodules for the EmbedDB repository are installed. This can be done with the command `git submodule update --init --recursive`. 
- Then, run the command `make test`. This will run and output the results from the tests to a file called `results.xml` located in the [results](../build/results/) folder. This folder is automatically generated when the make command is run. This file is a JUnit style XML that summarizes the output from each test file.
