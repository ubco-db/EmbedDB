
## EmbedDB Distribution

The EmbedDB project is also conviently packaged in to a distribution version consisting of one header file and one C source file. Currently, this only includes the EmbedDB code in the source folder, and not the SD or serial interface (a version that is ready with both those for Arduino is being worked on).

To run the distribution code, make sure that PlatformIO is installed (see [setup](setup.md)). Then, select the PlatformIO extension on the sidebar. Under the *Project Tasks* choose *due-dist > General > Upload and Monitor* to compile, upload, and monitor the EmbedDB example using the distribution version of EmbedDB.

Alternatively, useing the PlatformIO command line interface, run the following command from the root of the project:

```cmd
pio run -e due-dist -t upload -t monitor
```

Only the Arduino Due is currently configured to run the distribution version of the project. Use the `due-dist` environment in PlatformIO when running the distribution version of EmbedDB. Please see the [setup](setup.md) file for configuring the project for another board.

For running the distribution version of EmbedDB on desktop platforms, see the [desktop](desktop.md) documentation.

### EmbedDB Distribution Unit Tests

The suite of unit tests for EmbedDB can also be run using the distribution version. 

Using PlatformIO, select the PlatformIO extension on the sidebar. Then, choose *due-dist > Advanced > Test* to run the unit tests on the Arduino Due.

Alternatively, useing the PlatformIO command line interface, run the following command from the root of the project:

```cmd
pio test -e due-dist -vv
```
