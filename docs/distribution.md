
# EmbedDB Distribution

The EmbedDB project is also conviently packaged in to a distribution version consisting of one header file and one C source file. Currently, this only includes the EmbedDB code in the source folder, and not the SD or serial interface (a version that is ready with both those for Arduino is being worked on).

To run the distribution code, you need to uncomment the following lines in the [configuration](../platformio.ini) file:

```ini
; Please uncomment these two lines below located at the top of the file
[platformio]
src_dir = distribution/
```

This will change the source directory for the project to the distribution version.

Only the Arduino Due is currently configured to run the distribution version of the project. Use the `due-dist` environment in PlatformIO when running the distribution version of EmbedDB. Please see the [setup](setup.md) file for configuring the project for another board.
