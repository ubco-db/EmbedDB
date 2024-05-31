# Project Setup

Our project uses the [PlatformIO](platformio.org) extension for Visualt Studio Code to compile and run code on embedded devices.

## Requirements

- [Visual Studio Code](https://code.visualstudio.com/)
- [PlatformIO](https://platformio.org/platformio-ide)

## Board Configuration

Our project is currently configured to run on the Arduino Mega, Due, and a custom board using the Arduino framework. If you have an Arduino board, the setup should be quite similar. Please see the [PlatformIO Docs](https://docs.platformio.org/en/latest/) for information on adding another board to the project [configuration](../platformio.ini) file. PlatformIO has an extensive [list of boards](https://docs.platformio.org/en/latest/boards/index.html) which are already supported.

The project includes files for interacting with an SD card on Arduino boards, and a wrapper for the Ardunio serial output library to define a custom `printf()` function. For an how to setup a main C file for Ardunio using these libraries, see [dueMain](../src/dueMain.cpp) in the source folder. Please note that if you add another main file to the source folder, you must modify the `build_src_filter` option in the [configuration](../platformio.ini) file to exclude the main files for the other boards configured in this project.

## Running our Example File

Once your board is configured with a main file, you can run the included [example file](../src/embedDBExample.h) by calling the `embedDBExample()` function from your main file. This file configures an instance of EmbedDB, and performs some basic functions with the database. To run the main file and example, please select the board you want to run in the PlatformIO extension, and under the *General* tab, choose the *Upload and Monitor* option. This will compile the code, upload it to your board, and monitor the serial connection for printed output.

## Running the Unit Tests

In order to run the unit tests for this project on a different board, you will need to implement a custom test setup file. Please see the [due](lib\Due\dueTestSetup.cpp) test setup file for an example.

Once you have a custom setup file, you can run the unit tests through the *Advanced* tab under the board you configured.
