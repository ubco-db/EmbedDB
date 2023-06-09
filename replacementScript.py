import os
from os.path import join

FILE_DIRECTORY = join("home", "runner", ".platformio", "framework-arduino-samd-adafruit")

files = os.listdir(FILE_DIRECTORY)

print(files)
