import os
from os.path import join

os.chdir("/home/runner")

FRAMEWORK_DIR = join(".platformio", "packages", "framework-arduino-samd-adafruit", "variants", "feather_m0")

variant_c_file_old = join(FRAMEWORK_DIR, "variant.cpp")
variant_h_file_old = join(FRAMEWORK_DIR, "variant.h")
