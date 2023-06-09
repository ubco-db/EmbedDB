import os
from os.path import join

print("Python Start")

print(os.getcwd())

os.chdir("/home/runner")

FRAMEWORK_DIR = join(".platformio", "packages", "framework-arduino-samd-adafruit", "variants", "feather_m0")
VARIANT_DIR = join("work", "iondb", "iondb", "variant")

variant_c_file_old = join(FRAMEWORK_DIR, "variant.cpp")
variant_h_file_old = join(FRAMEWORK_DIR, "variant.h")

os.remove(variant_c_file_old)
os.remove(variant_h_file_old)

variant_c_file_new = join(VARIANT_DIR, "variant.cpp")
variant_h_file_new = join(VARIANT_DIR, "variant.h")

os.replace(variant_c_file_new, variant_c_file_old)
os.replace(variant_h_file_new, variant_h_file_old)

print("Python End")
