import os
from os.path import join, isfile

Import("env")

FRAMEWORK_DIR = env.PioPlatform().get_package_dir("framework-arduino-samd-adafruit")
WORKING_DIRECTORY = os.getcwd()

print("Hello")
print(str(FRAMEWORK_DIR))
print(str(WORKING_DIRECTORY))
print("goodbye")

variant_c_file_old = join(FRAMEWORK_DIR, "variants", "feather_m0", "variant.cpp")
variant_h_file_old = join(FRAMEWORK_DIR, "variants", "feather_m0", "variant.h")

variant_c_file_new = join(WORKING_DIRECTORY, "variant", "variant.cpp")
variant_h_file_new = join(WORKING_DIRECTORY, "variant", "variant.h")

os.remove(variant_c_file_old)
os.remove(variant_h_file_old)

os.replace(variant_c_file_old, variant_c_file_new)
os.replace(variant_h_file_old, variant_h_file_new)
