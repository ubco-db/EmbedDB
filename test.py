from os.path import join, isfile

Import("env")

FRAMEWORK_DIR = env.PioPlatform().get_package_dir("framework-arduino-samd-adafruit")
print("Hello")
print(str(FRAMEWORK_DIR))
print("goodbye")
