import os
from os.path import join

FILE_DIRECTORY = join("..","..", "..","home", "runner")

files = os.listdir(FILE_DIRECTORY)

print(files)
