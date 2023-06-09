import os
from os.path import join

# FILE_DIRECTORY = join("..", "..", "..","home", "runner")
# os.chdir('../../../')
os.chdir("/home/runner")

print("Python File")
print(os.getcwd())
files = os.listdir()

print(files)
