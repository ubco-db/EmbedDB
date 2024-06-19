import os

# Define the path
path = 'build/artifacts'

# Create the directory if it does not exist
if not os.path.exists(path):
    os.makedirs(path)
    print(f"Directory '{path}' created.")
else:
    print(f"Directory '{path}' already exists.")
