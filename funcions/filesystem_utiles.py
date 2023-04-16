import os
import shutil

def remove_folder(path) :
    print("rm path", path)
    if os.path.exists(path):
        shutil.rmtree(path)

def remove_file(path):
    print("rm path", path)
    if os.path.exists(path):
        os.remove(path)

def create_folder(path):
    print("create path", path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        print("path created", path)
