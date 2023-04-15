import os
def create_folder(path):
    print("create path", path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        print("path created", path)