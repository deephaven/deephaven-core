import psutil, os

p = psutil.Process(os.getpid())
for dll in p.memory_maps():
    print(dll.path)

