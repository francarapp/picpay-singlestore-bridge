import sys

partitionElements = []

def args(argv):
    global partitionElements
    if len(argv) < 4:
        print("argumentos da particao nao localizados.")
        exit(1)
        
    partitionElements.append(argv)

