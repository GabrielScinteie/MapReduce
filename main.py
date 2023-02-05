import shutil
import string
from mpi4py import MPI
import numpy as np
import os
from enum import IntEnum
import re


class TAG(IntEnum):
    FILE = 0
    START_REDUCE = 1
    START = 2
    DONE = 3
    DONE_FILES = 4  # Master trimite procesului i => procesul i si-a primit toate fisierele
    # Procesul i trimite Masterului => procecsul i a terminat maparea pe procesele sale
    FINAL_REDUCE = 5


def readFile(fileName):
    file = open(f"input/{fileName}", "r", encoding='unicode_escape')
    input = file.read()
    file.close()

    # delimiters = '["() \t\v\n\r\f]+'
    # input = input.translate(str.maketrans('','', string.punctuation))
    # splittedInput = re.split(delimiters, input)

    splittedInput = re.findall("[a-zA-Z]+", input)

    # Daca textul se termina intr-un delimitator, atunci ultimul element din split va fi gol
    if len(splittedInput[len(splittedInput) - 1]) == 0:
        splittedInput = splittedInput[:len(splittedInput) - 1]

    return splittedInput


def getSliceOfAlphabet(rank, noProcesses):
    # mapping the processes ranks (0, n - 1) to (1, n)
    rank = rank - 1
    noProcesses = noProcesses - 1
    letters = string.ascii_lowercase
    slice = []
    for i in range(len(letters)):
        if i % noProcesses == rank:
            slice.append(letters[i])
    return slice


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.size

    path = os.path.abspath(os.getcwd())
    fileNames = os.listdir(f'{path}/input')
    directoryPath = os.path.join(path, 'tempFiles')
    outputPath = os.path.join(path, 'output')

    # print(f'Procesul {rank} am inceput')

    if rank == 0:

        # Creez folderul unde se vor stoca fisierele intermediare
        if os.path.isdir(directoryPath):
            try:
                shutil.rmtree(directoryPath)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
        os.mkdir(directoryPath)

        # Creez folderul une vor fi rezutlatel reducerii
        if os.path.isdir(outputPath):
            try:
                shutil.rmtree(outputPath)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
        os.mkdir(outputPath)

        # Anunt fiecare proces ca poate incepe prelucrarea
        for i in range(1, size):
            comm.send(1, dest=i, tag=TAG.START)

        index = 0
        destination = 1
        # print('IMPARTIRE FISIERE')
        while index != len(fileNames):
            print(f'Am trimis procesului {destination} fisierul {fileNames[index]}')
            comm.send(fileNames[index], dest=destination, tag=TAG.FILE)
            destination = (destination + 1)
            if destination == size:
                destination = 1
            index += 1

        # Anunt fiecare proces ca i-am trimis toate fisierele
        for i in range(1, size):
            print(f'Am anuntat procesul {i} ca i-am trimis toate fisierele!')
            comm.send(True, dest=i, tag=TAG.DONE_FILES)

        # print("\nMAPARE\n")
        # Astept confirmarea ca fiecare proces sa-mi spuna ca si-a terminat de procesat fisierele
        for i in range(1, size):
            data = int(comm.recv(source=MPI.ANY_SOURCE, tag=TAG.DONE_FILES))
            print(f'Am primit confirmare de la procesul {i} ca si-a terminat maparea')

        # print('\nREDUCERE\n')
        # Trimit fiecarui proces lista de litere pentru care trebuie sa faca reduce
        for i in range(1, size):
            comm.send(getSliceOfAlphabet(i, size), dest=i, tag=TAG.START_REDUCE)

        # Astept mesaje de DONE de la procese pentru a putea anunta procesele care au terminat treaba
        for i in range(1, size):
            comm.recv(source=MPI.ANY_SOURCE, tag=TAG.DONE)

        comm.send(1, dest=1, tag=TAG.FINAL_REDUCE)

    else:
        # Nu incepem procesarea pana cand procesul Master nu a creat folderul unde se vor stoca fisierele temporare
        data = comm.recv(source=MPI.ANY_SOURCE, tag=TAG.START)

        # Creez fisierele unde voi scrie rezultatele intermediare
        tempProcessPath = os.path.join(directoryPath, f'p{rank}')
        if os.path.isdir(tempProcessPath):
            try:
                shutil.rmtree(tempProcessPath)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
        os.mkdir(tempProcessPath)

        while 1:
            status = MPI.Status()
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if tag == TAG.FILE:
                # In acest caz data este un nume de fisier ce trebuie procesat
                words = readFile(data)

                for word in words:
                    if word != '':
                        word = word.lower()
                        outFilePath = os.path.join(tempProcessPath, word)
                        outFile = open(outFilePath, 'a')
                        outFile.write(f'{data} ')
                        outFile.close()

                print(f'Procesul {rank} am citit fisierul {data}!')

            elif tag == TAG.DONE_FILES:
                print(f'Procesul {rank} am terminat etapa de mapare!')
                comm.send(True, dest=0, tag=TAG.DONE_FILES)

            elif tag == TAG.START_REDUCE:
                print(f'Procesul {rank} incep etapa de reducere!')

                for root, dirs, files in os.walk(directoryPath):
                    for filename in files:
                        if filename[0] in data:
                            f = open(os.path.join(root, filename), 'r')

                            words = f.read().split()
                            words.sort()

                            result = {}

                            for word in words:
                                if word in result:
                                    result[word] += 1
                                else:
                                    result[word] = 1

                            for key in result:
                                outFilePath = os.path.join(outputPath, filename)
                                outFile = open(outFilePath, 'a')
                                outFile.write(f'({key},{result[key]}) ')
                                outFile.close()

                            f.close()
                break

        comm.send(rank, dest=0, tag=TAG.DONE)

        if rank == 1:
            # print('Procesul 1 incep etapa de reducere finala!')
            comm.recv(source=MPI.ANY_SOURCE, tag=TAG.FINAL_REDUCE)
            outputFilePath = os.path.join(path, 'finalOutput')
            if os.path.isfile(outputFilePath):
                os.remove(outputFilePath)
            output = open(outputFilePath, 'a')
            for root, dirs, files in os.walk(outputPath):
                files.sort()
                for filename in files:
                    f = open(os.path.join(root, filename), 'r')
                    content = f.read()
                    f.close()

                    output.write(f'{filename}:{content}\n')
            output.close()
        print(f'Procesul {rank} am terminat!')
