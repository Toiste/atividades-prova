import numpy as np
from multiprocessing import Process, Array
import time


def bubble_sort_segment(start, end, shared_array):
    
    arr = shared_array[start:end]
    n = len(arr)
    for i in range(n):
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    shared_array[start:end] = arr


def merge_sorted_segments(shared_array, chunk_indices):
   
    chunks = [shared_array[start:end] for start, end in chunk_indices]
    sorted_result = []
    for chunk in chunks:
        sorted_result.extend(chunk)
    sorted_result.sort()  # Ordena para garantir consistência
    for i in range(len(shared_array)):
        shared_array[i] = sorted_result[i]


def parallel_bubble_sort(arr, num_processes=6):
    """
    Implementa o Bubble Sort paralelo sem Pool.
    """
    n = len(arr)
    chunk_size = n // num_processes
    shared_array = Array('i', arr)  # Vetor compartilhado entre processos
    processes = []

    
    chunk_indices = []
    for i in range(num_processes):
        start = i * chunk_size
        end = n if i == num_processes - 1 else (i + 1) * chunk_size
        chunk_indices.append((start, end))
        p = Process(target=bubble_sort_segment, args=(start, end, shared_array))
        processes.append(p)
        p.start()

    
    for p in processes:
        p.join()

    
    merge_sorted_segments(shared_array, chunk_indices)

    
    return list(shared_array)


def main_bubble_sort_parallel(N=20000, num_processes=6):
    # Gerar vetor de números inteiros aleatórios
    arr = np.random.randint(0, 100000, N).tolist()
    print(f"Tamanho do vetor: {N}, Processos usados: {num_processes}")

    start_time = time.time()
    sorted_arr = parallel_bubble_sort(arr, 1)
    end_time = time.time()
    
     # Exibir resultados
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos: serial")
    serialtime = end_time - start_time
    start_time = time.time()
    sorted_arr = parallel_bubble_sort(arr, num_processes)
    end_time = time.time()

    # Exibir resultados
    print(f"Tempo de execução: {end_time - start_time:.2f} segundos: paralela")
    paraleltime = end_time - start_time
    
    print(f"Speedup: {paraleltime}")


# Chamada do algoritmo
main_bubble_sort_parallel()