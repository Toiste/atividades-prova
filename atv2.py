import multiprocessing
import csv
import time

def read_csv(file_path):
    """
    Lê o arquivo CSV e retorna os números como uma lista de inteiros.
    O formato do número no arquivo pode ser como '2.338296000000000000e+06'.
    """
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        numbers = [int(float(row[0])) for row in reader]  # Converte notação científica para int
    return numbers

def merge(left, right):
    """Função para mesclar dois vetores ordenados"""
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result

def merge_sort(arr):
    """Função de merge sort convencional"""
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)

def merge_sort_parallel(arr, num_processes):
    """Função principal que divide o trabalho entre os processos e mescla os resultados"""
    if len(arr) <= 1:
        return arr

    # Dividindo o array em pedaços para cada processo
    chunk_size = len(arr) // num_processes
    chunks = [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]

    with multiprocessing.Pool(num_processes) as pool:
        # Ordena cada pedaço em paralelo
        sorted_chunks = pool.map(merge_sort, chunks)

    # Mescla os pedaços ordenados
    sorted_arr = sorted_chunks[0]
    for chunk in sorted_chunks[1:]:
        sorted_arr = merge(sorted_arr, chunk)

    return sorted_arr


if __name__ == "__main__":
    # Arquivo CSV com os números
    csv_file = "A.csv"

    # Número de processos
    num_processes = 6

    # Carregar os números do arquivo CSV
    arr = read_csv(csv_file)
    
    print(f"Total de números carregados: {len(arr)}")

    # Ordenação Serial
    start_time = time.time()
    arr_sorted_serial = merge_sort_parallel(arr, 1)
    end_time = time.time()

    serial_time = end_time - start_time
    print(f"Ordenação serial completa. Tempo: {serial_time:.4f} segundos")

    # Ordenação Paralela
    start_time = time.time()
    arr_sorted_parallel = merge_sort_parallel(arr, num_processes)
    end_time = time.time()

    parallel_time = end_time - start_time
    print(f"Ordenação paralela completa. Tempo: {parallel_time:.4f} segundos")

    # Verificar se os resultados coincidem
    if arr_sorted_serial == arr_sorted_parallel:
        print("Os resultados coincidem!")
    else:
        print("Os resultados NÃO coincidem!")

    # Calcular o speedup
    speedup = serial_time / parallel_time
    print(f"Speedup: {speedup:.2f}")
