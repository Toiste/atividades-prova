import multiprocessing
import csv
import time

def read_csv(file_path):
    """
    Lê o arquivo CSV e retorna os números como uma lista de inteiros.
    """
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        numbers = [int(float(row[0])) for row in reader]
    return numbers

def search_in_chunk(chunk, target, start_index, queue):
    """
    Procura o número-alvo em um segmento do vetor.
    Retorna a posição relativa no vetor original, ou None se não encontrado.
    """
    for i, number in enumerate(chunk):
        if number == target:
            queue.put(start_index + i)
            return
    queue.put(None)

def parallel_search(numbers, target, num_processes):
    """
    Realiza a busca paralela em um vetor de números.
    Divide o vetor entre processos e retorna a posição do número ou None.
    """
    chunk_size = len(numbers) // num_processes
    processes = []
    queue = multiprocessing.Queue()

    for i in range(num_processes):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < num_processes - 1 else len(numbers)
        chunk = numbers[start_index:end_index]
        process = multiprocessing.Process(
            target=search_in_chunk,
            args=(chunk, target, start_index, queue)
        )
        processes.append(process)
        process.start()

    # Verificar os resultados
    result = None
    for _ in processes:
        res = queue.get()
        if res is not None:
            result = res
            break

    # Finalizar os processos
    for process in processes:
        process.join()

    return result

if __name__ == "__main__":
    # Caminho do arquivo CSV com os números
    csv_file = "A.csv"

    # Número que queremos encontrar
    target_number = 753784

    # Número de processos
    num_processes = 4

    numbers = read_csv(csv_file)
    print(f"Total de números carregados: {len(numbers)}")

    # Busca serial
    start_time = time.time()
    try:
        position_serial = parallel_search(numbers, target_number, 1)
    except ValueError:
        position_serial = None
    end_time = time.time()

    serial_time = end_time - start_time

    print(f"Busca serial completa. Tempo: {end_time - start_time:.4f} segundos")
    print(f"Posição encontrada (serial): {position_serial}")

    # Busca paralela

    start_time = time.time()
    position_parallel = parallel_search(numbers, target_number, num_processes)
    end_time = time.time()
    parallel_time = end_time - start_time

    print(f"Busca paralela completa. Tempo: {end_time - start_time:.4f} segundos")
    print(f"Posição encontrada (paralela): {position_parallel}")

    # Verificar resultados
    if position_serial == position_parallel:
        print("Os resultados coincidem!")
    else:
        print("Os resultados NÃO coincidem! Verifique a implementação.")

    # Calcular o speedup
    speedup = serial_time / parallel_time
    print(f"Speedup: {speedup}")
