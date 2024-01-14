import os
import sys
import time
import resource
import concurrent.futures

INPUT_FILE_PATH = "./measurements.txt"
MAX_LINES = 1_000_000_000
MAX_LINES_PER_CHUNK = 100_000_000

def process_chunk(chunk_start, chunk_end):
    print("processing chunk: {0}".format(chunk_start//MAX_LINES_PER_CHUNK))
    ws_dict = {}
    with open(INPUT_FILE_PATH, 'r', newline='') as f_reader:
        for i in range(chunk_start):
            f_reader.readline()

        for _ in range(chunk_start, chunk_end):
            line = f_reader.readline()
            sep_index = line.index(';')
            city, temp = str(line[:sep_index]), float(line[sep_index+1:-1])

            if city in ws_dict:
                    ws_dict[city]['min'] = min(ws_dict[city]['min'], temp)
                    ws_dict[city]['max'] = max(ws_dict[city]['max'], temp)
                    ws_dict[city]['sum'] += temp
                    ws_dict[city]['count'] += 1
            else:
                ws_dict[city] = {'min': temp, 'max': temp, 'sum': temp, 'count': 1}
    
    return ws_dict

def launch():
    print("launching processes on chunks...")
    start = time.time()

    ws_dict = {}

    with concurrent.futures.ProcessPoolExecutor() as executor:
        chunk_result_futures = [executor.submit(process_chunk, start, (start + MAX_LINES_PER_CHUNK)) for start in range(0, MAX_LINES, MAX_LINES_PER_CHUNK)]

        for future in concurrent.futures.as_completed(chunk_result_futures):
            try:
                # print(future.__dict__)
                chunk_result = future.result()
            except Exception as excpt:
                print("Exception: {0}".format(excpt))
            else:
                for city, temps in chunk_result.items():
                    if city in ws_dict:
                        ws_dict[city]['min'] = min(ws_dict[city]['min'], temps['min'])
                        ws_dict[city]['max'] = max(ws_dict[city]['max'], temps['min'])
                        ws_dict[city]['sum'] += temps['sum']
                        ws_dict[city]['count'] += temps['count']
                    else:
                        ws_dict[city] = {'min': temps['min'], 'max': temps['max'], 'sum': temps['sum'], 'count': temps['count']}

    print(sorted(list(map(lambda city,temps: (city, f"{temps['min']}/{round(temps['sum']/temps['count'])*10.0/10.0}/{temps['max']}"), ws_dict.keys(), ws_dict.values())), key=lambda ws:ws[0]))

    end = time.time()
    print("\n---------------------***---------------------\n")
    print("       user mode time :", resource.getrusage(resource.RUSAGE_SELF).ru_utime)
    print("     system mode time :", resource.getrusage(resource.RUSAGE_SELF).ru_stime)
    print("         time elapsed :", end-start)
    print("\n---------------------***---------------------")

if __name__ == "__main__":

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"Missing {INPUT_FILE_PATH} file")
        sys.exit(1)

    launch()
