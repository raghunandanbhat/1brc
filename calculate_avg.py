import os
import io
import sys
import time
import concurrent.futures

INPUT_FILE_PATH = "./measurements.txt"

def get_chunk_boundaries():
    f_size = os.stat(INPUT_FILE_PATH).st_size
    size_per_core = f_size // os.cpu_count()
    boundaries = []
    with io.open(INPUT_FILE_PATH, 'rb') as f:
        start_pos = 0
        end_pos = start_pos + size_per_core
        while end_pos < f_size:
            if not f.seekable():
                print("can't seek file")
                sys.exit(1)
            if (start_pos + size_per_core) < f_size:
                f.seek(size_per_core, os.SEEK_CUR)
                byte_char = f.read(1)
                while byte_char != b'' and byte_char != b'\n':
                    # print(f"char at {f.tell()}: {byte_char}")
                    byte_char = f.read(1)
                end_pos = f.tell()
            else:
                end_pos = f_size
            boundaries.append((start_pos, end_pos))
            print(f"start: {start_pos}, end: {end_pos}, size-diff: {end_pos-start_pos}")
            start_pos = end_pos
    return boundaries

def process_chunk(chunk_start, chunk_end):
    ws_dict = {}
    chunk_size = chunk_end - chunk_start
    bytes_read = 0
    line_count = 0
    with open(INPUT_FILE_PATH, 'r') as f_reader:
        if not f_reader.seekable():
            print("Can't seek file")
            sys.exit(1)
        f_reader.seek(chunk_start)
        for line in f_reader:
            # stop if bytes read is more than chunk size
            bytes_read += len(line)
            if bytes_read > chunk_size:
                break;
            sep_index = line.index(';')
            city, temp = str(line[:sep_index]), float(line[sep_index+1:-1])
            try:
                    ws_dict[city]['min'] = min(ws_dict[city]['min'], temp)
                    ws_dict[city]['max'] = max(ws_dict[city]['max'], temp)
                    ws_dict[city]['sum'] += temp
                    ws_dict[city]['count'] += 1
            except:
                ws_dict[city] = {'min': temp, 'max': temp, 'sum': temp, 'count': 1}
            line_count += 1
        print(f"processed {line_count} lines in range ({chunk_start}, {chunk_end})")
    return ws_dict

def launch():
    print("launching processes on chunks...")
    start = time.time()
    ws_dict = {}
    chunk_boundaries = get_chunk_boundaries()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        chunk_result_futures = [executor.submit(process_chunk, start, end) for start, end in chunk_boundaries]
        for future in concurrent.futures.as_completed(chunk_result_futures):
            try:
                chunk_result = future.result()
            except Exception as excpt:
                print("Exception: {0}".format(excpt))
            else:
                for city, temps in chunk_result.items():
                    try:
                        ws_dict[city]['min'] = min(ws_dict[city]['min'], temps['min'])
                        ws_dict[city]['max'] = max(ws_dict[city]['max'], temps['min'])
                        ws_dict[city]['sum'] += temps['sum']
                        ws_dict[city]['count'] += temps['count']
                    except:
                        ws_dict[city] = {'min': temps['min'], 'max': temps['max'], 'sum': temps['sum'], 'count': temps['count']}

    print(sorted(list(map(lambda city,temps: (city, f"{temps['min']}/{round(temps['sum']/temps['count'])*10.0/10.0}/{temps['max']}"), ws_dict.keys(), ws_dict.values())), key=lambda ws:ws[0]))
    end = time.time()
    print("\n---------------------***---------------------\n")
    print("         time elapsed :", end-start)
    print("\n---------------------***---------------------")

if __name__ == "__main__":

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"Missing {INPUT_FILE_PATH} file")
        sys.exit(1)
    
    launch()