import os
import io
import sys
import mmap
import concurrent.futures

INPUT_FILE_PATH = "./measurements.txt"
ALLOC_GRAN = mmap.ALLOCATIONGRANULARITY 

def get_chunk_boundaries():
    f_size = os.stat(INPUT_FILE_PATH).st_size
    size_per_core = f_size // os.cpu_count()
    boundaries = []
    with io.open(INPUT_FILE_PATH, 'rb') as f:
        start_pos = 0
        end_pos = start_pos + size_per_core
        while end_pos < f_size:
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
            # print(f"start: {start_pos}, end: {end_pos}, size-diff: {end_pos-start_pos}")
            start_pos = end_pos
    return boundaries

def process_chunk(chunk_start, chunk_end):
    ws_dict = {}
    aligned_offset = (chunk_start // ALLOC_GRAN) * ALLOC_GRAN
    aligned_seek_pos = abs(aligned_offset-chunk_start)
    aligned_length = aligned_seek_pos + chunk_end - chunk_start
    with open(INPUT_FILE_PATH, 'rb') as f_reader:
        mm = mmap.mmap(fileno=f_reader.fileno(), length=aligned_length, offset=aligned_offset, flags=mmap.MAP_PRIVATE)
        mm.seek(aligned_seek_pos)
        line = mm.readline()
        while line != b'':
            city, temp = line.split(b';')
            temp = float(temp)
            try:
                # min temp
                if ws_dict[city][0] > temp: 
                    ws_dict[city][0] = temp
                # max temp
                if ws_dict[city][1] < temp: 
                    ws_dict[city][1] = temp
                ws_dict[city][2] += temp
                ws_dict[city][3] += 1
            except:
                ws_dict[city] = [temp, temp, temp, 1]
            line = mm.readline()
        mm.close()
    return ws_dict

def launch():
    ws_dict = {}
    chunk_boundaries = get_chunk_boundaries()
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        chunk_result_futures = [executor.submit(process_chunk, start, end) for start, end in chunk_boundaries]
        for future in concurrent.futures.as_completed(chunk_result_futures):
            try:
                chunk_result = future.result()
            except Exception as excpt:
                print("Exception in reading result from future: {0}".format(excpt))
            else:
                for city, temps in chunk_result.items():
                    try:
                        if ws_dict[city][0] > temps[0]:
                            ws_dict[city][0] = temps[0]
                        if ws_dict[city][1] < temps[1]:
                            ws_dict[city][1] = temps[1]
                        ws_dict[city][2] += temps[2]
                        ws_dict[city][3] += temps[3]
                    except:
                        ws_dict[city] = [temps[0], temps[1], temps[2], temps[3]]

    print(sorted([(city.decode(), f"{temps[0]}/{round(temps[2]/temps[3], 2)}/{temps[1]}") for city,temps in ws_dict.items()], key=lambda ws:ws[0]))

if __name__ == "__main__":

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"Missing {INPUT_FILE_PATH} file")
        sys.exit(1)
    
    launch()