import argparse
import os
import sys
import time

INPUT_FILE_PATH = "./measurements.txt"
MIN_TEMP = float('inf')
MAX_TEMP = float('-inf')

if __name__ == "__main__":

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"Missing {INPUT_FILE_PATH} file")
        sys.exit(1)

    print("File exists.")
    ws_dict = {}

    start = time.time()

    with open(INPUT_FILE_PATH, 'r') as f_reader:
        for line in f_reader:
            city = str(line.split(';')[0])
            temp = float(line.split(';')[1][:-1])
            
            if city in ws_dict:
                ws_dict[city]['min'] = min(ws_dict[city]['min'], temp)
                ws_dict[city]['max'] = max(ws_dict[city]['max'], temp)
                ws_dict[city]['sum'] += temp
                ws_dict[city]['count'] += 1
            else:
                ws_dict[city] = {'min': min(MIN_TEMP, temp), 'max': max(MAX_TEMP, temp), 'sum': temp, 'count': 1}
    
    # f"{temps['min']}/{round(temps['sum']/temps['count'])*10.0/10.0}/{temps['max']}"
    print(sorted(list(map(lambda city,temps: (city, f"{temps['min']}/{round(temps['sum']/temps['count'])*10.0/10.0}/{temps['max']}"), ws_dict.keys(), ws_dict.values())), key=lambda ws:ws[0]))

    end = time.time()
    print(f"Time elapsed: {end-start} seconds")