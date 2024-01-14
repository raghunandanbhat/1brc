## 1BRC in Python
This is me attempting [One Billion Row Challenge](https://github.com/gunnarmorling/1brc/tree/main) by [Gunnar Morling](https://twitter.com/gunnarmorling) in Python. The original challenge only accepts solutions written in Java 21. However, [Show & Tell](https://github.com/gunnarmorling/1brc/discussions/categories/show-and-tell) section features all kinds of implementations.

### Running the Challenge
- Generate `measurements.txt` file using `create_measurements.py`
  
  ```
  python create_measurements.py -r 1_000_000_000
  ```
  >This takes a few minutes to generate a file with billion lines. File size is around **13 GB**, make sure to have enough space on your machine.
  It took **816.72** seconds or around **13** minutes on my 16 GB M2 Air.
- Run the `calculate_avg.py` and record time.
---

### Results
These results are from 
#### Attempt 1
- No tricks, just basic python. It took 869.78 seconds and it's painful to look at.
- Read the file line by line and start aggregating the values in a python `dict`
  
  ```python
  with open('measurements.txt') as f_reader:
    for line in f_reader:
      # aggregate values here
  ```
- This avoids loading the entire file into the memory at once.
---
#### Attempt 2
- Processing the file as chunks, each chunk with 100 million lines. Took 319 seconds.
- Used multi-processing to process the chunks concurrently. Aggregation remains same - using python `dict`.
- Instead of one process reading line by line, multiple processes read the same file line by line. Chunk results are again aggregated using python `dict`