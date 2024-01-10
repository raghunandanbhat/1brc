## 1BRC in Python
This is me attempting [One Billion Row Challenge](https://github.com/gunnarmorling/1brc/tree/main) by [Gunnar Morling](https://twitter.com/gunnarmorling) using Python. The original challenge only accepts solutions written in Java 21. However, [Show & Tell](https://github.com/gunnarmorling/1brc/discussions/categories/show-and-tell) section features all kinds of implementations.

### Running the Challenge
- Generate `measurements.txt` file using `create_measurements.py`
 
  ```
  python create_measurements.py -r 1_000_000_000
  ```
> [!NOTE]
> This takes a few minutes to generate a file with billion lines. File size is around **13 GB**, make sure to have enough space on your machine.
  It took **816.7147829532623** seconds or **13** minutes on my 16 GB M2 Air and file size is **13 GB**.
- Run the `calculate_avg.py` and record time.
---

#### Runs
<details>
<summary>v1</summary>
No tricks, just basic python. It's painful to see how long it took to complete.
Took **869.7821958065033** seconds.
</details>
