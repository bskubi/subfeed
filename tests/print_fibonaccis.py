import sys
import os

# Read from stdin (fibonacci sequence)
fibonaccis = sys.stdin

# Read from Side Channel (File Descriptor passed via env var)
fd = int(os.environ["line_numbers"])
line_numbers = open(fd, "r")

for line_number, fib in zip(line_numbers, fibonaccis):
    print(f"{line_number.strip()}\t{fib.strip()}")