# subfeed
**High-throughput I/O coordinator for feeding persistent subprocesses.**

`subfeed` is a Python library designed to feed stream data into swarms of persistent subprocesses. Unlike standard process pools that focus on *executing* discrete functions, `subfeed` focuses on *streaming* data into long-running external processes via multiple pipes (stdin + side channels) without deadlocks.

It handles the complex mechanics of non-blocking I/O, file descriptor management, and graceful shutdowns, allowing you to focus on your data flow.

**Features**
+ **Deadlock-Free I/O:** Safely writes to multiple input pipes (e.g., stdin and 3) simultaneously using threaded workers.
+ **Persistent Processes:** "Stoke" long-running processes rather than spawning a new process for every item.
+ **Multi-Channel Support:** Feed distinct data streams to different file descriptors on the same subprocess.
+ **Lazy Startup:** Workers initialize in parallel and come online as ready.
+ **Graceful Shutdown:** Handles the complex sequence of draining buffers, closing pipes, and reaping processes.

## Installation

```bash
pip install subfeed
```

## Usage Example

In this example, we feed a subprocess that expects data on `stdin` AND a side channel (File Descriptor 3).

**1. The Child Process (`child.py`)**
A script that reads fibonacci numbers from stdin and line numbers from a side channel.
```python
import sys
import os

# Read from stdin (fibonacci sequence)
fibonaccis = sys.stdin

# Read from Side Channel (File Descriptor passed via env var)
fd = int(os.environ["line_numbers"])
line_numbers = open(fd, "r")

for line_number, fib in zip(line_numbers, fibonaccis):
    print(f"{line_number.strip()}\t{fib.strip()}")
```

**2. The Coordinator (`main.py`)**
Using `subfeed` to manage the swarm
```python
from subfeed import *

# Define how to format data for the pipes
class LineNumbersWriter(Writer):
    def filter(self, batch):
        line_number = str(batch[0]) + "\n"
        return line_number.encode()

class TextWriter(Writer):
    def filter(self, batch):
        fib_line = str(batch[1]) + "\n"
        return fib_line.encode()

# 1. Define the Task Template
# We want to run "python child.py"
# We need stdin (implicit) and a side channel named "line_numbers"
template = TaskTemplate(
    args="python tests/print_fibonaccis.py",
    stdout=FileChannel("{id}.out"),  # Write output to 0.out, 1.out...
    sidein={"line_numbers": AnonChannel()}
)

# 2. Map data types to channels
specs = {
    "stdin": WriterSpec(TextWriter),
    "line_numbers": WriterSpec(LineNumbersWriter)
}

# 3. Start the Swarm (2 concurrent processes)
with Coordinator(template, count=2, writer_specs=specs) as swarm:
    # Generate first 100 fibonacci sequence and indices
    fibs = [0, 1]
    for i in range(98):
        fibs.append(fibs[-2] + fibs[-1])

    expected = {}
    for line_number, fib in enumerate(fibs, start = 1):
        # The 'feed' method accepts a data object that your Writers understand
        expected[line_number] = fib
        batch = [line_number, fib]
        swarm.feed(batch)

output = open("0.out").readlines() + open("1.out").readlines()
recovered = [[int(i) for i in line.split()] for line in output]
for line_number, fib in recovered:
    assert expected[line_number] == fib
```