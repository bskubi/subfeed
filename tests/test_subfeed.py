from subfeed import *

def test_line_numbers_fibonaccis():
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
        stdout=FileChannel("/tmp/{id}.out"),  # Write output to 0.out, 1.out...
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

    # Confirm that recovered output matches input
    output = open("/tmp/0.out").readlines() + open("/tmp/1.out").readlines()
    recovered = [[int(i) for i in line.split()] for line in output]
    for line_number, fib in recovered:
        assert expected[line_number] == fib
    
    from pathlib import Path
    Path("/tmp/0.out").unlink(missing_ok=True)
    Path("/tmp/1.out").unlink(missing_ok=True)