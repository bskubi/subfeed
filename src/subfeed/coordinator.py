from dataclasses import dataclass, field
from typing import List, Dict, Type, IO
from threading import Thread
from queue import Queue
import time
import os
import sys
from .sync_context import SyncContext
from .task import TaskTemplate, Task, Mode
from .worker import Worker
from .writer import Writer

@dataclass
class WriterSpec:
    type: Type[Writer] = field(default_factory = Writer)
    exhaust: bool = True
    mode: Mode = field(default_factory = Mode)

@dataclass
class Coordinator:
    # --- Configuration ---
    template: TaskTemplate
    count: int
    writer_specs: Dict[str, WriterSpec]
    daemonize: bool = False
    bind_id: str = "id"

    # Tuning Parameters
    common_queue_multiplier: int = 10
    writer_queue_maxsize: int = 2  # <--- CRITICAL: Prevents stealing

    # --- Internal State ---
    context: SyncContext = field(init=False)
    tasks: List[Task] = field(init=False)
    workers: List[Worker] = field(init=False)
    startup_threads: List[Thread] = field(init=False)

    def __post_init__(self):
        # 1. Common Queue (The "Pool")
        # Large enough to absorb stdin bursts, but not infinite
        maxsize = self.count * self.common_queue_multiplier
        self.context = SyncContext(Queue(maxsize=maxsize))

        self.tasks = [
            Task.from_template(self.template, bind={self.bind_id: i})
            for i in range(self.count)
        ]
        self.workers = []
        self.startup_threads = []

    def _activate_node(self, task: Task, modes: Dict, writer_types: Dict):
        """
        Runs in a background thread. Tries to bring a node online.
        """
        # 1. Start Process
        task.start(modes)

        # 2. Create Worker
        # Clamp maxsize to prevent a fast-initializing worker from
        # hoarding all the batches in its queue before the others can
        # claim them.
        worker = Worker.from_task(
            self.context, 
            task, 
            writer_types,
            maxsize=self.writer_queue_maxsize
        )

        # Ignore BrokenPipeError if channel does not need to be exhausted
        exhaust_channels = self.exhaust_channels
        for name, writer in worker.writers.items():
            writer.ignore_broken_pipe = name not in exhaust_channels
        
        
        worker.start()
        
        # 3. Register
        self.workers.append(worker)

    def start(self):
        """
        Launches startup threads and returns AS SOON AS the system is viable.
        """
        # 1. Create Channels
        for task in self.tasks:
            task.create_channels()

        # 1.5 Daemonize (Optional)
        if self.daemonize:
            self._daemonize()

        # 2. Prepare Config
        modes = {name: spec.mode for name, spec in self.writer_specs.items()}
        writer_types = {name: spec.type for name, spec in self.writer_specs.items()}

        # 3. Fire and Forget (mostly)
        self.startup_threads = []
        for task in self.tasks:
            t = Thread(
                target=self._activate_node, 
                args=(task, modes, writer_types),
                daemon=True
            )
            t.start()
            self.startup_threads.append(t)

        # 4. The "Viability" Latch
        # Wait until at least one worker is online.
        while len(self.workers) == 0:
            # Check if all startup threads died without success
            if not any(t.is_alive() for t in self.startup_threads):
                raise RuntimeError("All workers failed to start!")
            time.sleep(0.01)
        
        # Return control to main thread immediately.
        # Other workers will join the pool whenever they finish booting.

    def feed(self, item):
        self.context.common.put(item)

    def close(self):
        self.context.eof.set()

        exhaust_channels = self.exhaust_channels
        
        while True:
            # COPY the list to avoid "RuntimeError: list changed size during iteration"
            # if a straggler worker starts up right now.
            current_workers = list(self.workers)
            
            if not current_workers:
                # Should not happen if start() worked, but safety first
                time.sleep(0.01)
                continue

            if all(w.exhausted(*exhaust_channels) for w in current_workers):
                break
            time.sleep(0.01)

        for task in self.tasks:
            if task.process:
                task.process.wait()

    @property
    def exhaust_channels(self) -> List[str]:
        return [
            name for name, spec in self.writer_specs.items() 
            if spec.exhaust
        ]

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _daemonize(self, source: IO, log_file: str = "/dev/null"):
        """
        Detach from the terminal using the Double Fork pattern.
        """
        self.log.debug("Daemonizing")
        # 1. Flush buffers
        sys.stdout.flush()
        sys.stderr.flush()

        # 2. PRESERVE INPUT
        # If source is stdin (FD 0), move it to a safe FD (e.g. 3) before nuking FD 0.
        if hasattr(self.source, "fileno") and self.source.fileno() == 0:
            saved_fd = os.dup(0)
            # Re-wrap the safe FD into a new file object.
            # buffering=0 is standard for pipe throughput; adjust if line-buffering is preferred.
            source = os.fdopen(saved_fd, "rb", buffering=0)

        # 3. FORK #1 (Detach from Parent)
        try:
            if os.fork() > 0:
                sys.exit(0) # Parent returns to shell
        except OSError as e:
            sys.exit(f"Fork #1 failed: {e}")

        # --- CHILD (Session Leader) ---
        
        # 4. NEW SESSION
        os.setsid()

        # 5. FORK #2 (Prevent TTY Acquisition)
        try:
            if os.fork() > 0:
                sys.exit(0) # Leader dies
        except OSError as e:
            sys.exit(f"Fork #2 failed: {e}")

        # --- GRANDCHILD (Daemon) ---

        # 6. REDIRECT STREAMS
        # Point 0, 1, 2 to /dev/null or logfile
        devnull = os.open(os.devnull, os.O_RDWR)
        log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

        # stdin -> /dev/null (We already saved the real input to self.source)
        os.dup2(devnull, 0)
        # stdout -> log
        os.dup2(log_fd, 1)
        # stderr -> log
        os.dup2(log_fd, 2)

        if devnull > 2: os.close(devnull)
        if log_fd > 2: os.close(log_fd)