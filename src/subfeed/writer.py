import copy
from subprocess import Popen
from threading import Thread, Event
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import IO, Callable, List, Any, Literal, Dict, Iterable
import sys
from subprocess import Popen, PIPE
import os
from abc import ABC, abstractmethod
import io
from .sync_context import SyncContext

def identity(self, batch):
    return batch

@dataclass
class Writer:
    context: SyncContext
    io: IO
    queue: Queue = field(default_factory = Queue)
    ignore_broken_pipe: bool = False
    timeout = 1.
    thread: Thread = field(init = False)

    # Events

    def start(self):
        self.thread.start()

    def write(self):
        while not self.exhausted():
            try:
                batch = self.queue.get(timeout = self.timeout)
                try:
                    output = self.filter(batch)
                    self.io.write(output)
                    self.io.flush()
                    self.queue.task_done()
                except BrokenPipeError:
                    if self.ignore_broken_pipe:
                        break
                    else:
                        raise
            except Empty:
                continue

        # Close input to unblock processes that are waiting on EOF for it
        try:
            self.io.close()
        except BrokenPipeError as e:
            if self.ignore_broken_pipe:
                pass
            else:
                raise

    def filter(self, batch):
        "Identity filter"
        return batch
    
    def exhausted(self) -> bool:
        return self.context.exhausted(self.queue)

    def __post_init__(self):
        self.thread = Thread(target = self.write, daemon = True)
