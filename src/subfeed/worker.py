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
from .channel import Channel
from .task import Task
from .writer import Writer

@dataclass
class Worker:
    context: SyncContext
    writers: Dict[str, Writer]
    timeout = 1.
    thread: Thread = field(init=False)

    @staticmethod
    def from_task(
        context: SyncContext, 
        task: Task, 
        writer_types: Dict[str, Writer] = None,
        maxsize: int = 2
    ) -> "Worker":
        if writer_types is None:
            writer_types = {"stdin": Writer}
        write_channels = {}
        writers: Dict[str, Writer] = {}
        if task.stdin:
            write_channels["stdin"] = task.stdin
            if "stdin" in task.sidein:
                raise ValueError(
                    "Channel.stdin exists but 'stdin' is also in Channel.sidein'" 
                )
            else:
                write_channels = {**write_channels, **task.sidein}
                for name, WriterType in writer_types.items():
                    channel = write_channels.get(name)
                    assert issubclass(WriterType, Writer), f"{name} type {WriterType} is not a Writer"
                    assert name in write_channels, f"writer type {name} is not a Task channel"
                    assert isinstance(channel, Channel), f"channel {name} is not a Channel"
                    assert hasattr(channel, "io"), f"channel {name} has no 'io'. Call {name}.create() first."
                    assert channel.io is not None, f"channel {name}.io is None."
                    assert hasattr(channel.io, "write"), f"channel {name}.io has no 'write' method'."
                    writers[name] = WriterType(
                        context = context, 
                        io = channel.io, 
                        queue = Queue(maxsize=maxsize)
                    )

        return Worker(
            context = context,
            writers = writers
        )
    
    def start(self):
        self.thread.start()
        for writer in self.writers.values():
            writer.start()

    def take(self):
        while not self.exhausted():
            try:
                batch = self.context.common.get(timeout=self.timeout)

                # Fan out to all writers
                for writer in self.writers.values():
                    writer.queue.put(batch)

                self.context.common.task_done()
            except Empty:
                continue

    def exhausted(self, *names: List[str]) -> bool:
        if not names:
            names = list(self.writers.keys())
        queues = [
            writer.queue
            for name, writer in self.writers.items()
            if name in names
        ]
        return self.context.exhausted(*queues)

    def __post_init__(self):
        self.thread = Thread(target = self.take, daemon = True)