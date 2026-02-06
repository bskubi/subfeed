import copy
from subprocess import Popen
from threading import Thread, Event
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import IO, Callable, List, Any, Literal, Dict, Iterable
import sys
import os
from abc import ABC, abstractmethod
import io
from subprocess import Popen, PIPE

@dataclass
class Channel:
    io: IO = field(init = False)

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def init_process(self, mode: str = "r"):
        pass

@dataclass
class SubprocessPipe(Channel):
    name: Literal["stdin", "stdout", "stderr"]

    def create(self):
        pass

    def init_process(self, mode: str = "r") -> int:
        return PIPE

    def open(self, process: Popen, mode: str = None):
        if hasattr(process, self.name):
            self.io = getattr(process, self.name)

@dataclass
class AnonChannel(Channel):
    r: int = field(init=False)
    w: int = field(init = False)

    def create(self):
        self.r, self.w = os.pipe()

    def init_process(self, mode: str = "r") -> int:
        if "w" in mode:
            return self.w
        else:
            return self.r

    def open(self, mode: str = "r", close_other_fd: bool = True):
        if "r" in mode:
            self.io = open(self.r, mode)
            other_fd = self.w
        elif "w" in mode or "a" in mode:
            self.io = open(self.w, mode)
            other_fd = self.r
        if close_other_fd:
            os.close(other_fd)

@dataclass
class HandleChannel(Channel):
    io: IO

    def create(self):
        pass

    def init_process(self, mode: str = "r"):
        return self.io
    
    def open(self, mode: str = "r"):
        return self.io

@dataclass
class PathChannel(Channel):
    path: str

class FileChannel(PathChannel):
    def create(self):
        open(self.path, "w")

    def init_process(self, mode: str = "r"):
        return open(self.path, mode)

    def open(self, mode: str = "r"):
        self.io = open(self.path, mode)

def subprocess_pipe(name: Literal["stdin", "stdout", "stderr"]):
    return field(
        default_factory = lambda: SubprocessPipe(name = name)
    )
