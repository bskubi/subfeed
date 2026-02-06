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
from .channel import *

@dataclass
class TaskTemplate:
    args: str | List[str]
    stdin: Channel | None = subprocess_pipe("stdin")
    stdout: Channel | None = subprocess_pipe("stdout")
    stderr: Channel | None = subprocess_pipe("stderr")
    sidein: Dict[str, Channel] = field(default_factory = dict)

    @property
    def std(self) -> Dict[Literal["stdin", "stdout", "stderr"], Channel]:
        std = {"stdin": self.stdin, "stdout": self.stdout, "stderr": self.stderr}
        return {k: v for k, v in std.items() if isinstance(v, Channel)}

@dataclass
class Mode:
    parent: str = "wb"
    child: str = "rb"

@dataclass
class Task(TaskTemplate):
    process: Popen | None = field(init = False)

    @staticmethod
    def from_template(template: TaskTemplate, bind: Dict) -> "Task":
        task = copy.deepcopy(template)
        match task.args:
            case str(): task.args = task.args.format(**bind)
            case list(): task.args = [task.arg.format(**bind) for arg in template.args]
            case _: raise TypeError("args must be str or list of strings")

        for name, channel in {**task.std, **task.sidein}.items():
            
            if isinstance(channel, PathChannel):
                channel.path = str(channel.path).format(**bind)
        return Task(
            args = task.args,
            stdin = task.stdin,
            stdout = task.stdout,
            stderr = task.stderr,
            sidein = task.sidein
        )

    def create_channels(self):
        for channel in self.std.values():
            channel.create()
        for channel in self.sidein.values():
            channel.create()

    def start(self, modes: Dict[str, Mode] = None):
        modes = modes or {}
        env = os.environ.copy()
        pass_fds = self._pass_fds(env)
        modes.setdefault("stdin", Mode(parent = "w", child = "r"))
        modes.setdefault("stdout", Mode(parent = "r", child = "w"))
        modes.setdefault("stderr", Mode(parent = "r", child = "w"))

        self.process = Popen(
            args = self.args,
            shell = isinstance(self.args, str),
            stdin = self.stdin.init_process(modes["stdin"].child),
            stdout = self.stdout.init_process(modes["stdout"].child),
            stderr = self.stderr.init_process(modes["stderr"].child),
            env = env,
            pass_fds = pass_fds,
            close_fds = True
        )
        for name, channel in self.std.items():
            if isinstance(channel, SubprocessPipe):
                channel.open(self.process)
            elif name == "stdin":
                channel.open(modes["stdin"].parent)
            else:
                channel.open(modes[name].parent)
        for name, channel in self.sidein.items():
            channel.open(mode=modes[name].parent)


    def _pass_fds(self, env: Dict[str,str]) -> List[int]:
        pass_fds = []
        for name, channel in self.sidein.items():
            match channel:
                case AnonChannel(): fd = channel.r
                case PathChannel(): fd = os.open(channel.path, os.O_RDONLY)
            pass_fds.append(fd)
            env[name] = str(fd)
        return pass_fds

    def __post_init__(self):
        self.thread = None
        self.process = None