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

EventField = field(default_factory=Event)

@dataclass
class SyncContext:
    common: Queue = field(default_factory = Queue)

    # Events
    eof: Event = EventField

    def exhausted(self, *queues: Iterable[Queue]) -> bool:
        eof = self.eof.is_set()
        no_tasks = all([
            q.unfinished_tasks == 0 for q in [self.common, *queues]
        ])
        return eof and no_tasks