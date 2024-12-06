from __future__ import annotations
from abc import ABC, abstractmethod
from uuid import uuid4
from typing import Optional
from threading import Event
from enum import Enum


class TaskStatus(Enum):
    PENDING = 'pending'
    SCHEDULED = 'scheduled'
    RUNNING = 'running'
    FAILED = 'failed'
    COMPLETED = 'completed'
    CANCELED = 'canceled'


class Task(ABC):

    def __init__(self, dependencies: Optional[set[Task]] = None, task_id: Optional[str] = None):
        self.__dependencies = frozenset(dependencies) if dependencies else frozenset()
        self.__task_status = TaskStatus.PENDING
        self.__on_task_completed = None
        self.__on_task_canceled = None
        self.__on_task_failed = None

        self.__canceled_event = Event()

        self._task_id = task_id if task_id else str(uuid4())
        self._task_result = None


    @abstractmethod
    def execute(self):
        pass


    def tag(self) -> str:
        return 'default'


    def execute_task(self):
        if self.__canceled_event.is_set():
            self.__task_status = TaskStatus.CANCELED
            if self.__on_task_canceled:
                self.__on_task_canceled(self)

            return

        self.__task_status = TaskStatus.RUNNING

        try:
            self._task_result = self.execute()
            self.__task_status = TaskStatus.COMPLETED

            if self.__on_task_completed:
                self.__on_task_completed(self)
        except Exception as exception:
            self._task_result = exception
            self.__task_status = TaskStatus.FAILED

            if self.__on_task_failed:
                self.__on_task_failed(self)


    def __eq__(self, other: Task) -> bool:
        return self._task_id == other._task_id


    def __hash__(self) -> int:
        return hash(self._task_id)


    def cancel(self):
        self.__canceled_event.set()


    @property
    def dependencies(self) -> set[Task]:
        return self.__dependencies


    @property
    def status(self) -> TaskStatus:
        return self.__task_status


    @property
    def result(self):
        return self._task_result


    def mark_as_scheduled(self):
        self.__task_status = TaskStatus.SCHEDULED


    def register_on_task_completed(self, func):
        self.__on_task_completed = func


    def register_on_task_failed(self, func):
        self.__on_task_failed = func


    def register_on_task_canceled(self, func):
        self.__on_task_canceled = func


    def __repr__(self):
        return f"Task(task_id={self._task_id!r}, task_status={self.__task_status.value!r}, task_result={self._task_result}, dependencies={list(dependency._task_id for dependency in self.__dependencies)!r})"