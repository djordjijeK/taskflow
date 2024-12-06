from threading import Condition
from collections import defaultdict, deque
from src.taskflow.task import Task, TaskStatus
from typing import Optional


class SchedulingException(Exception):
    pass


class Scheduler:

    def __init__(self, tasks: Optional[set[Task]] = None):
        self.__task_to_dependencies: dict[Task, set[Task]] = defaultdict(set)
        self.__task_to_dependents: dict[Task, set[Task]] = defaultdict(set)
        self.__condition = Condition()

        for task in tasks if tasks else set():
            self.schedule(task)


    def schedule(self, task: Task):
        if task in self.__task_to_dependencies:
            raise SchedulingException(f"Task {task} already exists in the scheduler.")

        task.register_on_task_completed (self.__on_task_completed)
        task.register_on_task_failed(self.__on_task_failed)
        task.register_on_task_canceled(self.__on_task_canceled)

        self.__task_to_dependencies[task] = set(task.dependencies)
        for dependency in task.dependencies:
            self.__task_to_dependents[dependency].add(task)


    def __on_task_completed(self, task: Task):
        with self.__condition:
            self.__condition.notifyAll()


    def __on_task_failed(self, task: Task):
        for dependents in self.__task_to_dependents[task]:
            dependents.cancel()

        with self.__condition:
            self.__condition.notifyAll()


    def __on_task_canceled(self, task: Task):
        for dependents in self.__task_to_dependents[task]:
            dependents.cancel()

        with self.__condition:
            self.__condition.notifyAll()


    @property
    def ready_tasks(self):
        if self.__has_cycles():
            raise SchedulingException("Dependency graph contains circular dependencies")

        with self.__condition:
            while True:
                has_pending_tasks = any(task.status == TaskStatus.PENDING for task in self.__task_to_dependencies.keys())

                if not has_pending_tasks:
                    return

                ready_task = None
                for task, dependencies in self.__task_to_dependencies.items():
                    if task.status != TaskStatus.PENDING:
                        continue

                    all_deps_completed = all((dep.status != TaskStatus.RUNNING and dep.status != TaskStatus.PENDING and dep.status != TaskStatus.SCHEDULED) for dep in dependencies)

                    if all_deps_completed:
                        ready_task = task
                        break

                if ready_task:
                    ready_task.mark_as_scheduled()
                    yield ready_task
                else:
                    self.__condition.wait()


    def __has_cycles(self) -> bool:
        out_degree = {task: len(dependencies) for task, dependencies in self.__task_to_dependencies.items()}

        ready_queue = deque(task for task, degree in out_degree.items() if degree == 0)

        processed_count = 0
        while ready_queue:
            task = ready_queue.popleft()
            processed_count += 1

            for dependent in self.__task_to_dependents[task]:
                out_degree[dependent] -= 1
                if out_degree[dependent] == 0:
                    ready_queue.append(dependent)

        return processed_count != len(self.__task_to_dependencies)
