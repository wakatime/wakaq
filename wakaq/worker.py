import asyncio
import inspect
import logging
import os
import random
import signal
import sys
import time
import traceback

import psutil

from .exceptions import SoftTimeout, WakaQError
from .logger import log, setup_logging
from .serializer import deserialize, serialize
from .utils import (
    close_fd,
    current_task,
    exception_in_chain,
    flush_fh,
    get_timeouts,
    kill,
    mem_usage_percent,
    read_fd,
    write_fd,
    write_fd_or_raise,
)

ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""


class Child:
    __slots__ = [
        "pid",
        "stdin",
        "pingin",
        "broadcastout",
        "last_ping",
        "soft_timeout_reached",
        "max_mem_reached_at",
        "done",
        "soft_timeout",
        "hard_timeout",
        "current_task",
    ]

    def __init__(self, pid, stdin, pingin, broadcastout):
        os.set_blocking(stdin, False)
        os.set_blocking(pingin, False)
        os.set_blocking(broadcastout, False)
        self.current_task = None
        self.pid = pid
        self.stdin = stdin
        self.pingin = pingin
        self.broadcastout = broadcastout
        self.soft_timeout_reached = False
        self.last_ping = time.time()
        self.done = False
        self.soft_timeout = None
        self.hard_timeout = None
        self.max_mem_reached_at = 0

    def close(self):
        close_fd(self.pingin)
        close_fd(self.stdin)
        close_fd(self.broadcastout)

    def set_timeouts(self, wakaq, task=None, queue=None):
        self.current_task = task
        soft_timeout, hard_timeout = get_timeouts(wakaq, task=task, queue=queue)
        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout

    @property
    def mem_usage_percent(self):
        return psutil.Process(self.pid).memory_percent()


class Worker:
    __slots__ = [
        "wakaq",
        "children",
        "_stop_processing",
        "_pubsub",
        "_pingout",
        "_broadcastin",
        "_num_tasks_processed",
        "_loop",
        "_active_async_tasks",
        "_async_task_context",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self):
        setup_logging(self.wakaq)
        log.info(f"concurrency={self.wakaq.concurrency}")
        log.info(f"async_concurrency={self.wakaq.async_concurrency}")
        log.info(f"soft_timeout={self.wakaq.soft_timeout}")
        log.info(f"hard_timeout={self.wakaq.hard_timeout}")
        log.info(f"wait_timeout={self.wakaq.wait_timeout}")
        log.info(f"exclude_queues={self.wakaq.exclude_queues}")
        log.info(f"max_retries={self.wakaq.max_retries}")
        log.info(f"max_mem_percent={self.wakaq.max_mem_percent}")
        log.info(f"max_tasks_per_worker={self.wakaq.max_tasks_per_worker}")
        log.info(f"worker_log_file={self.wakaq.worker_log_file}")
        log.info(f"scheduler_log_file={self.wakaq.scheduler_log_file}")
        log.info(f"worker_log_level={self.wakaq.worker_log_level}")
        log.info(f"scheduler_log_level={self.wakaq.scheduler_log_level}")
        log.info(f"starting {self.wakaq.concurrency} workers...")
        self._run()

    def _stop(self):
        self._stop_processing = True
        for child in self.children:
            kill(child.pid, signal.SIGTERM)
        try:
            self._pubsub.unsubscribe()
        except:
            log.debug(traceback.format_exc())

    def _run(self):
        self.children = []
        self._stop_processing = False

        pid = None
        for i in range(self.wakaq.concurrency):

            # postpone fork if using too much ram
            if self.wakaq.max_mem_percent:
                percent_used = mem_usage_percent()
                if percent_used >= self.wakaq.max_mem_percent:
                    log.info(
                        f"postpone forking workers... mem usage {percent_used}% is more than max_mem_percent threshold ({self.wakaq.max_mem_percent}%)"
                    )
                    break

            pid = self._fork()
            if pid == 0:
                break

        if pid != 0:  # parent
            self._parent()

    def _fork(self) -> int:
        pingin, pingout = os.pipe()
        broadcastin, broadcastout = os.pipe()
        stdin, stdout = os.pipe()
        pid = os.fork()
        if pid == 0:  # child worker process
            close_fd(stdin)
            close_fd(pingin)
            close_fd(broadcastout)
            self._child(stdout, pingout, broadcastin)
        else:  # parent process
            close_fd(stdout)
            close_fd(pingout)
            close_fd(broadcastin)
            self._add_child(pid, stdin, pingin, broadcastout)
        return pid

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exited)
        signal.signal(signal.SIGINT, self._on_exit_parent)
        signal.signal(signal.SIGTERM, self._on_exit_parent)
        signal.signal(signal.SIGQUIT, self._on_exit_parent)

        log.info("finished forking all workers")

        try:
            self._pubsub = self.wakaq.broker.pubsub()
            self._pubsub.subscribe(self.wakaq.broadcast_key)

            while not self._stop_processing:
                self._read_child_logs()
                self._check_max_mem_percent()
                self._refork_missing_children()
                self._enqueue_ready_eta_tasks()
                self._cleanup_children()
                self._check_child_runtimes()
                self._listen_for_broadcast_task()

            if len(self.children) > 0:
                log.info("shutting down...")
                while len(self.children) > 0:
                    self._cleanup_children()
                    self._check_child_runtimes()
                    time.sleep(0.05)
            log.info("all workers stopped")

        except:
            try:
                log.error(traceback.format_exc())
            except:
                print(traceback.format_exc())
            self._stop()

    def _child(self, stdout, pingout, broadcastin):
        os.dup2(stdout, sys.stdout.fileno())
        os.dup2(stdout, sys.stderr.fileno())
        close_fd(stdout)
        os.set_blocking(pingout, False)
        os.set_blocking(broadcastin, False)
        os.set_blocking(sys.stdout.fileno(), False)
        os.set_blocking(sys.stderr.fileno(), False)
        self._pingout = pingout
        self._broadcastin = broadcastin

        # reset sigchld
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        # stop processing and gracefully shutdown
        signal.signal(signal.SIGTERM, self._on_exit_child)

        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # raise SoftTimeout
        signal.signal(signal.SIGQUIT, self._on_soft_timeout_child)

        setup_logging(self.wakaq, is_child=True)

        try:

            # redis should eventually detect pid change and reset, but we force it
            self.wakaq.broker.connection_pool.reset()

            # cleanup file descriptors opened by parent process
            self._remove_all_children()
            self._num_tasks_processed = 0

            log.debug("started worker process")

            if callable(self.wakaq.after_worker_started_callback):
                self.wakaq.after_worker_started_callback()

            self._active_async_tasks = set()
            self._async_task_context = {}
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._event_loop())

        except (MemoryError, BlockingIOError, BrokenPipeError):
            if current_task.get():
                raise
            log.debug(traceback.format_exc())

        except Exception as e:
            if exception_in_chain(e, SoftTimeout):
                if current_task.get():
                    raise
                log.error(traceback.format_exc())
            else:
                log.error(traceback.format_exc())

        except:  # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
            log.error(traceback.format_exc())

        finally:
            if hasattr(self, "_loop"):
                self._loop.close()

            flush_fh(sys.stdout)
            flush_fh(sys.stderr)
            close_fd(self._broadcastin)
            close_fd(self._pingout)
            close_fd(sys.stdout)
            close_fd(sys.stderr)

    async def _event_loop(self):
        while not self._stop_processing and (
            not self.wakaq.async_concurrency or len(self._active_async_tasks) < self.wakaq.async_concurrency
        ):
            self._send_ping_to_parent()

            queue_broker_key, payload = await self._blocking_dequeue()
            if payload is not None:
                try:
                    task = self.wakaq.tasks[payload["name"]]
                except KeyError:
                    log.error(f'Task not found: {payload["name"]}')
                    task = None

                if task is not None:
                    queue = self.wakaq.queues_by_key[queue_broker_key]
                    current_task.set((task, payload))
                    retry = payload.get("retry") or 0

                    # make sure parent process is still around (OOM killer may have stopped it without sending child signal)
                    try:
                        self._send_ping_to_parent(task_name=task.name, queue_name=queue.name if queue else None)
                    except:
                        # give task back to queue so it's not lost
                        self.wakaq.broker.lpush(queue_broker_key, serialize(payload))
                        current_task.set(None)
                        raise

                    async_task = self._loop.create_task(self._execute_task(task, payload, queue=queue))
                    self._active_async_tasks.add(async_task)
                    self._async_task_context[async_task] = {
                        "task": task,
                        "payload": payload,
                        "queue": queue,
                        "start_time": time.time(),
                    }

            try:
                if self._active_async_tasks:
                    done, pending = await asyncio.wait(
                        self._active_async_tasks, timeout=0.01, return_when=asyncio.FIRST_COMPLETED
                    )

                    for async_task in done:
                        self._active_async_tasks.remove(async_task)
                        context = self._async_task_context.pop(async_task)
                        try:
                            await async_task
                        except (MemoryError, BlockingIOError, BrokenPipeError):
                            current_task.set((context["task"], context["payload"]))
                            raise
                        except asyncio.exceptions.CancelledError:
                            current_task.set((context["task"], context["payload"]))
                            retry += 1
                            max_retries = context["task"].max_retries
                            if max_retries is None:
                                max_retries = (
                                    queue.max_retries if queue.max_retries is not None else self.wakaq.max_retries
                                )
                            if retry > max_retries:
                                log.error(traceback.format_exc())
                            else:
                                log.warning(traceback.format_exc())
                                self.wakaq._enqueue_at_end(
                                    context["task"].name,
                                    queue.name,
                                    context["payload"]["args"],
                                    context["payload"]["kwargs"],
                                    retry=retry,
                                )
                        except Exception as e:
                            current_task.set((context["task"], context["payload"]))
                            if exception_in_chain(e, SoftTimeout):
                                retry += 1
                                max_retries = context["task"].max_retries
                                if max_retries is None:
                                    max_retries = (
                                        queue.max_retries if queue.max_retries is not None else self.wakaq.max_retries
                                    )
                                if retry > max_retries:
                                    log.error(traceback.format_exc())
                                else:
                                    log.warning(traceback.format_exc())
                                    self.wakaq._enqueue_at_end(
                                        context["task"].name,
                                        queue.name,
                                        context["payload"]["args"],
                                        context["payload"]["kwargs"],
                                        retry=retry,
                                    )
                            else:
                                log.error(traceback.format_exc())

                    for async_task in pending:
                        context = self._async_task_context.get(async_task)
                        if not context:
                            continue
                        soft_timeout, _ = get_timeouts(self.wakaq, task=context["task"], queue=context["queue"])
                        if not soft_timeout:
                            continue
                        runtime = time.time() - context["start_time"]
                        if runtime - 0.1 > soft_timeout and not async_task.cancelled():
                            current_task.set((context["task"], context["payload"]))
                            log.debug(
                                f"async task {context['task'].name} runtime {runtime} reached soft timeout, raising asyncio.CancelledError"
                            )
                            async_task.cancel()

                current_task.set(None)
                self._send_ping_to_parent()

            except (MemoryError, BlockingIOError, BrokenPipeError):
                raise

            # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
            except:
                log.error(traceback.format_exc())

            flush_fh(sys.stdout)
            flush_fh(sys.stderr)
            await self._execute_broadcast_tasks()
            if self.wakaq.max_tasks_per_worker and self._num_tasks_processed >= self.wakaq.max_tasks_per_worker:
                log.info(f"restarting worker after {self._num_tasks_processed} tasks")
                self._stop_processing = True
            flush_fh(sys.stdout)
            flush_fh(sys.stderr)

    def _send_ping_to_parent(self, task_name=None, queue_name=None):
        msg = task_name or ""
        if msg:
            msg = f"{msg}:{queue_name or ''}"
        write_fd_or_raise(self._pingout, f"{msg}\n")

    def _add_child(self, pid, stdin, pingin, broadcastout):
        self.children.append(Child(pid, stdin, pingin, broadcastout))

    def _remove_all_children(self):
        for child in self.children:
            self._remove_child(child)

    def _cleanup_children(self):
        for child in self.children:
            if child.done:
                self._remove_child(child)

    def _remove_child(self, child):
        child.close()
        self.children = [c for c in self.children if c.pid != child.pid]

    def _on_exit_parent(self, signum, frame):
        log.debug(f"Received signal {signum}")
        self._stop()

    def _on_exit_child(self, signum, frame):
        self._stop_processing = True

    def _on_soft_timeout_child(self, signum, frame):
        raise SoftTimeout("SoftTimeout")

    def _on_child_exited(self, signum, frame):
        for child in self.children:
            if child.done:
                continue
            try:
                pid, _ = os.waitpid(child.pid, os.WNOHANG)
                if pid != 0:  # child exited
                    child.done = True
            except InterruptedError:  # child exited while calling os.waitpid
                child.done = True
            except ChildProcessError:  # child pid no longer valid
                child.done = True
            if child.done and child.max_mem_reached_at:
                after = round(time.time() - child.max_mem_reached_at, 2)
                log.info(f"Stopped {child.pid} after {after} seconds")

    def _enqueue_ready_eta_tasks(self):
        script = self.wakaq.broker.register_script(ZRANGEPOP)
        for queue in self.wakaq.queues:
            results = script(keys=[queue.broker_eta_key], args=[int(round(time.time()))])
            for payload in results:
                payload = deserialize(payload)
                task_name = payload.pop("name")
                args = payload.pop("args")
                kwargs = payload.pop("kwargs")
                self.wakaq._enqueue_at_front(task_name, queue.name, args, kwargs)

    async def _execute_task(self, task, payload, queue=None):
        log.debug(f"running with payload {payload}")
        if callable(self.wakaq.before_task_started_callback):
            if inspect.iscoroutinefunction(self.wakaq.before_task_started_callback):
                await self.wakaq.before_task_started_callback()
            else:
                self.wakaq.before_task_started_callback()

        try:
            if callable(self.wakaq.wrap_tasks_function):
                if inspect.iscoroutinefunction(task.fn) and not inspect.iscoroutinefunction(
                    self.wakaq.wrap_tasks_function
                ):
                    raise WakaQError(
                        "Unable to execute sync wrap_tasks_with when task is async. Make your wrap_tasks_with function async."
                    )
                if inspect.iscoroutinefunction(self.wakaq.wrap_tasks_function):
                    await self.wakaq.wrap_tasks_function(task.fn, payload["args"], payload["kwargs"])
                else:
                    self.wakaq.wrap_tasks_function(task.fn, payload["args"], payload["kwargs"])

            else:
                if inspect.iscoroutinefunction(task.fn):
                    await task.fn(*payload["args"], **payload["kwargs"])
                else:
                    task.fn(*payload["args"], **payload["kwargs"])

        finally:
            self._num_tasks_processed += 1
            if callable(self.wakaq.after_task_finished_callback):
                if inspect.iscoroutinefunction(self.wakaq.after_task_finished_callback):
                    await self.wakaq.after_task_finished_callback()
                else:
                    self.wakaq.after_task_finished_callback()

    async def _execute_broadcast_tasks(self):
        payloads = read_fd(self._broadcastin)
        if payloads == "":
            return
        for payload in payloads.splitlines():
            payload = deserialize(payload)
            try:
                task = self.wakaq.tasks[payload["name"]]
            except KeyError:
                log.error(f'Task not found: {payload["name"]}')
                continue
            retry = 0
            current_task.set((task, payload))
            while True:
                try:
                    self._send_ping_to_parent(task_name=task.name)
                    await self._execute_task(task, payload)
                    current_task.set(None)
                    self._send_ping_to_parent()
                    break

                except (MemoryError, BlockingIOError, BrokenPipeError):
                    raise

                except Exception as e:
                    if exception_in_chain(e, SoftTimeout):
                        retry += 1
                        max_retries = task.max_retries
                        if max_retries is None:
                            max_retries = self.wakaq.max_retries
                        if retry > max_retries:
                            log.error(traceback.format_exc())
                            break
                        else:
                            log.warning(traceback.format_exc())
                    else:
                        log.error(traceback.format_exc())
                        break

                except:  # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
                    log.error(traceback.format_exc())
                    break

    def _read_child_logs(self):
        for child in self.children:
            logs = read_fd(child.stdin)
            if logs != "":
                if log.handlers[0].stream is None:  # filehandle can disappear if we run out of RAM
                    print(logs)
                    self._stop()
                else:
                    log.handlers[0].stream.write(logs)

    def _check_max_mem_percent(self):
        if not self.wakaq.max_mem_percent:
            return
        max_mem_reached_at = max([c.max_mem_reached_at for c in self.children if not c.done], default=0)
        task_timeout = self.wakaq.hard_timeout or self.wakaq.soft_timeout or 120
        now = time.time()
        if now - max_mem_reached_at < task_timeout:
            return
        if len(self.children) == 0:
            return
        percent_used = mem_usage_percent()
        if percent_used < self.wakaq.max_mem_percent:
            return
        log.info(f"Mem usage {percent_used}% is more than max_mem_percent threshold ({self.wakaq.max_mem_percent}%)")
        self._log_mem_usage_of_all_children()
        child = self._child_using_most_mem()
        if child:
            task = ""
            if child.current_task:
                task = f" while processing task {child.current_task.name}"
            log.info(f"Stopping child process {child.pid}{task}...")
            child.soft_timeout_reached = True  # prevent raising SoftTimeout twice for same child
            child.max_mem_reached_at = now
            kill(child.pid, signal.SIGTERM)

    def _log_mem_usage_of_all_children(self):
        if self.wakaq.worker_log_level != logging.DEBUG:
            return
        for child in self.children:
            task = ""
            if child.current_task:
                task = f" while processing task {child.current_task.name}"
            try:
                log.debug(f"Child process {child.pid} using {round(child.mem_usage_percent, 2)}% ram{task}")
            except:
                log.warning(f"Unable to get ram usage of child process {child.pid}{task}")
                log.warning(traceback.format_exc())

    def _child_using_most_mem(self):
        try:
            return max(self.children, lambda c: c.mem_usage_percent)
        except:
            return random.choice(self.children)

    def _check_child_runtimes(self):
        for child in self.children:
            ping = read_fd(child.pingin)
            if ping != "":
                log.debug(f"received ping from child process {child.pid}")
                child.last_ping = time.time()
                child.soft_timeout_reached = False
                ping = ping[:-1] if ping[-1] == "\n" else ping
                ping = ping.rsplit("\n", 1)[-1]
                task, queue = None, None
                if ping != "":
                    try:
                        task_name, queue_name = ping.split(":", 1)
                        task = self.wakaq.tasks[task_name]
                        queue = self.wakaq.queues_by_name.get(queue_name)
                    except ValueError:
                        log.error(f"Unable to unpack message from child process {child.pid}: {ping}")
                child.set_timeouts(self.wakaq, task=task, queue=queue)
            else:
                soft_timeout = child.soft_timeout or self.wakaq.soft_timeout
                hard_timeout = child.hard_timeout or self.wakaq.hard_timeout
                if soft_timeout or hard_timeout:
                    runtime = time.time() - child.last_ping
                    if hard_timeout and runtime > hard_timeout:
                        log.debug(f"child process {child.pid} runtime {runtime} reached hard timeout, sending sigkill")
                        kill(child.pid, signal.SIGKILL)
                    elif not child.soft_timeout_reached and soft_timeout and runtime > soft_timeout:
                        log.debug(f"child process {child.pid} runtime {runtime} reached soft timeout, sending sigquit")
                        child.soft_timeout_reached = True  # prevent raising SoftTimeout twice for same child
                        kill(child.pid, signal.SIGQUIT)

    def _listen_for_broadcast_task(self):
        msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=self.wakaq.wait_timeout)
        if msg:
            payload = msg["data"]
            for child in self.children:
                if child.done:
                    continue
                log.debug(f"run broadcast task: {payload}")
                write_fd(child.broadcastout, f"{payload}\n")
                break

    async def _blocking_dequeue(self):
        if len(self.wakaq.broker_keys) == 0:
            await asyncio.sleep(self.wakaq.wait_timeout)
            return None, None
        data = self.wakaq.broker.blpop(self.wakaq.broker_keys, self.wakaq.wait_timeout)
        if data is None:
            return None, None
        return data[0], deserialize(data[1])

    def _refork_missing_children(self):
        if self._stop_processing:
            return

        if len(self.children) >= self.wakaq.concurrency:
            return

        # postpone fork missing children if using too much ram
        if self.wakaq.max_mem_percent:
            percent_used = mem_usage_percent()
            if percent_used >= self.wakaq.max_mem_percent:
                log.debug(
                    f"postpone forking missing workers... mem usage {percent_used}% is more than max_mem_percent threshold ({self.wakaq.max_mem_percent}%)"
                )
                return

        log.debug("restarting a crashed worker")
        self._fork()
