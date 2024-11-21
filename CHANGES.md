# CHANGES

## 3.0.5 (2024-11-21) [commits](https://github.com/wakatime/wakaq/compare/3.0.4...3.0.5)

#### Bugfix

- Stop waiting for async tasks after exception raised.
[#7](https://github.com/wakatime/wakaq/issues/7)

## 3.0.4 (2024-11-20) [commits](https://github.com/wakatime/wakaq/compare/3.0.3...3.0.4)

#### Bugfix

- Fix raising asyncio.exceptions.CancelledError when async task hits soft timeout.

## 3.0.3 (2024-11-13) [commits](https://github.com/wakatime/wakaq/compare/3.0.2...3.0.3)

#### Bugfix

- Fix UnboundLocalError for async task context
[#15](https://github.com/wakatime/wakaq/pull/15)

## 3.0.2 (2024-11-13) [commits](https://github.com/wakatime/wakaq/compare/3.0.1...3.0.2)

#### Bugfix

- Support Python versions older than 3.11.

## 3.0.1 (2024-11-13) [commits](https://github.com/wakatime/wakaq/compare/3.0.0...3.0.1)

#### Bugfix

- Cancel async tasks when soft_timeout reached.
[#14](https://github.com/wakatime/wakaq/pull/14)
- Fix setting current async task for logging.
[#14](https://github.com/wakatime/wakaq/pull/14)

## 3.0.0 (2024-11-12) [commits](https://github.com/wakatime/wakaq/compare/2.1.25...3.0.0)

#### Breaking

- Custom task wrapper should no longer use an inner function.

#### Feature

- Support for async concurrent tasks on the same worker process.
[#2](https://github.com/wakatime/wakaq/issues/2)

## 2.1.25 (2024-11-07) [commits](https://github.com/wakatime/wakaq/compare/2.1.24...2.1.25)

#### Misc

- Synchronous mode for easier debugging.
[#10](https://github.com/wakatime/wakaq/pull/10)

## 2.1.24 (2024-08-19) [commits](https://github.com/wakatime/wakaq/compare/2.1.23...2.1.24)

#### Bugfix

- Fix number of cores not resolving to integer.

## 2.1.23 (2023-12-16) [commits](https://github.com/wakatime/wakaq/compare/2.1.22...2.1.23)

#### Bugfix

- Fix catching out of memory errors when executing broadcast tasks.

## 2.1.22 (2023-12-16) [commits](https://github.com/wakatime/wakaq/compare/2.1.21...2.1.22)

#### Bugfix

- Fix logging memory errors and treat BrokenPipeError as mem error.

## 2.1.21 (2023-12-15) [commits](https://github.com/wakatime/wakaq/compare/2.1.20...2.1.21)

#### Bugfix

- Log memory errors when to debug level.

## 2.1.20 (2023-12-14) [commits](https://github.com/wakatime/wakaq/compare/2.1.19...2.1.20)

#### Bugfix

- Silently exit child worker(s) when parent process missing.

## 2.1.19 (2023-11-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.18...2.1.19)

#### Bugfix

- Prevent lost tasks when OOM kills parent worker process, improvement.

## 2.1.18 (2023-11-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.17...2.1.18)

#### Bugfix

- Prevent lost tasks when OOM kills parent worker process.

## 2.1.17 (2023-11-10) [commits](https://github.com/wakatime/wakaq/compare/2.1.16...2.1.17)

#### Bugfix

- Fix typo.

## 2.1.16 (2023-11-10) [commits](https://github.com/wakatime/wakaq/compare/2.1.15...2.1.16)

#### Bugfix

- Fix function decorator callbacks after_worker_started, etc.

## 2.1.15 (2023-11-10) [commits](https://github.com/wakatime/wakaq/compare/2.1.14...2.1.15)

#### Bugfix

- Unsubscribe from pubsub while worker processes are waiting to exit.

## 2.1.14 (2023-11-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.13...2.1.14)

#### Bugfix

- Add missing wrapped log.handlers property.

## 2.1.13 (2023-11-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.12...2.1.13)

#### Misc

- Ignore logging errors.

## 2.1.12 (2023-11-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.11...2.1.12)

#### Bugfix

- Only refork crashed workers once per wait_timeout.

## 2.1.11 (2023-11-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.10...2.1.11)

#### Bugfix

- Postpone forking workers at startup if until ram usage below max threshold.

## 2.1.10 (2023-11-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.9...2.1.10)

#### Bugfix

- Postpone forking missing child workers while using too much RAM.

## 2.1.9 (2023-11-08) [commits](https://github.com/wakatime/wakaq/compare/2.1.8...2.1.9)

#### Bugfix

- Prevent UnboundLocalError from using task_name var before assignment.

## 2.1.8 (2023-11-08) [commits](https://github.com/wakatime/wakaq/compare/2.1.7...2.1.8)

#### Bugfix

- Prevent ValueError when unpacking invalid message from child process.

## 2.1.7 (2023-11-08) [commits](https://github.com/wakatime/wakaq/compare/2.1.6...2.1.7)

#### Bugfix

- Prevent ValueError if no worker processes spawned when checking max mem usage.

## 2.1.6 (2023-10-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.5...2.1.6)

#### Misc

- Log time task took until exited when killed because max_mem_percent reached.

## 2.1.5 (2023-10-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.4...2.1.5)

#### Bugfix

- Prevent reset max_mem_reached_at when unrelated child process exits.

## 2.1.4 (2023-10-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.3...2.1.4)

#### Misc

- Less noisy logs when max_mem_percent reached.
- Allow restarting child worker processes more than once per soft timeout.

## 2.1.3 (2023-10-11) [commits](https://github.com/wakatime/wakaq/compare/2.1.2...2.1.3)

#### Bugfix

- Wait for child worker to finish processing current task when max_mem_percent reached.

## 2.1.2 (2023-10-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.1...2.1.2)

#### Misc

- Log mem usage and current task when max_mem_percent threshold reached.

## 2.1.1 (2023-10-09) [commits](https://github.com/wakatime/wakaq/compare/2.1.0...2.1.1)

#### Bugfix

- Fix setting max_mem_percent on WakaQ.

## 2.1.0 (2023-09-22) [commits](https://github.com/wakatime/wakaq/compare/2.0.2...2.1.0)

#### Feature

- Include number of workers connected when inspecting queues.
- Log queue params on startup.

#### Misc

- Improve docs in readme.

## 2.0.2 (2022-12-09) [commits](https://github.com/wakatime/wakaq/compare/2.0.1...2.0.2)

#### Bugfix

- Make sure to catch system exceptions to prevent worker infinite loops.

## 2.0.1 (2022-12-09) [commits](https://github.com/wakatime/wakaq/compare/2.0.0...2.0.1)

#### Bugfix

- Always catch SoftTimeout even when nested in exception context.

## 2.0.0 (2022-11-18) [commits](https://github.com/wakatime/wakaq/compare/1.2.0...2.0.0)

#### Feature

- Support bytes in task arguments.

#### Misc

- Tasks always receive datetimes in UTC without tzinfo.

## 1.3.0 (2022-10-05) [commits](https://github.com/wakatime/wakaq/compare/1.2.1...1.3.0)

#### Feature

- Add username, password, and db redis connection options.
[#6](https://github.com/wakatime/wakaq/issues/6)

## 1.2.1 (2022-09-20) [commits](https://github.com/wakatime/wakaq/compare/1.2.0...1.2.1)

#### Bugfix

- Prevent reading from Redis when no queues defined.

#### Misc

- Improve error message when app path not WakaQ instance.

## 1.2.0 (2022-09-17) [commits](https://github.com/wakatime/wakaq/compare/1.1.8...1.2.0)

#### Feature

- Util functions to peek at tasks in queues.

## 1.1.8 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.7...1.1.8)

#### Bugfix

- Ignore SoftTimeout in child when not processing any task.

## 1.1.7 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.6...1.1.7)

#### Bugfix

- Allow custom timeouts defined on task decorator.

## 1.1.6 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.5...1.1.6)

#### Bugfix

- All timeouts should accept timedelta or int seconds.

## 1.1.5 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.4...1.1.5)

#### Bugfix

- Fix typo.

## 1.1.4 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.3...1.1.4)

#### Bugfix

- Fix setting task and queue on child from ping.

## 1.1.3 (2022-09-15) [commits](https://github.com/wakatime/wakaq/compare/1.1.2...1.1.3)

#### Bugfix

- Fix sending task and queue to parent process.

## 1.1.2 (2022-09-14) [commits](https://github.com/wakatime/wakaq/compare/1.1.1...1.1.2)

#### Bugfix

- Fix getattr.

## 1.1.1 (2022-09-14) [commits](https://github.com/wakatime/wakaq/compare/1.1.0...1.1.1)

#### Bugfix

- Add missing child timeout class attributes.

## 1.1.0 (2022-09-14) [commits](https://github.com/wakatime/wakaq/compare/1.0.6...1.1.0)

#### Feature

- Ability to overwrite timeouts per task or queue.

## 1.0.6 (2022-09-08) [commits](https://github.com/wakatime/wakaq/compare/1.0.5...1.0.6)

#### Bugfix

- Prevent unknown task crashing worker process.

## 1.0.5 (2022-09-08) [commits](https://github.com/wakatime/wakaq/compare/1.0.4...1.0.5)

#### Bugfix

- Make sure logging has current task set.

## 1.0.4 (2022-09-07) [commits](https://github.com/wakatime/wakaq/compare/1.0.3...1.0.4)

#### Bugfix

- Fix auto retrying tasks on soft timeout.

## 1.0.3 (2022-09-07) [commits](https://github.com/wakatime/wakaq/compare/1.0.2...1.0.3)

#### Bugfix

- Ignore SoftTimeout when waiting on BLPOP from Redis list.

## 1.0.2 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/1.0.1...1.0.2)

#### Bugfix

- Ping parent before blocking dequeue in case wait timeout is near soft timeout.

## 1.0.1 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/1.0.0...1.0.1)

#### Bugfix

- All logger vars should be strings.

## 1.0.0 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.11...1.0.0)

- First major release.

## 0.0.11 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.10...0.0.11)

#### Feature

- Add task payload to logger variables.

## 0.0.10 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.9...0.0.10)

#### Bugfix

- Prevent logging error from crashing parent process.

## 0.0.9 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.8...0.0.9)

#### Bugfix

- Prevent parent process looping forever while stopping children.

## 0.0.8 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.7...0.0.8)

#### Bugfix

- Prevent parent process crash leaving zombie child processes.

## 0.0.7 (2022-09-05) [commits](https://github.com/wakatime/wakaq/compare/0.0.6...0.0.7)

#### Feature

- Ability to retry tasks when they soft timeout.

#### Bugfix

- Ping parent process at start of task to make sure soft timeout timer is reset.

## 0.0.6 (2022-09-03) [commits](https://github.com/wakatime/wakaq/compare/0.0.5...0.0.6)

#### Feature

- Implement exclude_queues option.

#### Bugfix

- Prevent parent process crash if write to child broadcast pipe fails.

## 0.0.5 (2022-09-01) [commits](https://github.com/wakatime/wakaq/compare/0.0.4...0.0.5)

#### Bugfix

- Run broadcast tasks once per worker instead of randomly.

## 0.0.4 (2022-09-01) [commits](https://github.com/wakatime/wakaq/compare/0.0.3...0.0.4)

#### Feature

- Allow defining schedules as tuple of cron and task name, without args.

## 0.0.3 (2022-09-01) [commits](https://github.com/wakatime/wakaq/compare/0.0.2...0.0.3)

#### Bugfix

- Prevent worker process crashing on any exception.

#### Feature

- Ability to wrap tasks with custom dectorator function.

## 0.0.2 (2022-09-01) [commits](https://github.com/wakatime/wakaq/compare/0.0.1...0.0.2)

#### Breaking

- Run in foreground by default.
- Separate log files and levels for worker and scheduler.
- Decorators for after worker started, before task, and after task callbacks.

#### Bufix

- Keep processing tasks after SoftTimeout.
- Scheduler should sleep until scheduled time.

## 0.0.1 (2022-08-30)

- Initial release.
