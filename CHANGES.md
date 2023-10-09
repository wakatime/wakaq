# CHANGES

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
