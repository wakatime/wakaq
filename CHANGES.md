# CHANGES

## 0.0.9 (2022-09-05)

#### Bugfix

- Prevent parent process looping forever while stopping children.

## 0.0.8 (2022-09-05)

#### Bugfix

- Prevent parent process crash leaving zombie child processes.

## 0.0.7 (2022-09-05)

#### Feature

- Ability to retry tasks when they soft timeout.

#### Bugfix

- Ping parent process at start of task to make sure soft timeout timer is reset.

## 0.0.6 (2022-09-03)

#### Feature

- Implement exclude_queues option.

#### Bugfix

- Prevent parent process crash if write to child broadcast pipe fails.

## 0.0.5 (2022-09-01)

#### Bugfix

- Run broadcast tasks once per worker instead of randomly.

## 0.0.4 (2022-09-01)

#### Feature

- Allow defining schedules as tuple of cron and task name, without args.

## 0.0.3 (2022-09-01)

#### Bugfix

- Prevent worker process crashing on any exception.

#### Feature

- Ability to wrap tasks with custom dectorator function.

## 0.0.2 (2022-09-01)

#### Breaking

- Run in foreground by default.
- Separate log files and levels for worker and scheduler.
- Decorators for after worker started, before task, and after task callbacks.

#### Bufix

- Keep processing tasks after SoftTimeout.
- Scheduler should sleep until scheduled time.

## 0.0.1 (2022-08-30)

- Initial release.
