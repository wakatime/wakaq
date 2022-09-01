# CHANGES

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
