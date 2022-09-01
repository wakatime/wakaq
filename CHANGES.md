# CHANGES

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
