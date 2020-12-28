# Development Guide

## Run linter and tests

Run the following commands before committing your codes:
```
$ make lint
$ make test
```

## Coding Style
### Safe Codes
Avoid using `unsafe` and calls that could crash like `unwrap`, `unsafe_pinned`.
Use `pin-project` instead of `pin-utils`.

But you can still use `expect` for some cases only you have to:

- (1) Data manipulation in memory broker
- ~~(2) Locks~~ (Use poison-free locks in parking_lot)

### Employ dependency injection for better unit tests
Dependency injection not only make it much easier to write unit tests
but also makes more modular codes.

Now the whole server proxy could run just inside pure memory
and can run some tests towards the proxy without creating a real connection.
