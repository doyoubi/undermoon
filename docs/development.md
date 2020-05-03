# Development Guide

## Use Linter
Install linters:
```
$ make install-linters
```

Then run the following commands before commit your codes:
```
$ make lint
$ make test
```

## Coding Style
### Safe Codes
Avoid using `unsafe` and calls that could crash like `unwrap`, `unsafe_pinned`.
Use `pin-project` instead of `pin-utils`.

But you can still use `expect` for some cases:

- (1) Data manipulation in memory broker
- (2) Locks

### Employ dependency injection for better unit tests
Dependency injection not only make it much easier to write unit tests
but also makes more modular codes.

Now the whole server proxy could run just inside pure memory
and can run some tests towards the proxy without creating a real connection.
