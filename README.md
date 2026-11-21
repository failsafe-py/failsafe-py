# failsafe-py

[![License](http://img.shields.io/:license-mit-brightgreen.svg)](https://opensource.org/licenses/MIT)

failsafe-py is a library for building resilient, fault tolerant Python applications. It works by
wrapping functions with one or more resilience policies, which can be combined and composed as
needed. Policies include:

- Failure handling: Retry, Fallback
- Load limiting: Circuit Breaker, Adaptive Limiter, Adaptive Throttler, Bulkhead, Rate Limiter, Cache
- Time limiting: Timeout, Hedge

A Python port of [failsafe-go](https://github.com/failsafe-go/failsafe-go). Coming from Go or
Java? See [PORT.md](PORT.md).

> **Status:** early development. Nothing is released yet.

## License

&copy; 2026-present Nicolas Dumazet and contributors. Released under the
[MIT license](LICENSE).
