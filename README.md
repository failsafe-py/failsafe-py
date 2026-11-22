# failsafe-py

[![License](http://img.shields.io/:license-mit-brightgreen.svg)](https://opensource.org/licenses/MIT)

failsafe-py is a library for building resilient, fault tolerant Python applications. It works by
wrapping functions with one or more resilience policies, which can be combined and composed as
needed. Policies include:

- Failure handling: Retry, Fallback
- Load limiting: Circuit Breaker, Adaptive Limiter, Adaptive Throttler, Bulkhead, Rate Limiter, Cache
- Time limiting: Timeout, Hedge

A Python port of [failsafe-go](https://github.com/failsafe-go/failsafe-go). See
[PORT.md](PORT.md) for divergences.

## Project status

This is mostly agentic port of failsafe-go alongside its website, because I
needed a similar library at work in Python (Google-external equivalent of
"extensible stubs / canonical stubs"). It was fast to do, "only" costing me
about a weekend trying to improve the agent's writing / API choices.

Some disclaimers:

* Agents are ... agents. Their prose is annoying. Their tests are not that
  good. They sometimes make mistakes [...].
* I'm not that attached to the particular implementation.
* This is a v0.1.0. I would want, welcome, and encourage specific API design
  feedback (via Github bugs). I am happy to consider breaking API changes in
  future, in particular to have more "Python friendly" APIs if I missed
  something whilst guiding my agent.

I would aim for a v1.0 release before end of 2026, hopefully after feedback
from the community.

See [CONTRIBUTING.md](CONTRIBUTING.md) to file feedback or work on the code.

## License

&copy; 2026-present Nicolas Dumazet and contributors. Released under the
[MIT license](LICENSE).
