# Reporting Bugs

Bug reports are welcome and appreciated. When filing an issue, please include a small code snippet that demonstrates the bug if you can, else include a good description of how to reproduce the bug.

## Contributing Bug Fixes

Pull requests for bugs related to existing features are always welcome.

# Features

## New failsafe features: probably not

Overall the goal of this project is to follow `failsafe` (Java) and
`failsafe-go` (Go) implementations and be a strict subset of those projects. I
do not wish to deviate too much, and am not really planning to add features
which are not in one of those projects, please make your case in what I
consider the "upstream" projects first (CC'ing Python maintainers is welcome!).

If there's an agreement to support this new feature in Java or Go, I'm happy to
look into implementations here, or review Pull Requests.

## Python integration

That being said I expect that my Python integration of those projects may not
be perfect so I will happily consider feature requests for "The Python API of
this feature should do X / Y / Z" as long as this matches the behavior of
failsafe.

# Contributing docstrings, Documentation or Website Fixes

Fixes to the failsafe-py docstrings, documentation or website are welcome. Just
clone the [website repo](https://github.com/failsafe-py/failsafe-py.github.io)
and feel free to submit a pull request.

# Working on the Code

[uv](https://docs.astral.sh/uv/) manages the environment:

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy
```

All four must pass. CI runs them on every
[supported Python version](https://devguide.python.org/versions/).
