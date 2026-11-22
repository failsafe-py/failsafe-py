# AGENTS.md

## Commits

Conventional commits. Terse descriptions — a subject line, nothing more unless the
*why* is genuinely non-obvious.

```
<type>(<optional scope>): <description>
```

- `<description>`: imperative, lower case, no trailing period, ≤ 50 chars.
- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `build`, `ci`.
- Scope is the module or artifact (`retry`, `core`, `httpx`), omitted when it adds nothing.
- Breaking changes take a `!` after the type or scope: `feat!:`, `refactor(retry)!:`.
- Body only for surprising decisions or a `BREAKING CHANGE:` footer. No restating the diff.

Good:

```
feat(retry): add backoff jitter
fix(core): release bulkhead permit on abandon
docs: add PLAN.md
chore: add pyproject
```

Bad: `Updated some files`, `feat: this commit adds a new retry policy which
allows users to configure the maximum number of attempts`.

## Tooling

- No user-wide installs. Ephemeral `uv` environments or containers only — never
  `pip install` / `gem install` into the system or home directory.
- `uv run` for anything that needs a dependency.
- `uv.lock` is committed — it pins dev tooling only, not what consumers install. Use `uv sync`
  to honour it.

## Code

- Python 3.11+, stdlib only in the core. Optional extras at integration boundaries.
- `ruff` + `mypy --strict` clean; `py.typed` ships.
- Docstrings on every public symbol — they are the input to the API reference.
- Never swallow `BaseException`. `CancelledError` is not a retryable failure.
