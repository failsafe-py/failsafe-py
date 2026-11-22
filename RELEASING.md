# Releasing

## Versioning

[Semantic versioning](https://semver.org/), with the pre-1.0 caveat that breaking
changes are expected and only need a minor bump.

| Change | Before 1.0 | After 1.0 |
|---|---|---|
| Breaking API change | minor — `0.1.0` → `0.2.0` | major |
| New feature, backwards compatible | minor | minor |
| Bug fix, no API change | patch | patch |

A breaking change is anything that can turn working code into broken code: a removed
or renamed public name, a parameter that changes meaning or position, a narrowed
accepted type, a different exception, or a behavioural change a caller could be
relying on. Reordering keyword-only parameters is not breaking; renaming one is.

Until 1.0 these are acceptable when they improve the API — see
[CONTRIBUTING.md](CONTRIBUTING.md) — but they must be called out.

## Release notes

Every release lists breaking changes **first**, under their own heading, each with
what to write instead. A reader skimming the notes should learn in one screen whether
the upgrade will cost them anything.

```markdown
### Breaking

- `CachePolicy(key=)` is no longer required. Policies relying on positional
  construction must pass it by keyword.

### Added
### Fixed
```

A breaking commit marks itself with `!` after the type: `feat!: …`,
`refactor(retry)!: …`. Add a `BREAKING CHANGE:` footer as well when the migration
needs more than a subject line. Either way `git log` is the source for that first
section, and a release containing a `!` commit with no breaking section in its notes
means one of the two is wrong.

## Cutting a release

1. Update `version` in `pyproject.toml`.
2. Commit it: `chore: release 0.2.0`.
3. Tag it: `git tag v0.2.0 && git push --tags`.

The tag triggers `.github/workflows/release.yml`, which runs the full gates, verifies
the tag matches the packaged version, builds, and publishes to PyPI through trusted
publishing. No token is stored anywhere.

A mismatched tag fails the build before anything is published, so a wrong tag costs a
deletion rather than a yanked release.

## After publishing

Read the Docs builds the tag automatically, but **new versions arrive inactive** —
activate `v0.2.0` in the project's Versions tab for it to be served. `stable` then
follows the highest semver tag on its own, and `latest` always tracks `main`.
