---
paths:
  - "packages/markets-pipeline/src/markets_pipeline/sources/**/*.py"
  - "packages/markets-core/src/markets_core/interfaces/source.py"
  - "packages/markets-core/tests/contract/test_source_contract.py"
---

# Source implementations — rules

You are working on code that implements the `Source` protocol. These rules apply specifically to this layer and override any conflicting general guidance.

## The Source protocol is the contract

Any file in `packages/markets-pipeline/src/markets_pipeline/sources/` defines a concrete `Source` implementation. Every such implementation must:

1. **Conform to the `Source` protocol** from `markets_core.interfaces.source`. No subclassing; use structural typing.
2. **Pass every test in `test_source_contract.py`.** Not negotiable. If a new assertion is added to the contract, all existing implementations must pass it before the contract change merges.
3. **Be idempotent and side-effect-free.** Calling `fetch()` with the same arguments produces the same result. No writes to disk, no calls to other sources, no hidden state.

## Behavioral invariants every Source must satisfy

These are asserted by the contract tests but they're worth stating here because they're easy to violate subtly.

- `fetch()` returns `Iterator[Observation]` — streaming, not a list. Memory safety on long date ranges.
- Every yielded `Observation` satisfies `observation_date <= as_of_date`. No exceptions.
- If the source doesn't provide vintage information (yfinance, Treasury), set `as_of_date = today()` at the moment of ingest.
- If the source does provide vintage information (FRED's ALFRED), respect it — use the provider's vintage date as `as_of_date`.
- Unknown series must raise `SeriesNotFoundError`, not return empty iterator.
- Transient network failures must raise `DataSourceError` (so Airflow retries).
- Permanent failures (invalid API key, schema broken) must raise `DataSourceError` (still), but the DAG-level retry policy will eventually surface it.

## Error wrapping — specific to this layer

At the source boundary, **every** provider-native exception gets wrapped:

```python
try:
    response = self._client.get_series(...)
except requests.HTTPError as e:
    if e.response.status_code == 404:
        raise SeriesNotFoundError(...) from e
    raise DataSourceError(...) from e
except (requests.ConnectionError, requests.Timeout) as e:
    raise DataSourceError(f"transient network error: {e}", source=self.source_name) from e
```

Downstream code handles `DataSourceError` and `SeriesNotFoundError`; it should never have to know about `requests`, `yfinance`'s internals, or FRED's specific response formats.

## What belongs inside a Source implementation

- Provider-specific authentication (API keys, user agents).
- Provider-specific request formatting and response parsing.
- Retries or rate-limit backoff *only* if they're built into the provider's SDK. Otherwise, Airflow handles retries.
- Normalization of provider-native shapes into `Observation`.

## What does NOT belong inside a Source implementation

- Storing data. That's `Store`'s job.
- Computing derived values (z-scores, correlations). That's `Transform`'s job.
- Scheduling. That's Airflow's job.
- Caching. If the source is slow, Airflow's task-level retry and the `as_of_date` deduplication handle it.
- Deciding which series to fetch. That's the DAG's job — the DAG reads from config and tells the source what to fetch.

## Required methods

Every `Source` must implement:

- `fetch(series_id, start, end, as_of=None) -> Iterator[Observation]`
- `health_check() -> HealthStatus` — cheap; verifies reachability and auth but fetches no data.
- `metadata(series_id) -> SeriesMetadata` — describes a series (title, units, frequency).

Do not add methods beyond these. If a new method seems necessary, it either belongs in a different protocol or indicates the existing protocol needs updating — discuss before adding.

## Testing a new Source

Before merging a new Source implementation:

1. Add the implementation in `sources/<provider>.py`.
2. Add tests in `packages/markets-pipeline/tests/sources/test_<provider>_source.py`.
3. The test file subclasses `SourceContract` from `markets_core.tests.contract` and overrides the `source` and `known_series_id` fixtures.
4. Add any provider-specific tests (e.g., ALFRED vintage handling for FRED) in a separate class in the same file.
5. Run `make test-contract` to verify the contract passes.
6. Add an integration test marked `@pytest.mark.integration` that actually hits the provider's API.

## Common pitfalls in this layer

- **Mixing up `observation_date` and `as_of_date`.** If you're ever unsure which is which: `observation_date` is what the data is *about* (the calendar date of the datapoint); `as_of_date` is when we *knew* it.
- **Returning a list instead of an iterator.** Triggers a memory spike on long backfills. Always `yield`, never `return [...]`.
- **Silently returning empty results for invalid series.** This hides bugs. Raise `SeriesNotFoundError`.
- **Catching exceptions too broadly.** `except Exception:` hides real bugs. Catch specific exception types.
- **Reading environment variables directly.** Inject configuration via constructor arguments. Env-var reading happens at the composition root, not inside the class.
