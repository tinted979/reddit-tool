# 0008. Import subreddit histories from a local lake of the monthly dumps

**Status:** Accepted (recorded 2026-09-26). Amends 0005's Context: the API is no longer the only source of a chosen subreddit's history.

## Context

Since the per-subreddit torrents came down in July 2026, a big subreddit's history came only from Arctic Shift's download tool, which pages the API. That's hours of requests to a free, shared service (0002): r/Socialism_101's 687k items took about 6,900 requests.

Arctic Shift still publishes all of Reddit as one torrent a month (`RC_`/`RS_YYYY-MM.zst`, about 75 GB a month now, 4.4 TB for 2005 to 2025). Those files don't fit a hosted runner (0005). They're zstd with a 2 GB window, which DuckDB can't read directly. The owner wants to keep all of the data, text included, so nothing is ever downloaded twice and an Arctic Shift–like tool stays possible later.

## Decision

- **Keep the dumps:** the owner keeps them on a local drive (`raw/`), never changed.
- **Convert each month:** `tools/reddit_lake.py` turns each month into Parquet parts sorted by subreddit then time (`lake/`, text included). Its columns are versioned by `SCHEMA`, and the whole lake can be rebuilt from `raw/`.
- **Import from the lake:** a subreddit's first import is extracted from the lake in the download tool's JSONL shape and built with `build_dumps.py` as before.
- **Complete histories only:** `extract` refuses a lake with a month missing. It also refuses a subreddit with items in the lake's first month, unless `--allow-partial-history` says it began then, because a build records where it ends, not where it starts.
- **Local only:** the lake runs on the owner's machine. It's never in CI or the sync, and never published; the public archive still holds no text (0005).
- **From the lake's end on:** the sync takes each subreddit on from where the lake ends.
- **A new dependency for the Python tools:** they may use `zstandard`, pinned below its next major version, next to `duckdb`. ADR 0001 is unchanged, since it governs only the page.

## Consequences

- **Cheaper imports:** they take minutes and send no requests to Arctic Shift. Bigger subreddits become affordable to import, though the sync's cost per run still limits which ones suit the archive (0005).
- **The owner's upkeep:** terabytes of disk, and a monthly routine of downloading, then running `update`.
- **Text for later:** the lake keeps text locally, so a future text feature has a source without re-pulling from the API. Publishing any of it would need its own decision.
- **Revisit if** Arctic Shift stops the monthly torrents or changes their format, or the lake ever needs to run somewhere other than the owner's machine.
