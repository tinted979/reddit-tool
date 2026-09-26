# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1,<2", "zstandard>=0.23,<1"]
# ///
"""A local copy of Arctic Shift's monthly Reddit dumps that's quick to query (the lake), and
the per-subreddit files the archive is built from.

Under ROOT (--root):
  raw/   the monthly dumps as downloaded: RC_YYYY-MM.zst (comments) and RS_YYYY-MM.zst (posts),
         in any subfolders (the combined torrent's reddit/comments/, a monthly one's
         comments/). Never changed.
  lake/  comments/YYYY-MM/ and posts/YYYY-MM/, each a folder of Parquet parts sorted by
         subreddit then time, and months.json, what's been converted.

  uv run tools/reddit_lake.py status  --root F:/reddit [--releases]
  uv run tools/reddit_lake.py update  --root F:/reddit [--month 2026-08] [--chunk-mb 2048] [--memory-limit 8GB]
  uv run tools/reddit_lake.py extract --root F:/reddit --subreddits Socialism_101,socialism --out F:/new_dumps

`update` converts every downloaded month that isn't in the lake yet. It's safe to run again:
finished months are skipped, and one cut off partway is redone, since a month only appears
once all its parts are written. A file that ends partway (still downloading) or doesn't
decompress is reported and left out, and the rest go in.

`extract` writes r_<Name>_posts.jsonl and r_<Name>_comments.jsonl for each subreddit: the
shape Arctic Shift's download tool gives, which tools/build_dumps.py reads. It refuses a lake
with a month missing between its first and last, so no gap reaches the archive. It also
refuses a subreddit with items in the lake's first month, whose history may start earlier (a
build records where it ends, not where it starts), unless --allow-partial-history says it began
then.

`status` says what's in the lake, what's downloaded but not converted, what's still
downloading, and with --releases, which released months aren't downloaded, with magnet links
(from Arctic Shift's download page on GitHub).

The dumps are zstd with a 2 GB window, which DuckDB can't read, so this decompresses them in a
thread, cutting the stream into chunks of about --chunk-mb (kept in lake/.tmp) that DuckDB
turns into sorted Parquet parts. The lake keeps the fields worth querying, text included, and
drops ones the rest rebuild (permalink, name, *_html), the fetching account's own view, and
fields that are always empty; raw/ keeps everything, so the lake can be rebuilt with other
columns (bump SCHEMA).
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import urllib.request
from pathlib import Path

import duckdb
import zstandard

SCHEMA = 1
FIRST_MONTH = "2005-06"  # the dumps' first month of Reddit
MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
RAW_FILE = re.compile(r"^(RC|RS)_(\d{4}-(?:0[1-9]|1[0-2]))\.zst$")
IN_PROGRESS = re.compile(r"^(RC|RS)_\d{4}-\d{2}\.zst\..+$")  # a torrent client's .!qB, .part
KIND = {"RC": "comments", "RS": "posts"}
FILE_PREFIX = {"comments": "RC", "posts": "RS"}
SUBREDDIT = re.compile(r"^[A-Za-z0-9_]{2,21}$")
MEMORY = re.compile(r"^\d+(\.\d+)?\s*(KB|MB|GB|TB)$", re.I)
MAX_WINDOW = 2**31
READ_BYTES = 1 << 20
# Output buffer per decompress call: the default (128 KB) decompresses at about a sixth of the speed.
WRITE_BYTES = 1 << 22
CHUNK_BYTES = 2048 << 20
ROW_GROUP = 100_000
RELEASES_URL = "https://raw.githubusercontent.com/ArthurHeitmann/arctic_shift/master/download_links.md"
TRACKERS = "&tr=https%3A%2F%2Facademictorrents.com%2Fannounce.php&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"


# The lake's columns: each an SQL expression over the dump's fields, which are read as text (a
# field's type varies over the years) and cast here, with the fields it reads. `edited` is null
# when not edited, else when (0: edited, time unknown). Every row keeps Arctic Shift's `_meta`
# (the deletions and edits it saw).
def _text(name: str) -> tuple[str, tuple[str, ...]]:
    return name, (name,)


def _int(name: str) -> tuple[str, tuple[str, ...]]:
    return f"TRY_CAST(TRY_CAST({name} AS DOUBLE) AS BIGINT) AS {name}", (name,)


def _bool(name: str) -> tuple[str, tuple[str, ...]]:
    return f"TRY_CAST({name} AS BOOLEAN) AS {name}", (name,)


_KEY = ("lower(subreddit) AS subreddit_key", ("subreddit",))
_EDITED = ("CASE WHEN edited IS NULL OR edited = 'false' THEN NULL WHEN edited = 'true' THEN 0"
           " ELSE TRY_CAST(TRY_CAST(edited AS DOUBLE) AS BIGINT) END AS edited", ("edited",))
_RETRIEVED = ("TRY_CAST(TRY_CAST(coalesce(retrieved_on, retrieved_utc) AS DOUBLE) AS BIGINT) AS retrieved_on",
              ("retrieved_on", "retrieved_utc"))
_AUTHOR = [_text("author"), _text("author_fullname"), _int("author_created_utc"), _text("author_flair_text")]
_TIMES = [_int("created_utc"), _EDITED, _RETRIEVED]
_STATE = [_text("distinguished"), _bool("stickied"), _bool("locked"), _bool("archived"), _text("subreddit_type")]
_AWARDS = [_int("gilded"), _int("total_awards_received")]
COLUMNS = {
    "comments": [
        _KEY, _text("id"), _text("subreddit"), _text("subreddit_id"), _text("link_id"), _text("parent_id"),
        *_AUTHOR, _bool("is_submitter"), *_TIMES,
        _text("body"), _int("score"), _int("controversiality"), *_AWARDS,
        *_STATE, _text("collapsed_reason_code"), _text("_meta"),
    ],
    "posts": [
        _KEY, _text("id"), _text("subreddit"), _text("subreddit_id"),
        *_AUTHOR, *_TIMES,
        _text("title"), _text("selftext"), _text("url"), _text("domain"), _bool("is_self"), _bool("is_video"),
        _bool("over_18"), _bool("spoiler"), _text("post_hint"), _text("crosspost_parent"),
        _int("score"), ("TRY_CAST(upvote_ratio AS DOUBLE) AS upvote_ratio", ("upvote_ratio",)), _int("num_comments"),
        _int("num_crossposts"), *_AWARDS,
        _text("link_flair_text"), *_STATE, _text("removed_by_category"), _bool("quarantine"),
        _int("subreddit_subscribers"), _text("_meta"),
    ],
}


def _sources(kind: str) -> dict[str, str]:
    """The dump fields COLUMNS reads, and how read_ndjson reads each: as text, but _meta as JSON."""
    names = sorted({field for _sql, fields in COLUMNS[kind] for field in fields})
    return {n: ("JSON" if n == "_meta" else "VARCHAR") for n in names}


class Incomplete(Exception):
    """A dump that ends partway through (most likely still downloading)."""


class Stopped(Exception):
    """The main thread gave up on a conversion; the decompressing thread stops."""


def posix(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def month_of(t: int) -> str:
    return time.strftime("%Y-%m", time.gmtime(t))


def month_range(first: str, last: str) -> list[str]:
    y, m = map(int, first.split("-"))
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def spans(months: list[str]) -> str:
    """"2026-01 to 2026-03, 2026-05" for a sorted list of months."""
    runs: list[list[str]] = []
    for m in months:
        if runs and month_range(runs[-1][-1], m)[1:2] == [m]:
            runs[-1].append(m)
        else:
            runs.append([m])
    return ", ".join(r[0] if len(r) == 1 else f"{r[0]} to {r[-1]}" for r in runs)


def plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


# The lake's record ----------------------------------------------------------------------------

def load_state(lake: Path) -> dict:
    path = lake / "months.json"
    if not path.exists():
        return {"format": 1, "schema": SCHEMA, "months": {}}
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("schema") != SCHEMA:
        raise SystemExit(f"{path} is from lake schema {state.get('schema')}, and this tool writes {SCHEMA}: "
                         "rebuild the lake (move lake/ aside and run update)")
    return state


def save_state(lake: Path, state: dict) -> None:
    tmp = lake / "months.json.tmp"
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, lake / "months.json")


def scan_raw(raw: Path) -> tuple[dict, dict, list]:
    """{(month, kind): path} for each finished dump, {(month, kind): [paths]} for any held twice
    (left out), and the files a torrent client is still writing."""
    found: dict = {}
    twice: dict = {}
    downloading = []
    if raw.is_dir():
        for path in sorted(raw.rglob("*")):
            if not path.is_file():
                continue
            m = RAW_FILE.match(path.name)
            if m:
                key = (m[2], KIND[m[1]])
                if key in found or key in twice:
                    twice.setdefault(key, [found.pop(key)] if key in found else []).append(path)
                else:
                    found[key] = path
            elif IN_PROGRESS.match(path.name):
                downloading.append(path)
    return found, twice, downloading


# update --------------------------------------------------------------------------------------

def decompressed(src: Path):
    """The dump's bytes, decompressed as they're read. Raises Incomplete if it ends partway
    through a frame, and zstandard.ZstdError if it isn't valid zstd."""
    dctx = zstandard.ZstdDecompressor(max_window_size=MAX_WINDOW)
    dobj = dctx.decompressobj(write_size=WRITE_BYTES)
    fed = False
    frames = 0
    with open(src, "rb") as f:
        while data := f.read(READ_BYTES):
            while data:
                fed = True
                out = dobj.decompress(data)
                if out:
                    yield out
                if dobj.eof:
                    frames += 1
                    data = dobj.unused_data
                    dobj = dctx.decompressobj(write_size=WRITE_BYTES)
                    fed = False
                else:
                    data = b""
    if fed or not frames:
        raise Incomplete(f"{src.name} ends partway through: is it still downloading?")


def split(src: Path, tmp: Path, chunk_bytes: int, put) -> int:
    """Writes the decompressed dump to chunk files of about `chunk_bytes`, each ending at a line
    end, handing each to `put` when it's full. Returns how many lines there were."""
    lines = 0
    n = 0
    size = 0
    last = b"\n"
    path = tmp / f"chunk-{n:05d}.ndjson"
    out = open(path, "wb")
    try:
        for block in decompressed(src):
            lines += block.count(b"\n")
            last = block[-1:]
            view = memoryview(block)
            pos = 0
            # Close a chunk at the first line end past each chunk_bytes, however many a block holds.
            while size + len(block) - pos >= chunk_bytes:
                cut = block.find(b"\n", pos + max(0, chunk_bytes - size - 1))
                if cut < 0:
                    break
                out.write(view[pos:cut + 1])
                out.close()
                put(path)
                n += 1
                path = tmp / f"chunk-{n:05d}.ndjson"
                out = open(path, "wb")
                size = 0
                pos = cut + 1
            out.write(view[pos:])
            size += len(block) - pos
        if size and last != b"\n":
            lines += 1
    finally:
        out.close()
    if size:
        put(path)
    else:
        path.unlink()
    return lines


def to_parquet(con: duckdb.DuckDBPyConnection, chunk: Path, kind: str, part: Path) -> int:
    """One chunk of a dump as a Parquet part sorted by subreddit then time. Returns its rows.
    Lines that aren't JSON come through ignore_errors as empty rows, and every real item has an
    id, so rows without one are skipped (the caller counts them as malformed)."""
    columns = "{" + ", ".join(f"'{k}': '{v}'" for k, v in _sources(kind).items()) + "}"
    return con.execute(f"""
        COPY (
          SELECT {", ".join(sql for sql, _fields in COLUMNS[kind])}
          FROM read_ndjson('{posix(chunk)}', columns = {columns}, ignore_errors = true,
                           maximum_object_size = 268435456)
          WHERE id IS NOT NULL
          ORDER BY subreddit_key, created_utc
        ) TO '{posix(part)}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {ROW_GROUP})
    """).fetchone()[0]


def convert(con, src: Path, kind: str, month: str, lake: Path, chunk_bytes: int, log) -> dict:
    """One dump into lake/<kind>/<month>/, written as <month>.partial and renamed when done."""
    tmp = lake / ".tmp" / f"{kind}-{month}"
    partial = lake / kind / f"{month}.partial"
    final = lake / kind / month
    for d in (tmp, partial, final):  # `final` here was never recorded: cut off last time
        shutil.rmtree(d, ignore_errors=True)
    tmp.mkdir(parents=True)
    partial.mkdir(parents=True)
    chunks: queue.Queue = queue.Queue(maxsize=1)
    stop = threading.Event()
    result: dict = {}

    def put(item) -> None:
        # Raises Stopped once the main thread has given up, so the rest isn't decompressed.
        while True:
            if stop.is_set():
                raise Stopped
            try:
                chunks.put(item, timeout=0.5)
                return
            except queue.Full:
                continue

    def produce() -> None:
        try:
            result["lines"] = split(src, tmp, chunk_bytes, put)
        except Stopped:
            return
        except BaseException as err:  # handed to the main thread
            result["error"] = err
        try:
            put(None)
        except Stopped:
            pass

    started = time.monotonic()
    worker = threading.Thread(target=produce, daemon=True)
    worker.start()
    rows = parts = 0
    try:
        while (chunk := chunks.get()) is not None:
            rows += to_parquet(con, chunk, kind, partial / f"part-{parts:05d}.parquet")
            chunk.unlink()
            parts += 1
            log(f"  {src.name}: part {parts}, {rows:,} rows, {time.monotonic() - started:.0f} s")
    except BaseException:
        stop.set()
        raise
    finally:
        worker.join()
        shutil.rmtree(tmp, ignore_errors=True)
    if "error" in result:
        raise result["error"]
    os.replace(partial, final)
    skipped = result["lines"] - rows
    return {"source": src.name, "bytes": src.stat().st_size, "lines": result["lines"], "rows": rows,
            "skipped": skipped, "parts": parts, "seconds": round(time.monotonic() - started),
            "converted_utc": int(time.time())}


def connect(lake: Path, memory_limit: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    spill = lake / ".tmp" / "duckdb"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{posix(spill)}'")  # on the lake's drive, not the system's
    con.execute("SET preserve_insertion_order = false")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    return con


def update(root: Path, *, log=print, month: str | None = None, chunk_bytes: int = CHUNK_BYTES,
           memory_limit: str | None = None) -> int:
    """Converts each downloaded dump that isn't in the lake yet. Returns 0, or 1 if any couldn't be."""
    lake = root / "lake"
    lake.mkdir(parents=True, exist_ok=True)
    state = load_state(lake)
    for leftover in [*lake.glob("*/*.partial"), lake / ".tmp"]:
        shutil.rmtree(leftover, ignore_errors=True)
    found, twice, _ = scan_raw(root / "raw")
    failed = False
    for (m, kind), paths in sorted(twice.items()):
        if month in (None, m):
            log(f"error: two copies of {FILE_PREFIX[kind]}_{m}.zst ({', '.join(str(p) for p in paths)}): keep one")
            failed = True
    todo = [(m, kind, path) for (m, kind), path in sorted(found.items())
            if month in (None, m) and kind not in state["months"].get(m, {})]
    if not todo:
        log("nothing to convert" + (f" for {month}" if month else "") + ": every downloaded month is in the lake")
        return 1 if failed else 0
    con = connect(lake, memory_limit)
    for m, kind, path in todo:
        log(f"{path.name}: converting ({path.stat().st_size / 1e9:.1f} GB)")
        try:
            rec = convert(con, path, kind, m, lake, chunk_bytes, log)
        except Incomplete as err:
            log(f"error: {err} It's left out; run update again once it's finished.")
            failed = True
            continue
        except (zstandard.ZstdError, duckdb.Error, OSError) as err:
            log(f"error: {path.name} couldn't be converted ({err}); if it's still downloading, wait, else download it again")
            failed = True
            continue
        finally:
            shutil.rmtree(lake / kind / f"{m}.partial", ignore_errors=True)
        state["months"].setdefault(m, {})[kind] = rec
        save_state(lake, state)
        note = f", {plural(rec['skipped'], 'malformed line')} skipped" if rec["skipped"] else ""
        log(f"{path.name}: {rec['rows']:,} {kind} in {plural(rec['parts'], 'part')}, {rec['seconds']} s{note}")
    shutil.rmtree(lake / ".tmp", ignore_errors=True)
    return 1 if failed else 0


# extract -------------------------------------------------------------------------------------

def gaps(state: dict) -> list[str]:
    """What's missing between the lake's first and last month."""
    months = sorted(state["months"])
    problems = []
    for m in month_range(months[0], months[-1]) if months else []:
        rec = state["months"].get(m, {})
        if not rec:
            problems.append(f"{m} isn't in the lake")
        elif len(rec) == 1:
            have = next(iter(rec))
            problems.append(f"{m} has {have} but no {'posts' if have == 'comments' else 'comments'}")
    return problems


def extract(root: Path, names: list[str], out: Path, *, log=print, allow_partial_history: bool = False) -> int:
    """Writes r_<Name>_posts.jsonl and r_<Name>_comments.jsonl for each subreddit into `out`.
    Returns 0; 1 if a subreddit has nothing in the lake; or 2 if the lake has a gap, or a
    subreddit has items in the lake's first month, so its history may start earlier. An archive
    build records where it ends, not where it starts, so a build missing its early months would
    pass for complete. `allow_partial_history` lets such a subreddit through (one that really
    began that month), with a warning."""
    bad = [n for n in names if not SUBREDDIT.match(n)]
    if bad:
        raise ValueError(f"not subreddit names: {bad}")
    lake = root / "lake"
    state = load_state(lake)
    problems = gaps(state) or ([] if state["months"] else ["the lake is empty: run update first"])
    if problems:
        for p in problems:
            log(f"error: {p}")
        log("Nothing was written: download and convert what's missing (status lists it), then extract again.")
        return 2
    months = sorted(state["months"])
    con = connect(lake)
    con.execute("SET parquet_metadata_cache = true")
    sources = {}
    for kind in ("posts", "comments"):
        globs = [f"'{posix(lake / kind / m)}/part-*.parquet'" for m in months if state["months"][m][kind]["parts"]]
        sources[kind] = f"read_parquet([{', '.join(globs)}])" if globs else None
    out.mkdir(parents=True, exist_ok=True)
    missing = refused = False
    for name in names:
        key = name.lower()
        counts = {}
        for kind, src in sources.items():
            counts[kind] = con.execute(
                f"SELECT count(*), min(created_utc), max(created_utc) FROM {src} WHERE subreddit_key = '{key}'"
            ).fetchone() if src else (0, None, None)
        if not any(c[0] for c in counts.values()):
            log(f"r/{name}: nothing in the lake ({spans(months)})")
            missing = True
            continue
        union = " UNION ALL ".join(f"SELECT subreddit FROM {src} WHERE subreddit_key = '{key}'"
                                   for src in sources.values() if src)
        spelled = con.execute(f"SELECT subreddit FROM ({union}) GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1").fetchone()[0]
        first = min(c[1] for c in counts.values() if c[1] is not None)
        last = max(c[2] for c in counts.values() if c[2] is not None)
        early = month_of(first) == months[0] and months[0] != FIRST_MONTH
        if early and not allow_partial_history:
            log(f"error: r/{spelled} has items in {months[0]}, the lake's first month, so its history may start "
                "earlier: download and convert older months, then extract again (or pass --allow-partial-history "
                f"if r/{spelled} began in {months[0]}). Its files weren't written.")
            refused = True
            continue
        written = []
        for kind, src in sources.items():
            dest = out / f"r_{spelled}_{kind}.jsonl"
            tmp = out / f"r_{spelled}_{kind}.jsonl.partial"
            if src:
                con.execute(f"""COPY (SELECT * EXCLUDE (subreddit_key) FROM {src} WHERE subreddit_key = '{key}'
                                      ORDER BY created_utc, id) TO '{posix(tmp)}' (FORMAT json)""")
            else:
                tmp.write_text("", encoding="utf-8")
            os.replace(tmp, dest)
            written.append(dest.name)
        log(f"r/{spelled}: {counts['posts'][0]:,} posts and {counts['comments'][0]:,} comments, "
            f"{month_of(first)} to {month_of(last)}: {', '.join(written)}")
        if early:
            log(f"warning: r/{spelled} has items in {months[0]}, the lake's first month; written anyway "
                "(--allow-partial-history), as if it began then")
    return 2 if refused else 1 if missing else 0


# status --------------------------------------------------------------------------------------

def magnet(infohash: str, name: str) -> str:
    return f"magnet:?xt=urn:btih:{infohash}&dn=reddit-{name}{TRACKERS}"


def parse_releases(text: str) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """From Arctic Shift's download page: {month: infohash} for each monthly torrent, and
    (first, last, magnet) for each torrent of several months. Struck-out entries are left out."""
    row = re.compile(r"^\|\s*(~~)?\s*(\d{4}-\d{2})\s*(?:-\s*(\d{4}-\d{2}))?\s*(~~)?\s*\|([^|]*)\|")
    months: dict[str, str] = {}
    bundles: list[tuple[str, str, str]] = []
    for line in text.splitlines():
        m = row.match(line)
        if not m or m[1] or m[4] or "~~" in m[5]:
            continue
        h = re.search(r"academictorrents\.com/details/([0-9a-f]{40})", m[5])
        if not h:
            continue
        if m[3]:
            bundles.append((m[2], m[3], magnet(h[1], f"{m[2]}_{m[3]}")))
        else:
            months[m[2]] = h[1]
    for m in re.finditer(r"^(\d{4}-\d{2}) - (\d{4}-\d{2}) magnet link: `(magnet:[^`]+)`", text, re.M):
        bundles.append((m[1], m[2], m[3]))
    return months, bundles


def status(root: Path, *, log=print, releases: str | None = None) -> int:
    lake = root / "lake"
    state = load_state(lake)
    found, twice, downloading = scan_raw(root / "raw")
    if not (root / "raw").is_dir():
        log(f"no {(root / 'raw').as_posix()} yet: download the monthly dumps into it")
    done = sorted(m for m, rec in state["months"].items() if len(rec) == 2)
    log(f"in the lake: {spans(done) or 'nothing yet'} ({plural(len(done), 'month')})")
    for problem in gaps(state):
        log(f"gap: {problem}")
    pending = sorted({m for (m, kind) in found if kind not in state["months"].get(m, {})})
    if pending:
        log(f"to convert: {spans(pending)}: run update")
    for (m, kind), paths in sorted(twice.items()):
        log(f"two copies of {FILE_PREFIX[kind]}_{m}.zst: {', '.join(str(p) for p in paths)} (keep one)")
    if downloading:
        log(f"downloading: {', '.join(p.name for p in downloading)}")
    if releases is not None:
        released, bundles = parse_releases(releases)
        have = {m for (m, _kind) in found} | set(state["months"])
        wanted = sorted(m for m in released if m not in have)
        log(f"released, not downloaded: {spans(wanted) or 'none'}")
        for m in wanted:
            log(f"  {m}: {magnet(released[m], m)}")
        for first, last, link in bundles:
            log(f"also as one torrent, {first} to {last}: {link}")
    return 0


# the command ---------------------------------------------------------------------------------

def _month(text: str) -> str:
    if not MONTH.match(text):
        raise argparse.ArgumentTypeError(f"not a month (YYYY-MM): {text!r}")
    return text


def _subreddits(text: str) -> list[str]:
    names = [n.strip().removeprefix("r/") for n in text.split(",") if n.strip()]
    bad = [n for n in names if not SUBREDDIT.match(n)]
    if bad or not names:
        raise argparse.ArgumentTypeError(f"not subreddit names: {', '.join(bad) or text!r}")
    return names


def _memory(text: str) -> str:
    if not MEMORY.match(text):
        raise argparse.ArgumentTypeError(f"not a size such as 8GB: {text!r}")
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("status", help="what's in the lake, what to convert, what to download")
    s.add_argument("--releases", action="store_true", help="also check Arctic Shift's download page for months to download")
    u = sub.add_parser("update", help="convert each downloaded month that isn't in the lake yet")
    u.add_argument("--month", type=_month, help="only this month (YYYY-MM)")
    u.add_argument("--chunk-mb", type=int, default=CHUNK_BYTES >> 20, help="decompressed MB per Parquet part (default 2048)")
    u.add_argument("--memory-limit", type=_memory, help="DuckDB's memory limit, e.g. 8GB")
    e = sub.add_parser("extract", help="write r_<Name>_posts.jsonl and r_<Name>_comments.jsonl for subreddits")
    e.add_argument("--subreddits", type=_subreddits, required=True, help="comma-separated names")
    e.add_argument("--out", type=Path, required=True, help="where to write the JSONL files")
    e.add_argument("--allow-partial-history", action="store_true",
                   help="write a subreddit with items in the lake's first month anyway (it began then)")
    for p in (s, u, e):
        p.add_argument("--root", type=Path, required=True, help="the folder holding raw/ and lake/")
    args = parser.parse_args(argv)
    if args.command == "status":
        text = None
        if args.releases:
            try:
                with urllib.request.urlopen(RELEASES_URL, timeout=30) as resp:
                    text = resp.read().decode("utf-8")
            except OSError as err:
                print(f"warning: couldn't read the download page ({err})", file=sys.stderr)
        return status(args.root, releases=text)
    if args.command == "update":
        if args.chunk_mb < 1:
            parser.error("--chunk-mb must be at least 1")
        return update(args.root, month=args.month, chunk_bytes=args.chunk_mb << 20, memory_limit=args.memory_limit)
    return extract(args.root, args.subreddits, args.out, allow_partial_history=args.allow_partial_history)


if __name__ == "__main__":
    sys.exit(main())
