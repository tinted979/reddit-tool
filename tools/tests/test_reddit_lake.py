"""Tests for tools/reddit_lake.py: a local copy of the monthly Reddit dumps that's quick to
query (the lake), and per-subreddit files for the archive. The dumps here are small, but
compressed as the real ones are, with a frame that declares a 2 GB window. Run from the repo
root:

  uv run --with duckdb --with pytest --with zstandard pytest tools
"""

import io
import json
import sys
from pathlib import Path

import duckdb
import pytest
import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import build_dumps  # noqa: E402
import reddit_lake  # noqa: E402

JAN, FEB, MAR = 1_767_225_600, 1_769_904_000, 1_772_323_200  # 2026-01-01, 02-01, 03-01 UTC


def dump(path: Path, rows: list, *, frames: int = 1, cut: int | None = None) -> Path:
    """A monthly dump as the real ones are: zstd with a 2 GB window and no content size, one
    JSON object a line. `rows` may hold raw strings (a malformed line). `cut` keeps only the
    first bytes, like a download that isn't finished."""
    lines = [r if isinstance(r, str) else json.dumps(r) for r in rows]
    per = -(-len(lines) // frames)
    params = zstandard.ZstdCompressionParameters.from_level(3, window_log=31, write_content_size=False)
    blob = b""
    for i in range(frames):
        buf = io.BytesIO()
        with zstandard.ZstdCompressor(compression_params=params).stream_writer(buf, closefd=False) as w:
            w.write("".join(line + "\n" for line in lines[i * per:(i + 1) * per]).encode())
        blob += buf.getvalue()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob[:cut] if cut else blob)
    return path


def comment(i, sub, author, t, **extra):
    return {"id": f"c{i}", "subreddit": sub, "subreddit_id": "t5_1", "author": author, "created_utc": t,
            "link_id": f"t3_p{i % 3}", "parent_id": f"t3_p{i % 3}", "body": f"comment {i}", "score": 2,
            "edited": False, "retrieved_on": int(float(t)) + 60, "permalink": "/r/x/", "likes": None, **extra}


def post(i, sub, author, t, **extra):
    return {"id": f"p{i}", "subreddit": sub, "author": author, "created_utc": t, "title": f"post {i}",
            "selftext": "", "url": "https://example.com", "score": 5, "num_comments": 3, "is_self": True,
            "over_18": False, "edited": False, **extra}


def month_rows(start: int, n: int = 30):
    """n comments and n posts over three subreddits, out of order, as the dumps can be."""
    subs = ["Socialism_101", "AskReddit", "rust"]
    comments = [comment(start + i, subs[i % 3], f"user{i % 4}", start + 3600 * ((i * 7) % n)) for i in range(n)]
    posts = [post(start + i, subs[i % 3], f"user{i % 5}", start + 3600 * ((i * 5) % n)) for i in range(n)]
    return comments, posts


@pytest.fixture
def root(tmp_path):
    """Two months downloaded, in the combined torrent's layout and a monthly torrent's."""
    for month, start, folder in (("2026-01", JAN, "reddit"), ("2026-02", FEB, "2026-02")):
        comments, posts = month_rows(start)
        dump(tmp_path / "raw" / folder / "comments" / f"RC_{month}.zst", comments)
        dump(tmp_path / "raw" / folder / "submissions" / f"RS_{month}.zst", posts)
    return tmp_path


def lines(log):
    return "\n".join(log)


def update(root, **kw):
    log = []
    code = reddit_lake.update(root, log=log.append, chunk_bytes=kw.pop("chunk_bytes", 2048), **kw)
    return code, log


def months(root):
    return json.loads((root / "lake" / "months.json").read_text(encoding="utf-8"))


def parts(root, kind, month):
    return sorted((root / "lake" / kind / month).glob("part-*.parquet"))


# update --------------------------------------------------------------------------------------

def test_update_converts_each_downloaded_month_into_parts_sorted_by_subreddit(root):
    code, log = update(root)
    assert code == 0, lines(log)
    state = months(root)
    assert sorted(state["months"]) == ["2026-01", "2026-02"]
    for month in ("2026-01", "2026-02"):
        for kind in ("comments", "posts"):
            rec = state["months"][month][kind]
            assert (rec["lines"], rec["rows"], rec["skipped"]) == (30, 30, 0)
            files = parts(root, kind, month)
            assert len(files) == rec["parts"] > 1  # small chunks: several parts
            for f in files:
                keys = duckdb.sql(f"SELECT subreddit_key, created_utc FROM read_parquet('{f.as_posix()}')").fetchall()
                assert keys == sorted(keys)
    # Every row is there once, whatever part it landed in.
    got = duckdb.sql(f"SELECT count(DISTINCT id) FROM read_parquet('{(root / 'lake' / 'comments').as_posix()}/*/*.parquet')").fetchone()
    assert got == (60,)


def test_the_lake_keeps_the_text_and_typed_fields_and_drops_the_noise(tmp_path):
    rows = [
        comment(1, "rust", "a", "1767225600", edited=1767300000, _meta={"was_deleted_later": True}),
        comment(2, "rust", "b", 1767225601.0, edited=True, retrieved_on=None, retrieved_utc=1767999999),
        comment(3, "Rust", "c", 1767225602),
    ]
    dump(tmp_path / "raw" / "RC_2026-01.zst", rows)
    dump(tmp_path / "raw" / "RS_2026-01.zst", [post(1, "rust", "a", JAN)])
    code, log = update(tmp_path)
    assert code == 0, lines(log)
    con = duckdb.connect()
    got = con.execute(f"SELECT * FROM read_parquet('{parts(tmp_path, 'comments', '2026-01')[0].as_posix()}') ORDER BY id").fetchall()
    cols = [d[0] for d in con.description]
    by_id = {r[cols.index("id")]: dict(zip(cols, r)) for r in got}
    assert cols[0] == "subreddit_key" and "body" in cols and "permalink" not in cols and "likes" not in cols
    assert by_id["c1"]["created_utc"] == 1767225600 and by_id["c2"]["created_utc"] == 1767225601
    assert [by_id[i]["edited"] for i in ("c1", "c2", "c3")] == [1767300000, 0, None]  # 0: edited, time unknown
    assert by_id["c2"]["retrieved_on"] == 1767999999  # retrieved_utc when retrieved_on is missing
    assert json.loads(by_id["c1"]["_meta"]) == {"was_deleted_later": True}
    assert {by_id[i]["subreddit_key"] for i in by_id} == {"rust"}
    assert by_id["c1"]["body"] == "comment 1"


def test_update_is_safe_to_run_again(root):
    assert update(root)[0] == 0
    before = {p: p.stat().st_mtime_ns for p in (root / "lake").rglob("*.parquet")}
    code, log = update(root)
    assert code == 0
    assert "nothing to convert" in lines(log)
    assert {p: p.stat().st_mtime_ns for p in (root / "lake").rglob("*.parquet")} == before


def test_a_download_that_isnt_finished_is_not_added(root):
    comments, _ = month_rows(MAR, n=300)
    full = dump(root / "raw" / "RC_2026-03.zst", comments)
    dump(root / "raw" / "RC_2026-03.zst", comments, cut=full.stat().st_size // 2)
    dump(root / "raw" / "RS_2026-03.zst", month_rows(MAR)[1])
    code, log = update(root)
    assert code == 1
    assert "RC_2026-03.zst" in lines(log) and "still downloading" in lines(log)
    state = months(root)["months"]
    assert "comments" not in state["2026-03"] and "posts" in state["2026-03"]  # the rest still goes in
    assert not (root / "lake" / "comments" / "2026-03").exists()
    assert not list((root / "lake").rglob("*.partial"))


def test_a_corrupt_file_is_not_added(root):
    (root / "raw" / "RC_2026-03.zst").write_bytes(b"\x28\xb5\x2f\xfd" + b"\x00garbage" * 50)
    code, log = update(root)
    assert code == 1 and "RC_2026-03.zst" in lines(log)
    assert "comments" not in months(root)["months"].get("2026-03", {})


def test_files_still_downloading_are_left_alone(root):
    (root / "raw" / "RC_2026-04.zst.!qB").write_bytes(b"partial")
    (root / "raw" / "RS_2026-04.zst.part").write_bytes(b"partial")
    code, log = update(root)
    assert code == 0, lines(log)
    assert "2026-04" not in months(root)["months"]


def test_a_conversion_cut_off_last_time_is_redone(root):
    assert update(root, month="2026-01")[0] == 0
    stale = root / "lake" / "comments" / "2026-02.partial"
    stale.mkdir(parents=True)
    (stale / "part-00000.parquet").write_bytes(b"half written")
    # And a finished folder that months.json never recorded (cut off between the two).
    orphan = root / "lake" / "posts" / "2026-02"
    orphan.mkdir(parents=True)
    (orphan / "part-00000.parquet").write_bytes(b"not recorded")
    code, log = update(root)
    assert code == 0, lines(log)
    assert not stale.exists()
    assert months(root)["months"]["2026-02"]["posts"]["rows"] == 30
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{orphan.as_posix()}/*.parquet')").fetchone() == (30,)


def test_malformed_lines_and_frames(tmp_path):
    comments, posts = month_rows(JAN, n=12)
    dump(tmp_path / "raw" / "RC_2026-01.zst", [*comments[:5], "{not json", *comments[5:]], frames=3)
    dump(tmp_path / "raw" / "RS_2026-01.zst", posts)
    code, log = update(tmp_path)
    assert code == 0, lines(log)
    rec = months(tmp_path)["months"]["2026-01"]["comments"]
    assert (rec["lines"], rec["rows"], rec["skipped"]) == (13, 12, 1)
    assert "1 malformed" in lines(log)


def test_two_copies_of_one_month_are_refused(root):
    dump(root / "raw" / "elsewhere" / "RC_2026-01.zst", month_rows(JAN)[0])
    code, log = update(root)
    assert code == 1 and "two copies of RC_2026-01.zst" in lines(log)
    assert "comments" not in months(root)["months"].get("2026-01", {})


# extract -------------------------------------------------------------------------------------

def extract(root, names, out, **kw):
    log = []
    code = reddit_lake.extract(root, names, out, log=log.append, **kw)
    return code, log


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_extract_writes_each_subreddit_in_the_download_tools_shape(root, tmp_path):
    assert update(root)[0] == 0
    out = tmp_path / "out"
    code, log = extract(root, ["socialism_101", "RUST"], out)
    assert code == 0, lines(log)
    # Named as the subreddit spells itself, like the download tool's files.
    assert sorted(p.name for p in out.iterdir()) == [
        "r_Socialism_101_comments.jsonl", "r_Socialism_101_posts.jsonl", "r_rust_comments.jsonl", "r_rust_posts.jsonl"]
    comments = read_jsonl(out / "r_Socialism_101_comments.jsonl")
    assert len(comments) == 20 and {c["subreddit"] for c in comments} == {"Socialism_101"}
    assert [c["created_utc"] for c in comments] == sorted(c["created_utc"] for c in comments)
    assert "subreddit_key" not in comments[0] and comments[0]["body"].startswith("comment")
    # build_dumps.py builds from them as from the download tool's.
    build_dumps.build("Socialism_101", out / "r_Socialism_101_posts.jsonl", out / "r_Socialism_101_comments.jsonl",
                      tmp_path / "dumps", now=MAR, log=lambda *_: None)
    manifest = json.loads((tmp_path / "dumps" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["subreddits"]["socialism_101"]["files"]["comments_by_author"]["rows"] == 20


def test_extract_refuses_a_lake_with_a_gap(root, tmp_path):
    comments, posts = month_rows(MAR)
    dump(root / "raw" / "RC_2026-03.zst", comments)
    dump(root / "raw" / "RS_2026-03.zst", posts)
    assert update(root, month="2026-01")[0] == 0
    assert update(root, month="2026-03")[0] == 0
    code, log = extract(root, ["rust"], tmp_path / "out")
    assert code == 2 and "2026-02" in lines(log)
    assert not (tmp_path / "out").exists()
    # A month with only one of its two files is a gap too.
    assert update(root, month="2026-02")[0] == 0
    state = months(root)
    del state["months"]["2026-02"]["posts"]
    (root / "lake" / "months.json").write_text(json.dumps(state), encoding="utf-8")
    code, log = extract(root, ["rust"], tmp_path / "out")
    assert code == 2 and "2026-02 has comments but no posts" in lines(log)


def test_extract_warns_when_a_subreddits_history_may_start_before_the_lake(root, tmp_path):
    assert update(root)[0] == 0
    code, log = extract(root, ["rust"], tmp_path / "out")
    assert code == 0
    assert "r/rust has items in 2026-01, the lake's first month" in lines(log)


def test_extract_reports_a_subreddit_with_nothing_in_the_lake(root, tmp_path):
    assert update(root)[0] == 0
    code, log = extract(root, ["rust", "NoSuchSub"], tmp_path / "out")
    assert code == 1 and "r/NoSuchSub: nothing in the lake" in lines(log)
    assert (tmp_path / "out" / "r_rust_posts.jsonl").exists()


def test_extract_checks_its_arguments(root, tmp_path):
    with pytest.raises(SystemExit):
        reddit_lake.main(["extract", "--root", str(root), "--subreddits", "../x", "--out", str(tmp_path)])
    with pytest.raises(SystemExit):
        reddit_lake.main(["update", "--root", str(root), "--month", "2026-13"])


# status --------------------------------------------------------------------------------------

RELEASES = """
| months            | zst | zst_blocks |
|-------------------|-----|------------|
| ~~2005 - 2023-09~~    | ~~[Academic Torrents (superseded)](https://academictorrents.com/details/89d24ff9d5fbc1efcdaf9d7689d72b7548f699fc)~~ | - |
| 2026-01           | [Academic Torrents](https://academictorrents.com/details/8412b89151101d88c915334c45d9c223169a1a60) | |
| 2026-02           | [Academic Torrents](https://academictorrents.com/details/c5ba00048236b60f819dbf010e9034d24fc291fb) | |
| 2026-03           | [Academic Torrents](https://academictorrents.com/details/668087bb8c8c9c763b27a1a4c5e7fcb6add25f2c) | |
| 2005-06 - 2025-12 | ~~[Academic Torrents](https://academictorrents.com/details/3d426c47c767d40f82c7ef0f47c3acacedd2bf44)~~ <br> **See below** | |

2005-06 - 2025-12 magnet link: `magnet:?xt=urn:btih:3d426c47c767d40f82c7ef0f47c3acacedd2bf44&dn=reddit&xl=3804096351995&tr=https%3A%2F%2Facademictorrents.com%2Fannounce.php`
"""


def test_releases_are_read_from_the_download_page():
    months_, bundles = reddit_lake.parse_releases(RELEASES)
    assert months_ == {
        "2026-01": "8412b89151101d88c915334c45d9c223169a1a60",
        "2026-02": "c5ba00048236b60f819dbf010e9034d24fc291fb",
        "2026-03": "668087bb8c8c9c763b27a1a4c5e7fcb6add25f2c",
    }
    assert bundles == [("2005-06", "2025-12", "magnet:?xt=urn:btih:3d426c47c767d40f82c7ef0f47c3acacedd2bf44&dn=reddit&xl=3804096351995&tr=https%3A%2F%2Facademictorrents.com%2Fannounce.php")]


def test_status_on_a_new_root_says_where_downloads_go(tmp_path):
    log = []
    assert reddit_lake.status(tmp_path, log=log.append) == 0
    assert f"no {(tmp_path / 'raw').as_posix()} yet: download the monthly dumps into it" in lines(log)
    assert "in the lake: nothing yet (0 months)" in lines(log)


def test_status_lists_what_to_convert_and_what_to_download(root):
    assert update(root, month="2026-01")[0] == 0
    (root / "raw" / "RC_2026-04.zst.!qB").write_bytes(b"partial")
    log = []
    reddit_lake.status(root, log=log.append, releases=RELEASES)
    text = lines(log)
    assert "in the lake: 2026-01 (1 month)" in text
    assert "to convert: 2026-02" in text and "run update" in text
    assert "downloading: RC_2026-04.zst.!qB" in text
    assert "released, not downloaded: 2026-03" in text
    assert "magnet:?xt=urn:btih:668087bb8c8c9c763b27a1a4c5e7fcb6add25f2c" in text
