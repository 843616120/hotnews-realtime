#!/usr/bin/env python3
"""Run the day-2 SQL baseline against the fixed JSONL inputs and optional Flink log."""

import argparse
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "generator" / "generator" / "test1_data"
LOG_RECORD = re.compile(
    r"\b(JOINED|DIRTY_ARTICLE|DIRTY_BEHAVIOR|LATE_DATA|UNMATCHED_BEHAVIOR)"
    r"(?::\d+)?>\s*(.*)$"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DATA, help="directory with article/behavior JSONL")
    parser.add_argument("--flink-log", type=Path, help="IDEA console output saved as UTF-8 text")
    args = parser.parse_args()

    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TEMP TABLE article_raw(line TEXT NOT NULL)")
        db.execute("CREATE TEMP TABLE behavior_raw(line TEXT NOT NULL)")
        db.execute("CREATE TEMP TABLE flink_output(tag TEXT NOT NULL, line TEXT NOT NULL)")
        for table, filename in (
            ("article_raw", "article_stream.jsonl"),
            ("behavior_raw", "behavior_stream.jsonl"),
        ):
            with (args.data_dir / filename).open(encoding="utf-8") as source:
                db.executemany(
                    f"INSERT INTO {table}(line) VALUES (?)",
                    ((line.strip(),) for line in source if line.strip()),
                )

        if args.flink_log:
            with args.flink_log.open(encoding="utf-8") as source:
                db.executemany(
                    "INSERT INTO flink_output(tag, line) VALUES (?, ?)",
                    (match.groups() for line in source if (match := LOG_RECORD.search(line))),
                )
            if db.execute("SELECT COUNT(*) FROM flink_output").fetchone()[0] == 0:
                parser.error("No JOINED/DIRTY/LATE/UNMATCHED console lines found in --flink-log")

        db.executescript((Path(__file__).with_name("verify.sql")).read_text(encoding="utf-8"))
        print(f"SQL baseline ({args.data_dir}):")
        for metric, count in db.execute("SELECT metric, n FROM day2_summary"):
            print(f"  {metric}: {count}")
        if args.flink_log:
            print("Flink console comparison:")
            for metric, count in db.execute("SELECT metric, n FROM day2_comparison"):
                print(f"  {metric}: {count}")


if __name__ == "__main__":
    main()
