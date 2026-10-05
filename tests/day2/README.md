# Day 2 Join / dirty-data check

Fixed inputs: `../../generator/generator/test1_data/article_stream.jsonl` and
`../../generator/generator/test1_data/behavior_stream.jsonl`. The SQL lives in
`verify.sql`; `verify.py` only loads the JSONL lines into an in-memory SQLite
database and runs the queries. Nothing is written to MySQL or Kafka.

From the project root:

```powershell
py tests\day2\verify.py
```

Reference result for the current fixed inputs:

```text
articles_input: 50
behaviors_input: 1000
articles_dirty: 0
behaviors_dirty: 10
dirty_invalid_duration: 6
dirty_future_event_time: 4
behavior_duplicate_event_id: 20
old_interval_30s_pairs: 11
state_join_candidates: 990
no_article_candidates: 0
```

The 990 rows are **schema-valid candidates before event_id deduplication**, not
a promise of 990 runtime Join outputs. The current production ETL removes duplicate
behavior event IDs before assigning watermarks and joining; SQL in `tests/roles`
reflects that same ordering. The old 30-second interval only covers 11
pairs in this dataset, because most behaviors occur well after publication.

The small fixed edge inputs are in `inputs/`. They cover behavior arriving
before its article, a missing article, a null `article_id`, negative duration,
future event time, malformed JSON, and a late-record candidate:

```powershell
py tests\day2\verify.py --data-dir tests\day2\inputs
```

Their SQL baseline is 1 article, 9 behaviors, 4 dirty behaviors, 4 join
candidates, and 1 behavior without an article. Of the 4 join candidates, the
last record has an old event time and is intended to enter `LATE_DATA` once
the behavior watermark has advanced beyond it. Kafka partitions and idle
watermarks make this timing-dependent; SQL does not label it definitively late.

To compare a **single isolated replay** with Flink, save the IDEA console
output to a UTF-8 file and run:

```powershell
py tests\day2\verify.py --flink-log tests\day2\run.log
```

Compare `dirty_missing`, `dirty_unexpected`, `joined_dirty_fields`, and
`joined_wrong_article_fields` first (all should be zero for a complete replay).
Then inspect `joined_missing` alongside `late_actual` and `unmatched_actual`.
Do not compare a run that includes earlier Kafka messages or multiple replays
against this one-dataset baseline.

Current Join design in `ArticleJoinBehavior`: both streams key by `article_id`;
article and first-publication state have a two-hour processing-time TTL. First
clean/schema-valid behavior is deduplicated by event ID. Behavior arriving before
its first publication immediately enters `UNMATCHED_BEHAVIOR` and is retained
in keyed `MapState` for rejoining when the publication arrives. Every pending
event has a two-hour processing-time deadline; unresolved records go to
`REPLAY_REQUIRED`, never disappear silently. With bounded replay a final
event-time audit handles unresolved keys. `LATE_DATA` records beyond the Join's
30-second allowed-lateness bound are still passed through temporal ETL/Join.
Article watermark disorder is 30 seconds, behavior is 65 minutes in continuous
mode and two hours for accelerated bounded cross-partition replay; idle partitions
are detected after 30 seconds. An event earlier than first publication is rejected
by temporal ETL into `DIRTY_BEHAVIOR_ETL`.

Compensation: late publication automatically joins pending behaviors within the
two-hour state period; `Day5SinkJob` saves unmatched/late/replay-required events
to the MySQL `pipeline_event` table, while `clean_behavior` is keyed by event_id.
After TTL expiry, a fresh `--bounded` replay from retained raw Kafka topics can
rebuild article state and upsert the clean behavior. Kafka retention must still
cover that replay; missing source data cannot be recreated.

Kafka is unbounded: at the end of a finite replay, event-time timers for the
last pending records need more input/watermark advancement before firing.
This dataset has no malformed JSON, missing fields, or nonexistent article IDs;
the edge inputs add those cases. A deterministic Flink replay of late/idle
behavior, versioned article updates, and an actual captured run log are still
needed before claiming full Day 2 acceptance.
