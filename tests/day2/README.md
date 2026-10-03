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

The 990 rows are **candidates**, not a promise of 990 runtime `JOINED` lines:
Kafka partition order, event-time watermarks, late records, and incomplete replay
can change the actual count. Duplicate `event_id` is not filtered by the Day 2
job; deduplication belongs to Day 4. The old 30-second interval only covers 11
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

Join design in `ArticleJoinBehavior`: both streams key by `article_id`; article
`ValueState` stores title/category/tags with a two-hour processing-time TTL.
Behavior arriving first stays in `ListState` until its event-time timer fires
or the article arrives, then it is emitted as `UNMATCHED_BEHAVIOR` or `JOINED`.
Pending state is cleaned by event-time timers, not a processing-time TTL, so
long idleness cannot silently drop an unmatched record. `LATE_DATA` is separate
from invalid input `DIRTY_ARTICLE`/`DIRTY_BEHAVIOR`. The behavior watermark waits
one hour because this generator deliberately sends some behavior records with
event times up to an hour ahead of their simulated arrival; the article stream
waits 30 seconds, and idle partitions are detected after 30 seconds.

Compensation design: persist `UNMATCHED_BEHAVIOR` by `event_id` and retain
`LATE_DATA` articles. When a late article arrives, replay the unmatched
behaviors with the same `article_id`, enrich them, and upsert by `event_id`
to avoid double-counting. This replay/sink is not implemented yet; currently
the three outputs are labeled separately in the console for inspection.

Kafka is unbounded: at the end of a finite replay, event-time timers for the
last pending records need more input/watermark advancement before firing.
This dataset has no malformed JSON, missing fields, or nonexistent article IDs;
the edge inputs add those cases. A deterministic Flink replay of late/idle
behavior, versioned article updates, and an actual captured run log are still
needed before claiming full Day 2 acceptance.
