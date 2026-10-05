#!/usr/bin/env python3
"""Generate reproducible article and behavior streams for the Flink assessment.

The generator intentionally keeps dirty records outside the JSON Schema contract.
That gives the Flink job realistic invalid input to route to a side output while
the clean records remain easy to validate.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


UTC = timezone.utc
DEFAULT_START_TIME = "2026-09-27T00:00:00Z"
DEFAULT_SEED = 20260927
DEFAULT_ARTICLE_COUNT = 500
DEFAULT_BEHAVIOR_COUNT = 100000
DEFAULT_DISORDER_MIN = 5
DEFAULT_DISORDER_MAX = 30

ACTION_WEIGHTS: Sequence[Tuple[str, float]] = (
    ("click", 0.82),
    ("share", 0.10),
    ("comment", 0.08),
)

CATEGORIES = (
    "technology",
    "finance",
    "sports",
    "entertainment",
    "science",
    "health",
    "world",
)

TITLE_TEMPLATES = (
    "{category}领域迎来新变化，专家解读背后的关键机会",
    "一线观察：{category}热点事件正在如何影响普通人",
    "最新进展公布，{category}行业进入加速阶段",
    "从数据看{category}，这几个趋势值得持续关注",
    "权威报告发布：{category}未来一年的重点方向",
)

TAG_POOL = (
    "热点",
    "实时",
    "趋势",
    "深度",
    "观察",
    "行业",
    "数据",
    "现场",
    "解读",
    "推荐",
)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate article_stream and behavior_stream test data."
    )
    parser.add_argument("--article-count", type=int, default=DEFAULT_ARTICLE_COUNT)
    parser.add_argument("--behavior-count", type=int, default=DEFAULT_BEHAVIOR_COUNT)
    parser.add_argument("--rate", type=float, default=0.0, help="Events per second; 0 disables throttling.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--disorder-min", type=int, default=DEFAULT_DISORDER_MIN)
    parser.add_argument("--disorder-max", type=int, default=DEFAULT_DISORDER_MAX)
    parser.add_argument("--dirty-ratio", type=float, default=0.01)
    parser.add_argument("--duplicate-ratio", type=float, default=0.02)
    parser.add_argument("--start-time", default=DEFAULT_START_TIME)
    parser.add_argument(
        "--output",
        choices=("json", "kafka", "both"),
        default="both",
        help="Write JSONL files, Kafka messages, or both.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("generator/data"))
    parser.add_argument("--article-topic", default="topic_article")
    parser.add_argument("--behavior-topic", default="topic_behavior")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--kafka-client-id",
        default="hotnews-generator",
        help="Kafka client id used when --output includes kafka.",
    )
    args = parser.parse_args(argv)
    validate_args(args)
    return args


def validate_args(args: argparse.Namespace) -> None:
    if args.article_count < 1:
        raise ValueError("--article-count must be >= 1")
    if args.behavior_count < 1:
        raise ValueError("--behavior-count must be >= 1")
    if args.rate < 0:
        raise ValueError("--rate must be >= 0")
    if args.disorder_min < 0 or args.disorder_max < args.disorder_min:
        raise ValueError("--disorder-min/max must satisfy 0 <= min <= max")
    if not 0 <= args.dirty_ratio <= 1:
        raise ValueError("--dirty-ratio must be between 0 and 1")
    if not 0 <= args.duplicate_ratio < 1:
        raise ValueError("--duplicate-ratio must be between 0 and 1")


def parse_timestamp(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_id(prefix: str, number: int) -> str:
    return f"{prefix}-{number:08d}"


def choose_weighted(rng: random.Random, values: Sequence[Tuple[str, float]]) -> str:
    return rng.choices(
        population=[value for value, _ in values],
        weights=[weight for _, weight in values],
        k=1,
    )[0]


def build_articles(
    count: int,
    start_time: datetime,
    disorder_min: int,
    disorder_max: int,
    rng: random.Random,
) -> Tuple[List[Dict[str, Any]], Dict[str, datetime]]:
    articles: List[Dict[str, Any]] = []
    article_published_at: Dict[str, datetime] = {}

    for index in range(1, count + 1):
        article_id = f"article-{index:06d}"
        published_at = start_time + timedelta(seconds=rng.uniform(0, 3600))
        category = rng.choice(CATEGORIES)
        title = rng.choice(TITLE_TEMPLATES).format(category=category)
        tags = rng.sample(TAG_POOL, k=3)
        ingest_time = published_at + timedelta(
            seconds=rng.uniform(disorder_min, disorder_max)
        )
        article = {
            "event_id": event_id("article-event", index),
            "event_type": "publish",
            "article_id": article_id,
            "title": title,
            "category": category,
            "tags": tags,
            "published_at": format_timestamp(published_at),
            "event_time": format_timestamp(published_at),
            "ingest_time": format_timestamp(ingest_time),
            "version": 1,
        }
        articles.append(article)
        article_published_at[article_id] = published_at

    articles.sort(key=lambda item: item["ingest_time"])
    return articles, article_published_at


def choose_article(
    rng: random.Random,
    article_ids: Sequence[str],
    hot_article_ids: Sequence[str],
    action: str,
) -> str:
    if action == "click" and rng.random() < 0.85:
        return rng.choice(hot_article_ids)
    return rng.choice(article_ids)


def build_behaviors(
    count: int,
    article_ids: Sequence[str],
    article_published_at: Dict[str, datetime],
    start_time: datetime,
    disorder_min: int,
    disorder_max: int,
    dirty_ratio: float,
    duplicate_ratio: float,
    rng: random.Random,
) -> Tuple[List[Dict[str, Any]], Counter, Sequence[str]]:
    unique_count = max(1, count - round(count * duplicate_ratio))
    dirty_count = round(count * dirty_ratio)
    hot_article_ids = tuple(article_ids[: min(2, len(article_ids))])
    behaviors: List[Dict[str, Any]] = []
    action_counts: Counter = Counter()

    for index in range(1, unique_count + 1):
        action = choose_weighted(rng, ACTION_WEIGHTS)
        action_counts[action] += 1
        article_id = choose_article(rng, article_ids, hot_article_ids, action)
        article_time = article_published_at[article_id]
        event_time = article_time + timedelta(seconds=rng.uniform(1, 3600))
        ingest_time = event_time + timedelta(
            seconds=rng.uniform(disorder_min, disorder_max)
        )
        # Make some behavior messages arrive before their article message.
        # The event time remains valid, but the article source is delayed.
        if index <= max(1, count // 100):
            ingest_time = article_time + timedelta(seconds=rng.uniform(1, 4))

        behavior = {
            "event_id": event_id("behavior-event", index),
            "user_id": f"user-{rng.randint(1, max(1000, count // 20)):06d}",
            "article_id": article_id,
            "action": action,
            "ip": f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
            "event_time": format_timestamp(event_time),
            "ingest_time": format_timestamp(ingest_time),
            "read_duration_ms": (
                rng.randint(200, 120_000) if action == "click" else rng.randint(500, 180_000)
            ),
        }
        behaviors.append(behavior)

    duplicate_count = count - unique_count
    for duplicate_index in range(duplicate_count):
        source = behaviors[duplicate_index % len(behaviors)]
        behaviors.append(dict(source))

    behaviors.sort(key=lambda item: item["ingest_time"])
    dirty_indices = set(rng.sample(range(len(behaviors)), k=min(dirty_count, len(behaviors))))
    dirty_kinds = ("null_article_id", "negative_duration", "future_event_time")
    dirty_kind_counts: Counter = Counter()

    for index in dirty_indices:
        behavior = behaviors[index]
        dirty_kind = dirty_kinds[index % len(dirty_kinds)]
        dirty_kind_counts[dirty_kind] += 1
        if dirty_kind == "null_article_id":
            behavior["article_id"] = None
        elif dirty_kind == "negative_duration":
            behavior["read_duration_ms"] = -rng.randint(1, 60_000)
        else:
            behavior["event_time"] = format_timestamp(start_time + timedelta(days=3650))

    return behaviors, dirty_kind_counts, hot_article_ids


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]], rate: float) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    interval = 1.0 / rate if rate > 0 else 0.0
    next_emit = time.monotonic()
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            written += 1
            if interval:
                next_emit += interval
                delay = next_emit - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
    return written


def kafka_producer(bootstrap_servers: str, client_id: str) -> Any:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka output requires kafka-python. Install it with: "
            "python -m pip install kafka-python"
        ) from exc
    return KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",") if item.strip()],
        client_id=client_id,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )


def write_kafka(
    producer: Any,
    article_topic: str,
    behavior_topic: str,
    articles: Sequence[Dict[str, Any]],
    behaviors: Sequence[Dict[str, Any]],
    rate: float,
) -> None:
    events = [
        (parse_timestamp(record["ingest_time"]), article_topic, record)
        for record in articles
    ]
    events.extend(
        (parse_timestamp(record["ingest_time"]), behavior_topic, record)
        for record in behaviors
    )
    events.sort(key=lambda item: item[0])
    interval = 1.0 / rate if rate > 0 else 0.0
    next_emit = time.monotonic()
    for _, topic, record in events:
        producer.send(topic, value=record, key=record["article_id"].encode("utf-8") if record.get("article_id") else None)
        if interval:
            next_emit += interval
            delay = next_emit - time.monotonic()
            if delay > 0:
                time.sleep(delay)
    producer.flush()


def write_report(
    output_dir: Path,
    args: argparse.Namespace,
    articles: Sequence[Dict[str, Any]],
    behaviors: Sequence[Dict[str, Any]],
    action_counts: Counter,
    dirty_kind_counts: Counter,
    hot_article_ids: Sequence[str],
) -> Path:
    click_counts = Counter(
        record["article_id"]
        for record in behaviors
        if record.get("action") == "click" and record.get("article_id")
    )
    total_clicks = sum(click_counts.values())
    hot_clicks = sum(click_counts[article_id] for article_id in hot_article_ids)
    report = {
        "seed": args.seed,
        "start_time": args.start_time,
        "article_count": len(articles),
        "behavior_count": len(behaviors),
        "action_counts": dict(action_counts),
        "dirty_record_count": sum(dirty_kind_counts.values()),
        "dirty_kinds": dict(dirty_kind_counts),
        "duplicate_record_count": args.behavior_count - len(
            {record["event_id"] for record in behaviors}
        ),
        "hot_article_ids": list(hot_article_ids),
        "hot_article_click_share": round(hot_clicks / total_clicks, 6) if total_clicks else 0,
        "first_article_ingest_time": articles[0]["ingest_time"],
        "last_article_ingest_time": articles[-1]["ingest_time"],
        "first_behavior_ingest_time": behaviors[0]["ingest_time"],
        "last_behavior_ingest_time": behaviors[-1]["ingest_time"],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "generation_report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = parse_args(argv)
        start_time = parse_timestamp(args.start_time)
        rng = random.Random(args.seed)
        articles, article_published_at = build_articles(
            args.article_count,
            start_time,
            args.disorder_min,
            args.disorder_max,
            rng,
        )
        behaviors, dirty_kind_counts, hot_article_ids = build_behaviors(
            args.behavior_count,
            [article["article_id"] for article in articles],
            article_published_at,
            start_time,
            args.disorder_min,
            args.disorder_max,
            args.dirty_ratio,
            args.duplicate_ratio,
            rng,
        )

        action_counts = Counter(record["action"] for record in behaviors)
        if args.output in ("json", "both"):
            write_jsonl(args.output_dir / "article_stream.jsonl", articles, args.rate)
            write_jsonl(args.output_dir / "behavior_stream.jsonl", behaviors, args.rate)

        if args.output in ("kafka", "both"):
            producer = kafka_producer(args.kafka_bootstrap_servers, args.kafka_client_id)
            try:
                write_kafka(
                    producer,
                    args.article_topic,
                    args.behavior_topic,
                    articles,
                    behaviors,
                    args.rate,
                )
            finally:
                producer.close()

        report_path = write_report(
            args.output_dir,
            args,
            articles,
            behaviors,
            action_counts,
            dirty_kind_counts,
            hot_article_ids,
        )
        print(f"Generated {len(articles)} article records.")
        print(f"Generated {len(behaviors)} behavior records.")
        print(f"Actions: {dict(action_counts)}")
        print(f"Report: {report_path}")
        return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"generation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
