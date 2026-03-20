"""
core/queue_manager.py — Async crawl queue with worker pool.

The REAL orchestration engine: loads input records into an asyncio queue,
spawns N worker coroutines inside a BrowserManager context, coordinates
checkpointing and progress reporting, and returns final metrics.

This is the central module that run_pilot.py delegates to.
"""

import asyncio
import logging
import random
import time
from typing import Any, Callable, Coroutine, List, Optional

from core.config import RunConfig
from core.browser import BrowserManager
from core.models import ExtractResult, PageStatus, RunMetrics
from core.state import StateStore

logger = logging.getLogger("extraction-engine")


class QueueManager:
    """
    Async crawl queue with configurable worker pool.

    Manages the full extraction lifecycle:
    1. Filter to pending records (resume support)
    2. Launch browser via BrowserManager
    3. Spawn N worker coroutines
    4. Each worker: dequeue → extract → update state → checkpoint
    5. Final persist + validation
    6. Return RunMetrics
    """

    def __init__(
        self,
        config: RunConfig,
        state: StateStore,
    ):
        self.config = config
        self.state = state
        self.metrics = RunMetrics()
        self._lock = asyncio.Lock()

    async def run(
        self,
        records: List[dict],
        process_fn: Callable[..., Coroutine[Any, Any, ExtractResult]],
        csv_columns: List[str],
        to_csv_rows_fn: Callable[[dict], List[dict]],
        url_field: str = "model_year_url",
        label_field: str = "model_year_label",
        site_name: str = "Site",
    ) -> RunMetrics:
        """
        Process all records through the extraction pipeline.

        This is the main entry point. It handles:
        - Filtering to pending (unfinished) records
        - Launching browser + creating worker contexts
        - Running concurrent workers
        - Checkpointing progress
        - Final persist and summary

        Args:
            records: Input records to process.
            process_fn: Async function(page, record, config) -> ExtractResult.
            csv_columns: Column names for CSV export.
            to_csv_rows_fn: Function to convert a record to flat CSV rows.
            url_field: Key in record dict that holds the URL.
            label_field: Key in record dict that holds the label.
            site_name: Human-readable site name for logging.

        Returns:
            RunMetrics with final counts and timing.
        """
        # Filter to pending records only
        pending = [r for r in records if not self.state.is_done(r.get(url_field, ""))]

        self.metrics.total_input = len(records)
        self.metrics.total_processed = 0

        logger.info("=" * 80)
        logger.info("EXTRACTION ENGINE — %s", site_name)
        logger.info("Input records    : %s", f"{len(records):,}")
        logger.info("Already done     : %s", f"{len(records) - len(pending):,}")
        logger.info("Pending          : %s", f"{len(pending):,}")
        logger.info("Concurrency      : %d", self.config.concurrency)
        logger.info("Max retries      : %d", self.config.max_retries)
        logger.info("Output dir       : %s", self.config.output_dir)
        logger.info("=" * 80)

        if not pending:
            logger.info("Nothing to process — all URLs already done.")
            return self.metrics

        # Build async queue
        queue: asyncio.Queue = asyncio.Queue()
        for r in pending:
            await queue.put(r)

        total = len(pending)
        t_start = time.time()

        # Launch browser and run workers
        async with BrowserManager(self.config) as bm:
            workers = [
                asyncio.create_task(
                    self._worker(
                        worker_id=i + 1,
                        queue=queue,
                        total=total,
                        t_start=t_start,
                        bm=bm,
                        process_fn=process_fn,
                        csv_columns=csv_columns,
                        to_csv_rows_fn=to_csv_rows_fn,
                        url_field=url_field,
                        label_field=label_field,
                    )
                )
                for i in range(self.config.concurrency)
            ]
            await asyncio.gather(*workers)

        # Final persist
        self.state.persist(force=True, csv_columns=csv_columns)

        self.metrics.elapsed_seconds = time.time() - t_start

        # Print summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("EXTRACTION COMPLETE")
        for line in self.metrics.summary_lines():
            logger.info(line)
        logger.info("=" * 80)

        return self.metrics

    async def _worker(
        self,
        worker_id: int,
        queue: asyncio.Queue,
        total: int,
        t_start: float,
        bm: BrowserManager,
        process_fn: Callable,
        csv_columns: List[str],
        to_csv_rows_fn: Callable,
        url_field: str,
        label_field: str,
    ) -> None:
        """Single worker: creates a browser context, pulls records, extracts, updates state."""
        ctx = await bm.create_context()
        page = await ctx.new_page()

        try:
            while True:
                try:
                    record = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                async with self._lock:
                    self.metrics.total_processed += 1
                    idx = self.metrics.total_processed

                url = record.get(url_field, "")
                label = record.get(label_field, url)

                elapsed_total = time.time() - t_start
                rate = idx / elapsed_total if elapsed_total > 0 else 0
                eta = (total - idx) / rate / 3600 if rate > 0 else 0

                logger.info("")
                logger.info("-" * 80)
                logger.info(
                    "[%d/%d] W%d | %s | rate=%.2f/s | ETA=%.1fh",
                    idx, total, worker_id, label, rate, eta,
                )

                # Rate limiting
                if self.config.delay_between_pages > 0 and idx > 1:
                    delay = self.config.delay_between_pages + random.uniform(
                        0, self.config.delay_jitter
                    )
                    await asyncio.sleep(delay)

                # Extract
                result: ExtractResult = await process_fn(page, record, self.config)

                # Update state
                async with self._lock:
                    if result.status == PageStatus.OK:
                        csv_rows = []
                        for rec in result.records:
                            csv_rows.extend(to_csv_rows_fn(rec))

                        self.state.add_success(url, result.records, csv_rows)
                        self.metrics.total_ok += 1
                        self.metrics.total_records += len(result.records)
                        self.metrics.total_csv_rows += len(csv_rows)
                        logger.info(
                            "STATUS=OK | records=%d | csv_rows=%d | total=%d",
                            len(result.records), len(csv_rows),
                            len(self.state.records),
                        )

                    elif result.status == PageStatus.FLAGGED:
                        self.state.add_flagged({
                            "url": url,
                            "label": label,
                            "reason": result.reason,
                            **result.meta,
                        })
                        self.metrics.total_flagged += 1
                        logger.info("STATUS=FLAGGED | %s", result.reason)

                    else:
                        self.state.add_failed({
                            "url": url,
                            "label": label,
                            "reason": result.reason,
                            **result.meta,
                        })
                        self.metrics.total_failed += 1
                        logger.info("STATUS=FAILED | %s", result.reason)

                    self.state.persist(
                        force=False,
                        checkpoint_every=self.config.checkpoint_every,
                        checkpoint_seconds=self.config.checkpoint_seconds,
                        csv_columns=csv_columns,
                    )

                queue.task_done()

        finally:
            await page.close()
            await ctx.close()
