"""Custom metrics sink that sends events to an HTTP endpoint.

Demonstrates implementing the MetricsSink ABC for a custom destination.
Events are buffered and flushed in batches to reduce HTTP overhead.
"""

import json
import logging
from dataclasses import asdict
from urllib.request import Request, urlopen

from databricks4py import CompositeMetricsSink, LoggingMetricsSink, Workflow
from databricks4py.metrics import MetricEvent, MetricsSink

logger = logging.getLogger(__name__)


class HttpMetricsSink(MetricsSink):
    """Buffers metric events and POSTs them as JSON to a webhook URL.

    Events are accumulated in memory and sent in a single batch on flush().
    The Workflow.execute() lifecycle calls flush() automatically at the end
    of every job run — you don't need to call it yourself.
    """

    def __init__(self, endpoint_url: str, *, batch_size: int = 50) -> None:
        self._endpoint_url = endpoint_url
        self._batch_size = batch_size
        self._buffer: list[dict] = []

    def emit(self, event: MetricEvent) -> None:
        self._buffer.append(asdict(event))
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        payload = json.dumps(self._buffer, default=str).encode("utf-8")
        req = Request(
            self._endpoint_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=10) as resp:
                logger.info("Flushed %d metrics (status=%d)", len(self._buffer), resp.status)
        except Exception:
            logger.exception("Failed to flush %d metrics", len(self._buffer))
        finally:
            self._buffer.clear()


class InventorySync(Workflow):
    """Syncs inventory snapshots — used here to show metrics wiring."""

    def run(self) -> None:
        snapshot = self.spark.read.table("warehouse.bronze.inventory_snapshot")
        count = snapshot.count()
        self.emit_metric("sync_complete", row_count=count)


if __name__ == "__main__":
    http_sink = HttpMetricsSink("https://metrics.internal.example.com/v1/ingest")
    log_sink = LoggingMetricsSink()

    # CompositeMetricsSink fans out to both: HTTP for dashboards, logging
    # for Spark driver logs and structured log aggregation
    combined = CompositeMetricsSink(http_sink, log_sink)
    InventorySync(metrics=combined).execute()
