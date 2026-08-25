import json
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from string import Template
from typing import Any, Literal

from .sampler import StreamSample


@dataclass(frozen=True)
class ReviewReportSummary:
    output_path: Path | None
    total_streams: int
    total_documents: int


class ReportGenerator:
    """Exports sampled stream documents to disk for manual or CI inspection."""

    def __init__(
        self,
        output_dir: str | Path = "reports",
        assets_dir: str | Path = "assets",
    ) -> None:
        self.output_dir = Path(output_dir)
        self.assets_dir = Path(assets_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._templates_loaded = False

    def _ensure_templates_loaded(self) -> None:
        """Lazy loader for HTML templates so non-HTML runs skip unnecessary disk I/O."""
        if self._templates_loaded:
            return

        self.tpl_base = Template(
            (self.assets_dir / "report_template.html").read_text(encoding="utf-8")
        )
        self.tpl_sidebar = Template(
            (self.assets_dir / "sidebar_item.html").read_text(encoding="utf-8")
        )
        self.tpl_card = Template(
            (self.assets_dir / "stream_card.html").read_text(encoding="utf-8")
        )
        self._templates_loaded = True

    def generate_report(
        self,
        samples: dict[str, StreamSample],
        base_filename: str = "stream_review",
        fmt: Literal["html", "json", "none"] = "none",
        volume_report: Any | None = None,
    ) -> ReviewReportSummary:
        """Writes report to disk if format is specified.

        Args:
            samples: Dictionary of stream samples.
            base_filename: Base prefix for the report file.
            fmt: Export target format. Use 'none' for head-less / CI executions.
                   If omitted, UTC timestamp formatted as YYYYMMDD_HHMMSS is generated.
        """
        total_docs = sum(len(s.documents) for s in samples.values())

        if fmt == "none":
            return ReviewReportSummary(
                output_path=None,
                total_streams=len(samples),
                total_documents=total_docs,
            )

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Generate unique filename using run_id or UTC timestamp
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_filename = f"{base_filename}_{suffix}"

        if fmt == "html":
            self._ensure_templates_loaded()
            filepath = self.output_dir / f"{unique_filename}.html"
            written_docs = self._write_html(filepath, samples, volume_report)
        elif fmt == "json":
            filepath = self.output_dir / f"{unique_filename}.json"
            written_docs = self._write_json(filepath, samples)
        else:
            raise ValueError(f"Unsupported report format: {fmt}")

        return ReviewReportSummary(
            output_path=filepath.resolve(),
            total_streams=len(samples),
            total_documents=written_docs,
        )

    def _write_html(
        self,
        filepath: Path,
        samples: dict[str, StreamSample],
        volume_report: Any | None = None,
    ) -> int:
        total_docs = 0
        sidebar_items: list[str] = []
        content_sections: list[str] = []

        # Map results by stream name if a volume report was generated
        vol_results = volume_report.results if volume_report else {}

        for stream_name, sample in samples.items():
            doc_count = len(sample.documents)
            total_docs += doc_count
            anchor_id = f"stream-{hash(stream_name) & 0xFFFFFFFF}"

            sidebar_items.append(
                self.tpl_sidebar.substitute(
                    anchor_id=anchor_id,
                    stream_name=escape(stream_name),
                    badge_class="active" if doc_count > 0 else "empty",
                    badge_text=f"{doc_count} docs" if doc_count > 0 else "empty",
                )
            )

            if not sample.documents:
                doc_html = (
                    '<div class="empty-state">'
                    "  No active documents sampled for this stream."
                    "</div>"
                )
            else:
                doc_blocks: list[str] = []
                for idx, doc in enumerate(sample.documents, 1):
                    ts = escape(str(doc.get("@timestamp", "N/A")))
                    json_raw = escape(json.dumps(doc, indent=2, ensure_ascii=False))
                    doc_blocks.append(
                        f'<details class="doc-wrapper" open>\n'
                        f'  <summary class="doc-summary">\n'
                        f'    Sample #{idx} <span class="ts">@timestamp: {ts}</span>\n'
                        f"  </summary>\n"
                        f'  <pre class="json-block"><code>{json_raw}</code></pre>\n'
                        f"</details>"
                    )
                doc_html = "\n".join(doc_blocks)

            # Extract volume result for this specific stream if available
            vol_info = vol_results.get(stream_name)
            if vol_info:
                weekly_cnt = vol_info.doc_count
                min_exp = vol_info.expected_min
                vol_class = "active" if vol_info.passed else "empty"
            else:
                weekly_cnt = "N/A"
                min_exp = "N/A"
                vol_class = "empty"

            content_sections.append(
                self.tpl_card.substitute(
                    anchor_id=anchor_id,
                    stream_name=escape(stream_name),
                    doc_count=doc_count,
                    documents_content=doc_html,
                    weekly_count=weekly_cnt,
                    min_expected=min_exp,
                    volume_status_class=vol_class,
                )
            )

        rendered_page = self.tpl_base.substitute(
            total_streams=len(samples),
            total_docs=total_docs,
            sidebar_items="\n".join(sidebar_items),
            content_sections="\n".join(content_sections),
        )

        filepath.write_text(rendered_page, encoding="utf-8")
        return total_docs

    def _write_json(self, filepath: Path, samples: dict[str, StreamSample]) -> int:
        total_docs = 0
        export_payload: dict[str, Any] = {}

        for stream_name, sample in samples.items():
            total_docs += len(sample.documents)
            export_payload[stream_name] = {
                "sample_count": len(sample.documents),
                "documents": sample.documents,
            }

        filepath.write_text(
            json.dumps(export_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return total_docs
