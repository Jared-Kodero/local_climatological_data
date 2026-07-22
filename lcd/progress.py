"""
Progress reporting for long-running download and cleaning loops.

An interactive session (Jupyter kernel or a TTY) gets a ``rich`` progress bar;
a non-interactive session (log file, batch job) gets a thread-safe milestone
emitter that writes percentages in place and reports elapsed time on
completion. ``rich`` is optional; without it the milestone emitter is used
everywhere.
"""

from __future__ import annotations

import sys
import threading
import time
from types import TracebackType
from typing import IO, Optional, Type


def _format_elapsed(seconds: float) -> str:
    """Human-readable elapsed time (e.g. '3.2s', '1m 04s', '1h 02m')."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s:02d}s"
    h, rem = divmod(int(seconds), 3600)
    m = rem // 60
    return f"{h}h {m:02d}m"


class Progress:
    """Context-managed progress reporter with a ``rich`` or milestone backend.

    Parameters
    ----------
    total : int
        Number of work items.
    description : str
        Label shown alongside the bar or milestones.
    step_pct : int, default 5
        Milestone spacing for the non-interactive emitter.
    stream : IO[str], optional
        Output stream (defaults to stdout).
    """

    def __init__(
        self,
        total: int,
        description: str = "",
        step_pct: int = 5,
        stream: Optional[IO[str]] = None,
    ) -> None:
        self._total = int(total)
        self.description = description
        self.step_pct = max(1, int(step_pct))
        self._stream = stream if stream is not None else sys.stdout

        self._completed = 0
        self._last_emitted = -1
        self._lock = threading.Lock()
        self._start: Optional[float] = None
        self._elapsed: Optional[float] = None

        isatty = getattr(self._stream, "isatty", lambda: False)()
        self._interactive = ("ipykernel" in sys.modules) or bool(isatty)
        self._rich = None
        self._task = None

    # ------------------------------------------------------------- context

    def __enter__(self) -> Progress:
        self._start = time.monotonic()
        if self._interactive:
            self._rich = self._make_rich()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        if self._rich is not None:
            self._rich.stop()
        return False

    # -------------------------------------------------------------- update

    def advance(self, n: int = 1) -> None:
        """Advance the counter by ``n`` and refresh the display."""
        if self._rich is not None:
            self._rich.advance(self._task, n)
            return
        with self._lock:
            self._completed += n
            completed = self._completed
        self._emit_pct(completed)

    # ------------------------------------------------------------ backends

    def _make_rich(self):
        try:
            from rich.progress import (
                BarColumn,
                TaskProgressColumn,
                TextColumn,
                TimeElapsedColumn,
            )
            from rich.progress import (
                Progress as RichProgress,
            )
        except Exception:
            return None
        bar = RichProgress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            transient=False,
        )
        bar.start()
        self._task = bar.add_task(self.description, total=self._total)
        return bar

    def _emit_pct(self, completed: int) -> None:
        """Write the current milestone in place, no newline until completion."""
        if self._total == 0:
            return
        pct = (100 * completed) // self._total
        milestone = (pct // self.step_pct) * self.step_pct
        with self._lock:
            if milestone <= self._last_emitted:
                return
            self._last_emitted = milestone

            end_char = "\n" if milestone >= 100 else " "
            if milestone >= 100 and self._start is not None:
                self._elapsed = time.monotonic() - self._start
                label = f"{self.description} " if self.description else ""
                msg = f"{label}{milestone}% [Finished in: {_format_elapsed(self._elapsed)}]"
            else:
                msg = f"{milestone}%"

            print(msg, end=end_char, file=self._stream, flush=True)
