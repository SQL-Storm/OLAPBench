from typing import Any

from rich.console import Console
from rich.highlighter import ReprHighlighter
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, ProgressColumn
from rich.syntax import Syntax
from rich.table import Table, Column
from rich.text import Text

console = Console()
highlighter = ReprHighlighter()
progress = None

verbose = False
very_verbose = False
length = 80


def set_verbose(enable: bool):
    global verbose
    verbose = enable


def set_very_verbose(enable: bool):
    if enable:
        set_verbose(enable)

    global very_verbose
    very_verbose = enable


def log_group(info: Any, group: str, group_color: str):
    table = Table(show_header=False, box=None)
    table.add_column("c1", min_width=10)
    table.add_column("c2", overflow="fold")

    text = highlighter(info) if isinstance(info, str) else info
    table.add_row(f'[bold {group_color}]{group.upper()}[/]', text)

    console.log(table, _stack_offset=3)


def log_error(info: Any):
    log_group(str(info).strip(), "error", "red")


def log_error_verbose(info: Any):
    if verbose or very_verbose:
        log_group(str(info).strip(), "error", "red")


def log_info(info: Any):
    log_group(info, "info", "green")


def log_warn(info: Any):
    log_group(info, "warning", "bright_green")


def log_warn_verbose(info: Any):
    if verbose or very_verbose:
        log_warn(info)


def log_driver(info: str):
    log_group(info, "driver", "green")


def log_verbose_driver(info: str):
    if verbose or very_verbose:
        log_group(info, "driver", "green")


def log_verbose_benchmark(info: str, benchmark):
    if verbose or very_verbose:
        log_group(info, benchmark.name, "bright_red")


def log_dbms(info: str, dbms):
    log_group(info, dbms.name, "yellow")


def log_verbose_dbms(info: str, dbms):
    if verbose or very_verbose:
        log_group(info, dbms.name, "yellow")


def log_very_verbose_dbms(info: str, dbms):
    if very_verbose:
        log_group(info, dbms.name, "yellow")


def log_verbose_sql(info: str):
    if very_verbose:
        log_group(Syntax(info, "sql", background_color="default", word_wrap=True), "sql", "blue")


def log_verbose_process(info: str):
    if very_verbose:
        log_group(info, "process", "bright_blue")


def log_verbose_process_stderr(info: str):
    if very_verbose:
        log_group(info, "process", "bright_blue")


def log_header(text: str):
    console.rule(f'[bold]{text}[/]')


def log_header2(text: str):
    console.rule(f'{text}')


class LogProgress:
    class MofNCompleteColumn(ProgressColumn):
        def __init__(self, base: int):
            super().__init__(table_column=None)
            self._base = base

        def render(self, task: "Task") -> Text:
            completed = int(task.completed / self._base) + 1
            total = int(task.total / self._base) if task.total is not None else "?"
            total_width = len(str(total))
            return Text(f"[{completed:{total_width}d}/{total}]", style="progress.download")

    class TimeColumn(ProgressColumn):
        # Only refresh twice a second to prevent jitter
        max_refresh = 0.5

        def __init__(self):
            super().__init__()

        def render(self, task: "Task") -> Text:
            def text(prefix: str, time: float, style: str) -> Text:
                if time is None:
                    return Text("--:--", style=style)

                minutes, seconds = divmod(int(time), 60)
                hours, minutes = divmod(minutes, 60)

                if not hours:
                    formatted = f"{minutes:02d}:{seconds:02d}"
                else:
                    formatted = f"{hours:d}:{minutes:02d}:{seconds:02d}"

                return Text(f'{prefix}: {formatted}', style=style)

            remaining = text("remaining", task.time_remaining, "progress.remaining")
            elapsed = text("elapsed", task.elapsed, "progress.remaining")

            return Text.assemble("(", remaining, ", ", elapsed, ")")

    def __init__(self, info: str, total: int, base: int = 1):
        self._info = info
        self._total = total
        self._base = base

    def __enter__(self):
        self.progress = Progress(
            self.MofNCompleteColumn(self._base),
            TextColumn("[progress.description]{task.description}", table_column=Column(no_wrap=True, width=25)),
            BarColumn(),
            TaskProgressColumn(),
            self.TimeColumn(),
            transient=True,
            console=console,
        )
        self.progress.start()
        self.task = self.progress.add_task(self._info, total=self._total)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()

    def next(self, info: str):
        self.progress.update(self.task, description=info)

    def finish(self):
        self.progress.update(self.task, advance=1)
