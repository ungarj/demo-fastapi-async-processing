from cached_property import cached_property
from dask.distributed import as_completed, Client
from functools import partial
from itertools import chain
import logging
import concurrent.futures
import os
from typing import Generator


logger = logging.getLogger(__name__)


class Job:
    """
    Wraps the output of a processing function into a generator with known length.

    This class also exposes the internal Executor.cancel() function in order to cancel all remaining
    tasks/futures.

    Will move into the mapchete core package.
    """

    def __init__(
        self,
        func: Generator,
        fargs: tuple = None,
        fkwargs: dict = None,
        as_iterator: bool = False,
        total: int = None,
        executor_concurrency: str = "processes",
        executor_kwargs: dict = None,
    ):
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.status = "pending"
        self.executor = None
        self.executor_concurrency = executor_concurrency
        self.executor_kwargs = executor_kwargs or {}
        self._total = total
        self._as_iterator = as_iterator
        if not as_iterator:
            self._results = list(self._run())

    def _run(self):
        with Executor(
            concurrency=self.executor_concurrency,
            **self.executor_kwargs
        ) as self.executor:
            self.status = "running"
            yield from self.func(self.executor, *self.fargs, **self.fkwargs)
            self.status = "finished"

    def cancel(self):
        if self._as_iterator:
            # requires client and futures
            if self.executor is None:
                raise ValueError("nothing to cancel because no executor is running")
            self.executor.cancel()
            self.status = "canceled"

    def __len__(self):
        return self._total

    def __iter__(self):
        if self._as_iterator:
            yield from self._run()
        else:
            return self._results

    def __repr__(self):  # pragma: no cover
        return f"<{self.status} Job with {self._total} tasks.>"


class Executor:
    """
    Executor factory for dask and concurrent.futures executor

    Will move into the mapchete core package.
    """
    def __new__(self, *args, concurrency=None, **kwargs):
        if concurrency == "dask":
            try:
                return DaskExecutor(*args, **kwargs)
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    f"this feature requires the mapchete[dask] extra: {e}"
                )
        elif concurrency in ["processes", "threads"]:
            return ConcurrentFuturesExecutor(*args, **kwargs)
        else:
            raise ValueError(f"concurrency must be one of 'processes', 'threads' or 'dask'")


class _ExecutorBase:
    """Define base methods and properties of executors."""

    futures = []
    _as_completed = None
    _executor = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def as_completed(self, func, iterable, fargs=None, fkwargs=None):
        """Yield finished tasks."""
        fargs = fargs or ()
        fkwargs = fkwargs or {}
        logger.debug(f"adding {len(iterable)} futures")
        futures = [
            self._executor.submit(func, *chain([item], fargs), **fkwargs)
            for item in iterable
        ]
        self.futures.extend(futures)
        logger.debug(f"added {len(futures)} futures")
        for future in self._as_completed(futures):
            logger.debug(future)
            yield future

    def cancel(self):
        logger.debug(f"cancel {len(self.futures)} futures...")
        for future in self.futures:
            future.cancel()
        logger.debug(f"{len(self.futures)} futures cancelled")

    def close(self):
        self.__exit__(None, None, None)

    def _as_completed(self, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def _executor(self):
        return self._executor_cls(*self._executor_args, **self._executor_kwargs)

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug(f"closing executor {self._executor}...")
        self._executor.__exit__(*args)
        logger.debug(f"closed executor {self._executor}")


class DaskExecutor(_ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        address=None,
        dask_scheduler=None,
        max_workers=None,
        **kwargs,
    ):
        from dask.distributed import LocalCluster
        from dask.distributed import Client

        local_cluster_kwargs = dict(
            n_workers=max_workers or os.cpu_count(), threads_per_worker=1
        )
        self._executor_cls = Client
        self._executor_kwargs = dict(
            address=dask_scheduler or LocalCluster(**local_cluster_kwargs)
        )
        logger.debug(f"starting dask.distributed.Client with kwargs {self._executor_kwargs}")

    def cancel(self):
        logger.debug(f"cancel {len(self.futures)} futures...")
        self._executor.cancel(self.futures)
        logger.debug(f"{len(self.futures)} futures cancelled")

    def _as_completed(self, futures):
        from dask.distributed import as_completed
        try:
            for future in as_completed(futures):
                yield future.result()
        except concurrent.futures._base.CancelledError:
            logger.debug("cancel execution!")

class ConcurrentFuturesExecutor(_ExecutorBase):
    """Execute tasks using concurrent.futures."""

    def __init__(
        self,
        *args,
        max_workers=None,
        concurrency="processes",
        **kwargs,
    ):
        """Set attributes."""
        self.max_workers = max_workers or os.cpu_count()
        logger.debug(f"init ConcurrentFuturesExecutor with {self.max_workers} workers")
        self.futures = []
        if concurrency == "processes":
            self._executor_cls = concurrent.futures.ProcessPoolExecutor
        elif concurrency == "threads":
            self._executor_cls = concurrent.futures.ThreadPoolExecutor
        else:
            raise ValueError(f"concurrency must be 'processes' or 'theads', not {concurrency}")

    def _as_completed(self, futures):
        """Yield finished tasks."""
        try:
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
        except concurrent.futures._base.CancelledError:
            logger.debug("cancel execution!")