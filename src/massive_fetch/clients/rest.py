"""Async wrapper over the synchronous ``massive`` SDK (SPEC §7).

The SDK (``massive-com/client-python``) is synchronous (urllib3) and owns retry
itself (urllib3 ``Retry`` with ``Retry-After`` handling — see SPEC §7.2 and
``SDK_NOTES.md``). This wrapper adds exactly three things on top:

1. an asyncio concurrency limit (a semaphore, default 3 from ``APIConfig``),
   bridging the sync SDK via a thread pool;
2. structured per-call logging (SPEC §11);
3. typed exception mapping — SDK/urllib3 errors become the ``MassiveClientError``
   hierarchy below and are re-raised. The wrapper never swallows them; the
   caller decides whether to skip a symbol or abort the job.

It performs **no** canonical normalization: ``list_aggs`` yields the SDK's ``Agg``
objects unchanged. Conversion to the canonical schema (ms→UTC ns, volume→int64,
symbol column) is ``transform/normalize.py``'s job (Slice 2+).
"""

from __future__ import annotations

import asyncio
import time
import weakref
from collections.abc import AsyncIterator, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal

import structlog
from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import Agg
from urllib3.exceptions import HTTPError

from massive_fetch.config import APIConfig

# We yield the SDK model directly rather than inventing a parallel dataclass.
Aggregate = Agg


# --- Typed exception hierarchy (SPEC §7.2) --------------------------------


class MassiveClientError(Exception):
    """Base for every error surfaced by this client. Callers catch this."""


class MassiveAuthError(MassiveClientError):
    """Missing/invalid API key at construction. Wraps SDK ``AuthError``."""


class MassiveBadRequest(MassiveClientError):
    """Non-retryable non-200 (400/401/403/404). Wraps SDK ``BadResponse``.

    The SDK does not expose the HTTP status code; the response body string is
    preserved as this exception's message for logging.
    """


class MassiveRetriesExhausted(MassiveClientError):
    """Transient failure (429/5xx/network/timeout) the SDK already retried and
    gave up on. Wraps urllib3 ``MaxRetryError`` / ``HTTPError``."""


# --- Multi-symbol fan-out spec (SPEC §7.4) ---------------------------------


@dataclass(frozen=True, slots=True)
class AggsRequest:
    """One ``list_aggs`` call, for fanning out across symbols via ``fetch_many``."""

    ticker: str
    multiplier: int
    timespan: Literal["minute", "day"]
    from_date: str  # 'YYYY-MM-DD'
    to_date: str  # 'YYYY-MM-DD'
    adjusted: bool = False  # RAW per SPEC §6.1
    sort: Literal["asc", "desc"] = "asc"


# --- Client ----------------------------------------------------------------


class MassiveRESTClient:
    """Concurrency-limited async facade over the synchronous ``massive`` SDK."""

    def __init__(
        self,
        api_key: str,
        config: APIConfig,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._config = config
        self._logger = logger
        try:
            self._sdk = RESTClient(
                api_key=api_key,
                retries=config.max_retries,
                connect_timeout=float(config.request_timeout_seconds),
                read_timeout=float(config.request_timeout_seconds),
                base=config.rest_base_url,
                pagination=True,
            )
        except AuthError as e:
            raise MassiveAuthError(str(e)) from e

        self._sem = asyncio.Semaphore(config.max_concurrent_requests)
        self._executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_requests,
            thread_name_prefix="massive-rest",
        )
        # Safety net if the caller forgets aclose()/`async with`.
        self._finalizer = weakref.finalize(self, self._executor.shutdown, False)

    async def __aenter__(self) -> "MassiveRESTClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Shut down the thread pool. Idempotent."""
        if self._finalizer.detach() is not None:
            self._executor.shutdown(wait=True)

    # -- aggregates ---------------------------------------------------------

    async def list_aggs(
        self,
        ticker: str,
        multiplier: int,
        timespan: Literal["minute", "day"],
        from_date: str,  # 'YYYY-MM-DD'
        to_date: str,  # 'YYYY-MM-DD'
        adjusted: bool = False,  # default RAW per SPEC §6.1 (SDK default is True)
        sort: Literal["asc", "desc"] = "asc",
    ) -> AsyncIterator[Aggregate]:
        """Yield aggregate bars for ``ticker`` over ``[from_date, to_date]``.

        The SDK auto-paginates; the full result is materialized in a worker
        thread (bounded by the semaphore) and then yielded. An unknown ticker
        yields nothing (the SDK returns an empty list — this is "no data", not
        an error).

        Being an async generator, this is lazy: the SDK call (and therefore any
        exception mapping or logging) does not fire until iteration begins.
        """
        fields = {
            "ticker": ticker,
            "multiplier": multiplier,
            "timespan": timespan,
            "from_date": from_date,
            "to_date": to_date,
        }
        start = time.monotonic()
        async with self._sem:
            try:
                bars = await asyncio.get_running_loop().run_in_executor(
                    self._executor,
                    self._list_aggs_sync,
                    ticker,
                    multiplier,
                    timespan,
                    from_date,
                    to_date,
                    adjusted,
                    sort,
                )
            except BadResponse as e:
                self._log_failure(start, e, **fields)
                raise MassiveBadRequest(str(e)) from e
            except HTTPError as e:
                # urllib3 MaxRetryError (and bare timeouts) after the SDK's
                # own retries were exhausted.
                self._log_failure(start, e, **fields)
                raise MassiveRetriesExhausted(str(e)) from e

        self._logger.debug(
            "api_call",
            response_time_ms=self._elapsed_ms(start),
            bar_count=len(bars),
            status="success",
            **fields,
        )
        for bar in bars:
            yield bar

    def _list_aggs_sync(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_date: str,
        to_date: str,
        adjusted: bool,
        sort: str,
    ) -> list[Agg]:
        """Blocking call run in a worker thread: walks all pages into a list."""
        return list(
            self._sdk.list_aggs(
                ticker,
                multiplier,
                timespan,
                from_date,
                to_date,
                adjusted=adjusted,
                sort=sort,
                limit=self._config.page_limit,
            )
        )

    async def fetch_many(
        self, requests: Iterable[AggsRequest]
    ) -> dict[str, list[Aggregate] | Exception]:
        """Fan out ``list_aggs`` across many tickers; return per-ticker results.

        Concurrency is bounded by this client's **existing** semaphore — every
        ``list_aggs`` acquires it — so no second semaphore is introduced (SPEC
        §7.4: "any ``asyncio.gather`` over client calls is already
        concurrency-bounded"). Effective parallelism is ``max_concurrent_requests``.

        Per-symbol errors are **captured, not raised**: each value is either the
        materialized list of bars or the exception that occurred. The caller
        decides what to do per result (skip the symbol, abort the run, …). If two
        requests share a ticker, the later one wins in the returned mapping.
        """
        reqs = list(requests)

        async def _collect(req: AggsRequest) -> list[Aggregate]:
            return [
                bar
                async for bar in self.list_aggs(
                    req.ticker,
                    req.multiplier,
                    req.timespan,
                    req.from_date,
                    req.to_date,
                    adjusted=req.adjusted,
                    sort=req.sort,
                )
            ]

        results = await asyncio.gather(
            *(_collect(r) for r in reqs), return_exceptions=True
        )
        return dict(zip((r.ticker for r in reqs), results))

    # -- logging helpers ----------------------------------------------------

    @staticmethod
    def _elapsed_ms(start: float) -> float:
        return round((time.monotonic() - start) * 1000, 1)

    def _log_failure(self, start: float, exc: Exception, **fields: object) -> None:
        self._logger.error(
            "api_call",
            response_time_ms=self._elapsed_ms(start),
            status=type(exc).__name__,
            error=str(exc),
            **fields,
        )
