import base64
import concurrent.futures
import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence
from urllib.parse import urlparse

import requests

from logger import logger


DEFAULT_KONTUR_BASE_URL = "https://mk.kontur.ru"
DEFAULT_WAREHOUSE_ID = "59739360-7d62-434b-ad13-4617c87a6d13"
DEFAULT_TRUE_API_PRODUCTION_BASE_URL = "https://markirovka.crpt.ru/api/v3/true-api"
DEFAULT_TRUE_API_SANDBOX_BASE_URL = "https://markirovka.sandbox.crptech.ru/api/v3/true-api"
DEFAULT_TRUE_API_BASE_URL = DEFAULT_TRUE_API_PRODUCTION_BASE_URL
DEFAULT_TRUE_API_PRODUCT_GROUP = "wheelchairs"
DEFAULT_KONTUR_PRODUCT_GROUP = "wheelChairs"
DEFAULT_BULK_AGGREGATION_MAX_WORKERS = 3

KONTUR_TO_TRUE_PRODUCT_GROUP = {
    "wheelChairs": "wheelchairs",
}

FAILED_DOCUMENT_STATUSES = {
    "CANCELLED",
    "CHECKED_NOT_OK",
    "OUTDATED",
    "PARSE_ERROR",
    "REJECTED",
}


def extract_sntin(full_code: str) -> str:
    full_code = (full_code or "").strip()
    if len(full_code) < 31:
        return full_code
    candidate = full_code[:31]
    if candidate.startswith("01") and candidate[16:18] == "21":
        return candidate

    gtin_pos = full_code.find("01")
    if gtin_pos >= 0 and len(full_code) >= gtin_pos + 31:
        candidate = full_code[gtin_pos:gtin_pos + 31]
        if candidate.startswith("01") and candidate[16:18] == "21":
            return candidate

    return full_code[:31]


def _chunks(items: Sequence[str], size: int) -> List[List[str]]:
    return [list(items[index:index + size]) for index in range(0, len(items), size)]


def _status_counts(states: Sequence["CodeState"]) -> str:
    counts: Dict[str, int] = {}
    for state in states:
        key = state.status or "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    return ", ".join(f"{status}: {count}" for status, count in sorted(counts.items()))


def _preview_items(items: Sequence[str], limit: int = 5) -> str:
    prepared = [str(item).strip() for item in items if str(item).strip()]
    if not prepared:
        return "-"
    if len(prepared) <= limit:
        return ", ".join(prepared)
    return ", ".join(prepared[:limit]) + f" ... (+{len(prepared) - limit})"


def _parse_env_bool(value: Optional[str]) -> Optional[bool]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _resolve_true_api_base_url(explicit_base_url: Optional[str]) -> tuple[str, str]:
    explicit = str(explicit_base_url or "").strip()
    if explicit:
        return explicit.rstrip("/"), "argument"

    env_base_url = str(os.getenv("TRUE_API_BASE_URL") or "").strip()
    if env_base_url:
        return env_base_url.rstrip("/"), "env:TRUE_API_BASE_URL"

    for env_name in ("TRUE_API_SANDBOX", "CRPT_SANDBOX"):
        env_value = os.getenv(env_name)
        parsed = _parse_env_bool(env_value)
        if parsed is True:
            return DEFAULT_TRUE_API_SANDBOX_BASE_URL, f"default sandbox via {env_name}"
        if parsed is False:
            return DEFAULT_TRUE_API_PRODUCTION_BASE_URL, f"default production via {env_name}"
        if env_value:
            logger.warning(
                "Переменная окружения %s=%r не распознана как bool, используем production True API",
                env_name,
                env_value,
            )

    return DEFAULT_TRUE_API_PRODUCTION_BASE_URL, "default production"


def _resolve_bulk_aggregation_workers(explicit_max_workers: Optional[int]) -> int:
    if explicit_max_workers is not None:
        return max(1, int(explicit_max_workers))

    env_value = str(os.getenv("BULK_AGGREGATION_MAX_WORKERS") or "").strip()
    if not env_value:
        return DEFAULT_BULK_AGGREGATION_MAX_WORKERS
    try:
        return max(1, int(env_value))
    except ValueError:
        logger.warning(
            "Переменная окружения BULK_AGGREGATION_MAX_WORKERS=%r некорректна, используем %s",
            env_value,
            DEFAULT_BULK_AGGREGATION_MAX_WORKERS,
        )
        return DEFAULT_BULK_AGGREGATION_MAX_WORKERS


@dataclass(frozen=True)
class AggregateInfo:
    document_id: str
    aggregate_code: str
    comment: str = ""
    status: str = ""
    product_group: str = DEFAULT_KONTUR_PRODUCT_GROUP
    includes_units_count: int = 0
    codes_check_errors_count: int = 0


@dataclass(frozen=True)
class CodeState:
    raw_code: str
    sntin: str
    status: str
    parent: Optional[str] = None
    owner_inn: Optional[str] = None
    requested_cis: Optional[str] = None
    api_error: Optional[str] = None


@dataclass(frozen=True)
class KonturSignContent:
    document_id: str
    base64_content: str
    participant_id: Optional[str]
    payload: Dict[str, Any]


@dataclass
class BulkAggregationSummary:
    ready_found: int = 0
    processed: int = 0
    sent_for_approve: int = 0
    skipped_due_to_status: int = 0
    skipped_empty: int = 0
    skipped_not_ready: int = 0
    skipped_unsupported: int = 0
    errors: int = 0
    disaggregated_parent_codes: set[str] = field(default_factory=set)

    @property
    def disaggregated_parents(self) -> int:
        return len(self.disaggregated_parent_codes)

    def to_lines(self) -> List[str]:
        return [
            f"Найдено readyForSend АК: {self.ready_found}",
            f"Обработано АК: {self.processed}",
            f"Отправлено на подпись: {self.sent_for_approve}",
            f"Пропущено из-за статусов КМ: {self.skipped_due_to_status}",
            f"Пропущено без кодов: {self.skipped_empty}",
            f"Пропущено как неподдерживаемые: {self.skipped_unsupported}",
            f"Пропущено не readyForSend: {self.skipped_not_ready}",
            f"Расформировано чужих АК: {self.disaggregated_parents}",
            f"Ошибок: {self.errors}",
        ]


@dataclass
class AggregateProcessingResult:
    aggregate: AggregateInfo
    summary: BulkAggregationSummary
    error: Optional[Exception] = None
    connectivity_error: Optional["TrueApiConnectivityError"] = None


class TrueApiConnectivityError(RuntimeError):
    """Сетевая недоступность True API, при которой продолжать прогон бессмысленно."""


class BulkAggregationService:
    def __init__(
        self,
        *,
        kontur_base_url: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        true_api_base_url: Optional[str] = None,
        true_api_product_group: Optional[str] = None,
        page_size: int = 100,
        batch_size: int = 1000,
        poll_interval_seconds: float = 2.0,
        document_timeout_seconds: float = 180.0,
        parent_clear_timeout_seconds: float = 120.0,
        kontur_send_timeout_seconds: float = 60.0,
        max_workers: Optional[int] = None,
        sleep_func: Callable[[float], None] = time.sleep,
        true_api_session: Optional[requests.Session] = None,
    ):
        self.kontur_base_url = (
            kontur_base_url or os.getenv("BASE_URL", DEFAULT_KONTUR_BASE_URL)
        ).rstrip("/")
        self.warehouse_id = warehouse_id or os.getenv("WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)
        self.true_api_base_url, self.true_api_base_url_source = _resolve_true_api_base_url(
            true_api_base_url
        )
        self.true_api_info_base_url = self._build_info_base_url(self.true_api_base_url)
        self.true_api_product_group = (
            true_api_product_group
            or os.getenv("TRUE_API_PRODUCT_GROUP", DEFAULT_TRUE_API_PRODUCT_GROUP)
        )
        self.page_size = page_size
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.document_timeout_seconds = document_timeout_seconds
        self.parent_clear_timeout_seconds = parent_clear_timeout_seconds
        self.kontur_send_timeout_seconds = kontur_send_timeout_seconds
        self.max_workers = _resolve_bulk_aggregation_workers(max_workers)
        self.sleep_func = sleep_func
        self.true_api_session = true_api_session or requests.Session()
        self._true_api_token: Optional[str] = None
        self._true_api_token_expires_at = 0.0
        logger.info(
            "BulkAggregationService инициализирован: kontur_base_url=%s, warehouse_id=%s, true_api_base_url=%s, true_api_info_base_url=%s, true_api_base_url_source=%s, page_size=%s, batch_size=%s, max_workers=%s",
            self.kontur_base_url,
            self.warehouse_id,
            self.true_api_base_url,
            self.true_api_info_base_url,
            self.true_api_base_url_source,
            self.page_size,
            self.batch_size,
            self.max_workers,
        )
        self._warn_if_true_api_base_url_looks_suspicious()

    def run(
        self,
        *,
        kontur_session: requests.Session,
        cert_provider: Callable[[], Any],
        sign_base64_func: Callable[[Any, str, bool], str],
        sign_text_func: Callable[[Any, str, bool], str],
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        confirm_callback: Optional[Callable[[str, str], bool]] = None,
        comment_filter: Optional[str] = None,
    ) -> BulkAggregationSummary:
        if not cert_provider():
            raise RuntimeError("Сертификат для подписи не найден")

        logger.info(
            "Запуск массового проведения АК: comment_filter=%r, true_api_base_url=%s, max_workers=%s",
            comment_filter,
            self.true_api_base_url,
            self.max_workers,
        )
        log = log_callback or (lambda message: None)
        progress = progress_callback or (lambda processed, total: None)
        confirm = confirm_callback or (lambda title, message: False)
        summary = BulkAggregationSummary()
        ready_aggregates = self.list_ready_aggregates(
            kontur_session,
            comment_filter=comment_filter,
        )
        summary.ready_found = len(ready_aggregates)
        logger.info(
            "Для проведения найдено readyForSend АК: %s",
            summary.ready_found,
        )

        worker_count = min(self.max_workers, len(ready_aggregates)) if ready_aggregates else 1
        if worker_count <= 1:
            self._run_sequential(
                ready_aggregates=ready_aggregates,
                kontur_session=kontur_session,
                cert_provider=cert_provider,
                sign_base64_func=sign_base64_func,
                sign_text_func=sign_text_func,
                log=log,
                progress=progress,
                confirm=confirm,
                summary=summary,
            )
        else:
            self._run_parallel(
                ready_aggregates=ready_aggregates,
                worker_count=worker_count,
                kontur_session=kontur_session,
                cert_provider=cert_provider,
                sign_base64_func=sign_base64_func,
                sign_text_func=sign_text_func,
                log=log,
                progress=progress,
                confirm=confirm,
                summary=summary,
            )

        progress(summary.ready_found or summary.processed, summary.ready_found or summary.processed or 1)
        logger.info(
            "Массовое проведение АК завершено: ready_found=%s, processed=%s, sent_for_approve=%s, skipped_due_to_status=%s, skipped_empty=%s, skipped_not_ready=%s, skipped_unsupported=%s, disaggregated=%s, errors=%s",
            summary.ready_found,
            summary.processed,
            summary.sent_for_approve,
            summary.skipped_due_to_status,
            summary.skipped_empty,
            summary.skipped_not_ready,
            summary.skipped_unsupported,
            summary.disaggregated_parents,
            summary.errors,
        )
        return summary

    def _run_sequential(
        self,
        *,
        ready_aggregates: Sequence[AggregateInfo],
        kontur_session: requests.Session,
        cert_provider: Callable[[], Any],
        sign_base64_func: Callable[[Any, str, bool], str],
        sign_text_func: Callable[[Any, str, bool], str],
        log: Callable[[str], None],
        progress: Callable[[int, int], None],
        confirm: Callable[[str, str], bool],
        summary: BulkAggregationSummary,
    ) -> None:
        for aggregate in ready_aggregates:
            local_summary = BulkAggregationSummary()
            processed_accounted = False
            cert = cert_provider()
            if not cert:
                raise RuntimeError("Сертификат для подписи не найден")
            logger.info(
                "Обработка АК %s (%s/%s), document_id=%s, comment=%r",
                aggregate.aggregate_code,
                summary.processed + 1,
                summary.ready_found,
                aggregate.document_id,
                aggregate.comment,
            )
            try:
                self._process_aggregate(
                    kontur_session=kontur_session,
                    aggregate=aggregate,
                    cert=cert,
                    sign_base64_func=sign_base64_func,
                    sign_text_func=sign_text_func,
                    log=log,
                    confirm=confirm,
                    summary=local_summary,
                )
                self._merge_summary(summary, local_summary)
            except TrueApiConnectivityError as exc:
                summary.errors += 1
                logger.exception("Ошибка массового проведения АК %s", aggregate.aggregate_code)
                log(f"❌ {aggregate.aggregate_code}: {exc}")
                log("⛔ Проведение остановлено: True API недоступен, повторите позже")
                summary.processed += 1
                processed_accounted = True
                progress(summary.processed, summary.ready_found or summary.processed)
                break
            except Exception as exc:
                summary.errors += 1
                logger.exception("Ошибка массового проведения АК %s", aggregate.aggregate_code)
                log(f"❌ {aggregate.aggregate_code}: {exc}")
            finally:
                if not processed_accounted:
                    summary.processed += 1
                    progress(summary.processed, summary.ready_found or summary.processed)

    def _run_parallel(
        self,
        *,
        ready_aggregates: Sequence[AggregateInfo],
        worker_count: int,
        kontur_session: requests.Session,
        cert_provider: Callable[[], Any],
        sign_base64_func: Callable[[Any, str, bool], str],
        sign_text_func: Callable[[Any, str, bool], str],
        log: Callable[[str], None],
        progress: Callable[[int, int], None],
        confirm: Callable[[str, str], bool],
        summary: BulkAggregationSummary,
    ) -> None:
        logger.info(
            "Запускаем параллельное проведение АК: worker_count=%s, ready_found=%s",
            worker_count,
            len(ready_aggregates),
        )
        stop_event = threading.Event()
        confirm_lock = threading.Lock()
        thread_local = threading.local()
        futures: Dict[concurrent.futures.Future, AggregateInfo] = {}
        aggregate_iter = iter(ready_aggregates)

        def confirm_serialized(title: str, message: str) -> bool:
            with confirm_lock:
                return confirm(title, message)

        def get_worker_context() -> Dict[str, Any]:
            context = getattr(thread_local, "context", None)
            if context is None:
                cert = cert_provider()
                if not cert:
                    raise RuntimeError("Сертификат для подписи не найден")
                context = {
                    "cert": cert,
                    "kontur_session": self._clone_requests_session(kontur_session),
                    "service": self._create_worker_service(),
                }
                thread_local.context = context
                logger.info(
                    "Инициализирован поток проведения АК: thread=%s",
                    threading.current_thread().name,
                )
            return context

        def process_aggregate(aggregate: AggregateInfo) -> AggregateProcessingResult:
            if stop_event.is_set():
                return AggregateProcessingResult(
                    aggregate=aggregate,
                    summary=BulkAggregationSummary(),
                )
            context = get_worker_context()
            local_summary = BulkAggregationSummary()
            try:
                context["service"]._process_aggregate(
                    kontur_session=context["kontur_session"],
                    aggregate=aggregate,
                    cert=context["cert"],
                    sign_base64_func=sign_base64_func,
                    sign_text_func=sign_text_func,
                    log=log,
                    confirm=confirm_serialized,
                    summary=local_summary,
                )
                return AggregateProcessingResult(
                    aggregate=aggregate,
                    summary=local_summary,
                )
            except TrueApiConnectivityError as exc:
                stop_event.set()
                return AggregateProcessingResult(
                    aggregate=aggregate,
                    summary=local_summary,
                    connectivity_error=exc,
                )
            except Exception as exc:
                return AggregateProcessingResult(
                    aggregate=aggregate,
                    summary=local_summary,
                    error=exc,
                )

        def submit_next(executor: concurrent.futures.ThreadPoolExecutor) -> bool:
            if stop_event.is_set():
                return False
            try:
                aggregate = next(aggregate_iter)
            except StopIteration:
                return False
            future = executor.submit(process_aggregate, aggregate)
            futures[future] = aggregate
            logger.info(
                "АК %s поставлен в очередь на проведение (%s/%s)",
                aggregate.aggregate_code,
                len(futures),
                worker_count,
            )
            return True

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="bulk-agg",
        ) as executor:
            for _ in range(min(worker_count, len(ready_aggregates))):
                submit_next(executor)

            connectivity_stop_logged = False
            while futures:
                done, _ = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    aggregate = futures.pop(future)
                    result = future.result()
                    self._merge_summary(summary, result.summary)
                    summary.processed += 1

                    if result.connectivity_error is not None:
                        summary.errors += 1
                        logger.exception(
                            "Ошибка массового проведения АК %s",
                            aggregate.aggregate_code,
                            exc_info=(
                                type(result.connectivity_error),
                                result.connectivity_error,
                                result.connectivity_error.__traceback__,
                            ),
                        )
                        log(f"❌ {aggregate.aggregate_code}: {result.connectivity_error}")
                        if not connectivity_stop_logged:
                            log("⛔ Проведение остановлено: True API недоступен, повторите позже")
                            connectivity_stop_logged = True
                    elif result.error is not None:
                        summary.errors += 1
                        logger.exception(
                            "Ошибка массового проведения АК %s",
                            aggregate.aggregate_code,
                            exc_info=(
                                type(result.error),
                                result.error,
                                result.error.__traceback__,
                            ),
                        )
                        log(f"❌ {aggregate.aggregate_code}: {result.error}")

                    progress(summary.processed, summary.ready_found or summary.processed)

                if stop_event.is_set():
                    logger.warning(
                        "Параллельное проведение АК останавливается после инфраструктурной ошибки True API"
                    )
                    continue

                while len(futures) < worker_count and submit_next(executor):
                    pass

    def list_ready_aggregates(
        self,
        kontur_session: requests.Session,
        *,
        comment_filter: Optional[str] = None,
    ) -> List[AggregateInfo]:
        matched: List[AggregateInfo] = []
        normalized_filter = self._normalize_comment_filter(comment_filter)
        offset = 0
        logger.info(
            "Начинаем загрузку readyForSend АК из Контур.Маркировки: comment_filter=%r",
            normalized_filter,
        )

        while True:
            aggregates, total = self.fetch_ready_aggregates_page(kontur_session, offset)
            page_size = len(aggregates)
            if not aggregates:
                logger.info("Страница АК offset=%s пустая, завершаем загрузку", offset)
                break

            page_aggregates = aggregates
            if normalized_filter:
                aggregates = [
                    aggregate
                    for aggregate in aggregates
                    if self._matches_comment_filter(aggregate.comment, normalized_filter)
                ]

            matched.extend(aggregates)
            logger.info(
                "Загружена страница АК: offset=%s, total=%s, page_size=%s, matched_after_filter=%s",
                offset,
                total,
                page_size,
                len(aggregates),
            )
            if normalized_filter and len(aggregates) != len(page_aggregates):
                logger.info(
                    "Фильтр comment_filter=%r отбросил %s АК на странице offset=%s",
                    normalized_filter,
                    len(page_aggregates) - len(aggregates),
                    offset,
                )
            offset += page_size
            if total and offset >= total:
                break
            if page_size < self.page_size:
                break

        logger.info("Загрузка readyForSend АК завершена: matched=%s", len(matched))
        return matched

    def fetch_ready_aggregates_page(
        self,
        kontur_session: requests.Session,
        offset: int,
    ) -> tuple[List[AggregateInfo], int]:
        response = kontur_session.get(
            f"{self.kontur_base_url}/api/v1/aggregates",
            params={
                "warehouseId": self.warehouse_id,
                "limit": self.page_size,
                "offset": offset,
                "statuses": "readyForSend",
                "sortField": "createDate",
                "sortOrder": "descending",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items") or []
        aggregates = [
            AggregateInfo(
                document_id=str(item.get("documentId") or ""),
                aggregate_code=str(item.get("aggregateCode") or ""),
                comment=str(item.get("comment") or ""),
                status=str(item.get("status") or ""),
                product_group=str(item.get("productGroup") or DEFAULT_KONTUR_PRODUCT_GROUP),
                includes_units_count=int(item.get("includesUnitsCount") or 0),
                codes_check_errors_count=int(item.get("codesCheckErrorsCount") or 0),
            )
            for item in items
            if item.get("documentId") and item.get("aggregateCode")
        ]
        total = int(payload.get("total") or len(aggregates))
        return aggregates, total

    def _create_worker_service(self) -> "BulkAggregationService":
        return BulkAggregationService(
            kontur_base_url=self.kontur_base_url,
            warehouse_id=self.warehouse_id,
            true_api_base_url=self.true_api_base_url,
            true_api_product_group=self.true_api_product_group,
            page_size=self.page_size,
            batch_size=self.batch_size,
            poll_interval_seconds=self.poll_interval_seconds,
            document_timeout_seconds=self.document_timeout_seconds,
            parent_clear_timeout_seconds=self.parent_clear_timeout_seconds,
            kontur_send_timeout_seconds=self.kontur_send_timeout_seconds,
            max_workers=1,
            sleep_func=self.sleep_func,
            true_api_session=self._clone_requests_session(self.true_api_session),
        )

    @staticmethod
    def _clone_requests_session(session: Any) -> Any:
        if not isinstance(session, requests.Session):
            return session
        cloned = requests.Session()
        cloned.headers.update(dict(session.headers))
        cloned.cookies.update(session.cookies)
        cloned.auth = session.auth
        cloned.verify = session.verify
        cloned.cert = session.cert
        cloned.proxies = dict(session.proxies)
        cloned.hooks = {
            key: list(value)
            for key, value in (session.hooks or {}).items()
        }
        cloned.params = dict(session.params or {})
        cloned.trust_env = session.trust_env
        cloned.max_redirects = session.max_redirects
        return cloned

    @staticmethod
    def _merge_summary(target: BulkAggregationSummary, source: BulkAggregationSummary) -> None:
        target.sent_for_approve += source.sent_for_approve
        target.skipped_due_to_status += source.skipped_due_to_status
        target.skipped_empty += source.skipped_empty
        target.skipped_not_ready += source.skipped_not_ready
        target.skipped_unsupported += source.skipped_unsupported
        target.disaggregated_parent_codes.update(source.disaggregated_parent_codes)

    def _process_aggregate(
        self,
        *,
        kontur_session: requests.Session,
        aggregate: AggregateInfo,
        cert: Any,
        sign_base64_func: Callable[[Any, str, bool], str],
        sign_text_func: Callable[[Any, str, bool], str],
        log: Callable[[str], None],
        confirm: Callable[[str, str], bool],
        summary: BulkAggregationSummary,
    ) -> None:
        log(f"▶️ Проверяем АК {aggregate.aggregate_code} ({aggregate.comment or 'без названия'})")
        logger.info(
            "АК %s: старт обработки, document_id=%s, product_group=%s, comment=%r",
            aggregate.aggregate_code,
            aggregate.document_id,
            aggregate.product_group,
            aggregate.comment,
        )

        detail = self.fetch_aggregate_detail(kontur_session, aggregate.document_id)
        logger.info(
            "АК %s: получены детали, status=%s, includes_units_count=%s, codes_check_errors_count=%s",
            aggregate.aggregate_code,
            detail.status,
            detail.includes_units_count,
            detail.codes_check_errors_count,
        )
        if detail.status != "readyForSend":
            summary.skipped_not_ready += 1
            log(f"⏭️ {aggregate.aggregate_code}: статус уже изменился на {detail.status}")
            return

        raw_codes, reaggregation_codes = self.fetch_aggregate_codes(kontur_session, aggregate.document_id)
        logger.info(
            "АК %s: загружены коды, km_count=%s, nested_ak_count=%s",
            aggregate.aggregate_code,
            len(raw_codes),
            len(reaggregation_codes),
        )
        if reaggregation_codes:
            summary.skipped_unsupported += 1
            log(
                f"⏭️ {aggregate.aggregate_code}: содержит вложенные АК "
                f"({len(reaggregation_codes)} шт.), этот сценарий в v1 не поддержан"
            )
            return

        if not raw_codes:
            summary.skipped_empty += 1
            log(f"⏭️ {aggregate.aggregate_code}: в Контуре нет кодов для проведения")
            return

        log(f"• {aggregate.aggregate_code}: найдено КМ {len(raw_codes)}")
        logger.info(
            "АК %s: запрашиваем статусы КМ в True API, product_group=%s, codes=%s",
            aggregate.aggregate_code,
            self._resolve_true_product_group(aggregate.product_group),
            len(raw_codes),
        )
        states = self.fetch_code_states(
            cert=cert,
            sign_text_func=sign_text_func,
            product_group=self._resolve_true_product_group(aggregate.product_group),
            raw_codes=raw_codes,
        )

        errored = [state for state in states if state.api_error]
        foreign_parents = sorted({
            (state.parent or "").strip()
            for state in states
            if state.parent and state.parent.strip() and state.parent.strip() != aggregate.aggregate_code
        })
        not_introduced = [state for state in states if state.status != "INTRODUCED"]
        logger.info(
            "АК %s: статусы КМ получены, total=%s, statuses=%s, api_errors=%s, not_introduced=%s, foreign_parents=%s",
            aggregate.aggregate_code,
            len(states),
            _status_counts(states),
            len(errored),
            len(not_introduced),
            _preview_items(foreign_parents),
        )
        if errored:
            raise RuntimeError(
                "True API вернул ошибки по кодам: "
                + "; ".join(
                    f"{state.sntin}: {state.api_error}"
                    for state in errored[:5]
                )
            )

        if not_introduced:
            summary.skipped_due_to_status += 1
            log(
                f"⏭️ {aggregate.aggregate_code}: КМ не готовы к проведению "
                f"({_status_counts(not_introduced)})"
            )
            logger.info(
                "АК %s: пропускаем из-за статусов КМ %s",
                aggregate.aggregate_code,
                _status_counts(not_introduced),
            )
            if foreign_parents:
                message = (
                    f"АК {aggregate.aggregate_code} содержит КМ со статусами "
                    f"{_status_counts(not_introduced)}.\n\n"
                    f"При этом часть КМ уже привязана к другим АК: {', '.join(foreign_parents)}.\n"
                    f"Расформировать эти АК сейчас?\n\n"
                    f"Текущий АК всё равно будет пропущен до следующего запуска."
                )
                if confirm("Расформировать чужие АК", message):
                    logger.info(
                        "АК %s: пользователь подтвердил расформирование чужих АК: %s",
                        aggregate.aggregate_code,
                        _preview_items(foreign_parents),
                    )
                    participant_inn = self.resolve_participant_inn(
                        kontur_session=kontur_session,
                        aggregate=aggregate,
                        states=states,
                    )
                    disaggregated = self.disaggregate_parents(
                        cert=cert,
                        sign_text_func=sign_text_func,
                        product_group=self._resolve_true_product_group(aggregate.product_group),
                        participant_inn=participant_inn,
                        parent_codes=foreign_parents,
                        log=log,
                    )
                    summary.disaggregated_parent_codes.update(disaggregated)
                    log(f"ℹ️ {aggregate.aggregate_code}: текущий АК пропущен, повторите запуск позже")
                else:
                    logger.info(
                        "АК %s: пользователь отменил расформирование чужих АК: %s",
                        aggregate.aggregate_code,
                        _preview_items(foreign_parents),
                    )
                    log(f"ℹ️ {aggregate.aggregate_code}: расформирование отменено пользователем")
            return

        if foreign_parents:
            logger.info(
                "АК %s: обнаружены чужие родительские АК, начинаем расформирование: %s",
                aggregate.aggregate_code,
                _preview_items(foreign_parents),
            )
            participant_inn = self.resolve_participant_inn(
                kontur_session=kontur_session,
                aggregate=aggregate,
                states=states,
            )
            disaggregated = self.disaggregate_parents(
                cert=cert,
                sign_text_func=sign_text_func,
                product_group=self._resolve_true_product_group(aggregate.product_group),
                participant_inn=participant_inn,
                parent_codes=foreign_parents,
                log=log,
            )
            summary.disaggregated_parent_codes.update(disaggregated)
            self.wait_until_parents_cleared(
                cert=cert,
                sign_text_func=sign_text_func,
                product_group=self._resolve_true_product_group(aggregate.product_group),
                raw_codes=raw_codes,
                current_aggregate_code=aggregate.aggregate_code,
                log=log,
            )

        logger.info("АК %s: отправляем в Контур на подпись", aggregate.aggregate_code)
        final_detail = self.send_aggregate_for_approve(
            kontur_session=kontur_session,
            aggregate=aggregate,
            cert=cert,
            sign_base64_func=sign_base64_func,
        )
        summary.sent_for_approve += 1
        logger.info(
            "АК %s: успешно отправлен на подпись, final_status=%s",
            aggregate.aggregate_code,
            final_detail.status,
        )
        log(
            f"✅ {aggregate.aggregate_code}: отправлен в Контур на подпись, "
            f"текущий статус {final_detail.status}"
        )

    def fetch_aggregate_detail(
        self,
        kontur_session: requests.Session,
        document_id: str,
    ) -> AggregateInfo:
        response = kontur_session.get(
            f"{self.kontur_base_url}/api/v1/aggregates/{document_id}",
            timeout=30,
        )
        response.raise_for_status()
        item = response.json()
        return AggregateInfo(
            document_id=str(item.get("documentId") or document_id),
            aggregate_code=str(item.get("aggregateCode") or ""),
            comment=str(item.get("comment") or ""),
            status=str(item.get("status") or ""),
            product_group=str(item.get("productGroup") or DEFAULT_KONTUR_PRODUCT_GROUP),
            includes_units_count=int(item.get("includesUnitsCount") or 0),
            codes_check_errors_count=int(item.get("codesCheckErrorsCount") or 0),
        )

    def fetch_aggregate_codes(
        self,
        kontur_session: requests.Session,
        document_id: str,
    ) -> tuple[List[str], List[str]]:
        response = kontur_session.get(
            f"{self.kontur_base_url}/api/v1/aggregates/{document_id}/codes",
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        aggregate_codes = [
            str(item.get("ttisCode") or item.get("aggregateCode") or "").strip()
            for item in payload.get("aggregateCodes") or []
            if (item.get("ttisCode") or item.get("aggregateCode"))
        ]
        reaggregation_codes = [
            str(item.get("ttisCode") or item.get("aggregateCode") or "").strip()
            for item in payload.get("reaggregationCodes") or []
            if (item.get("ttisCode") or item.get("aggregateCode"))
        ]
        return aggregate_codes, reaggregation_codes

    def fetch_code_states(
        self,
        *,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
        product_group: str,
        raw_codes: Sequence[str],
    ) -> List[CodeState]:
        normalized = [extract_sntin(code) for code in raw_codes]
        unique_codes = list(dict.fromkeys(normalized))
        by_sntin: Dict[str, Dict[str, Any]] = {}
        total_chunks = len(_chunks(unique_codes, self.batch_size))
        logger.info(
            "True API: начинаем получение статусов КМ, product_group=%s, raw_codes=%s, unique_codes=%s, chunks=%s",
            product_group,
            len(raw_codes),
            len(unique_codes),
            total_chunks,
        )

        for chunk_index, chunk in enumerate(_chunks(unique_codes, self.batch_size), start=1):
            logger.info(
                "True API: обрабатываем chunk %s/%s для статусов КМ, chunk_size=%s",
                chunk_index,
                total_chunks,
                len(chunk),
            )
            token = self.get_true_api_token(cert, sign_text_func)
            response = self._true_api_post(
                f"{self.true_api_base_url}/cises/short/list",
                params={"pg": product_group},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=chunk,
                timeout=30,
            )
            response.raise_for_status()
            items = response.json()
            if not isinstance(items, list):
                raise RuntimeError("Некорректный ответ True API при получении статусов КМ")
            logger.info(
                "True API: chunk %s/%s обработан, result_items=%s",
                chunk_index,
                total_chunks,
                len(items),
            )
            for item in items:
                result = item.get("result") if isinstance(item, dict) and isinstance(item.get("result"), dict) else item
                if not isinstance(result, dict):
                    continue
                requested = result.get("requestedCis") or result.get("cis")
                if not requested:
                    continue
                by_sntin[extract_sntin(str(requested))] = result

        states: List[CodeState] = []
        for raw_code in raw_codes:
            sntin = extract_sntin(raw_code)
            result = by_sntin.get(sntin)
            if not result:
                states.append(
                    CodeState(
                        raw_code=raw_code,
                        sntin=sntin,
                        status="UNKNOWN",
                        api_error="Код не найден в True API",
                    )
                )
                continue
            states.append(
                CodeState(
                    raw_code=raw_code,
                    sntin=sntin,
                    status=str(result.get("status") or "UNKNOWN"),
                    parent=self._clean_optional_string(result.get("parent")),
                    owner_inn=self._clean_optional_string(result.get("ownerInn")),
                    requested_cis=self._clean_optional_string(result.get("requestedCis") or result.get("cis")),
                    api_error=self._extract_result_error(result),
                )
            )
        logger.info(
            "True API: получение статусов КМ завершено, total_states=%s, statuses=%s, api_errors=%s",
            len(states),
            _status_counts(states),
            sum(1 for state in states if state.api_error),
        )
        return states

    def get_true_api_token(
        self,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
    ) -> str:
        now = time.monotonic()
        if self._true_api_token and now < self._true_api_token_expires_at:
            logger.debug("True API: используем закешированный bearer token")
            return self._true_api_token

        logger.info("True API: запрашиваем новый bearer token")
        auth_key_response = self._true_api_get(
            f"{self.true_api_base_url}/auth/key",
            timeout=15,
        )
        auth_key_response.raise_for_status()
        auth_key = auth_key_response.json()
        signature = sign_text_func(cert, str(auth_key["data"]), False)
        token_response = self._true_api_post(
            f"{self.true_api_base_url}/auth/simpleSignIn",
            json={
                "uuid": auth_key["uuid"],
                "data": signature,
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=15,
        )
        token_response.raise_for_status()
        token_payload = token_response.json()
        token = token_payload.get("token")
        if not token:
            raise RuntimeError("True API не вернул bearer token")
        self._true_api_token = str(token)
        self._true_api_token_expires_at = now + 8 * 60 * 60
        logger.info("True API: новый bearer token успешно получен")
        return self._true_api_token

    def resolve_participant_inn(
        self,
        *,
        kontur_session: requests.Session,
        aggregate: AggregateInfo,
        states: Sequence[CodeState],
    ) -> str:
        sign_content = self.fetch_content_for_sign(kontur_session, aggregate.document_id)
        if sign_content.participant_id:
            logger.info(
                "АК %s: participantId определён из content-for-sign: %s",
                aggregate.aggregate_code,
                sign_content.participant_id,
            )
            return sign_content.participant_id

        owner_inns = sorted({
            state.owner_inn
            for state in states
            if state.owner_inn
        })
        if len(owner_inns) == 1:
            logger.info(
                "АК %s: participantId определён по ownerInn кодов: %s",
                aggregate.aggregate_code,
                owner_inns[0],
            )
            return owner_inns[0]

        raise RuntimeError(
            f"Не удалось определить participantId для АК {aggregate.aggregate_code}"
        )

    def fetch_content_for_sign(
        self,
        kontur_session: requests.Session,
        document_id: str,
    ) -> KonturSignContent:
        response = kontur_session.get(
            f"{self.kontur_base_url}/api/v1/aggregates/{document_id}/content-for-sign",
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        base64_content = str(payload.get("base64Content") or "")
        if not base64_content:
            raise RuntimeError("Контур не вернул base64Content для подписи АК")
        decoded = base64.b64decode(base64_content).decode("utf-8")
        data = json.loads(decoded)
        participant_id = self._clean_optional_string(
            data.get("participantId") or data.get("participant_inn")
        )
        logger.info(
            "Контур: получен content-for-sign для document_id=%s, participant_id=%s, aggregation_units=%s",
            document_id,
            participant_id or "-",
            len(data.get("aggregationUnits") or []),
        )
        return KonturSignContent(
            document_id=str(payload.get("documentId") or document_id),
            base64_content=base64_content,
            participant_id=participant_id,
            payload=data,
        )

    def disaggregate_parents(
        self,
        *,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
        product_group: str,
        participant_inn: str,
        parent_codes: Sequence[str],
        log: Callable[[str], None],
    ) -> List[str]:
        disaggregated: List[str] = []
        logger.info(
            "True API: начинаем расформирование чужих АК, participant_inn=%s, parent_count=%s, parents=%s",
            participant_inn,
            len(dict.fromkeys(parent_codes)),
            _preview_items(parent_codes),
        )
        for parent_code in dict.fromkeys(parent_codes):
            document_body = {
                "participant_inn": participant_inn,
                "products_list": [
                    {"uitu": parent_code},
                ],
            }
            log(f"↪️ Расформировываем чужой АК {parent_code}")
            document_id = self.create_true_api_document(
                cert=cert,
                sign_text_func=sign_text_func,
                product_group=product_group,
                document_type="DISAGGREGATION_DOCUMENT",
                document_body=document_body,
            )
            self.wait_true_api_document(
                cert=cert,
                sign_text_func=sign_text_func,
                product_group=product_group,
                document_id=document_id,
            )
            disaggregated.append(parent_code)
            log(f"✅ Чужой АК {parent_code} успешно расформирован")
        return disaggregated

    def create_true_api_document(
        self,
        *,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
        product_group: str,
        document_type: str,
        document_body: Dict[str, Any],
    ) -> str:
        token = self.get_true_api_token(cert, sign_text_func)
        serialized = self._serialize_document(document_body)
        signature = sign_text_func(cert, serialized, True)
        logger.info(
            "True API: создаём документ, type=%s, product_group=%s, body_keys=%s",
            document_type,
            product_group,
            sorted(document_body.keys()),
        )
        response = self._true_api_post(
            f"{self.true_api_base_url}/lk/documents/create",
            params={"pg": product_group},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "document_format": "MANUAL",
                "product_document": base64.b64encode(serialized.encode("utf-8")).decode("ascii"),
                "type": document_type,
                "signature": signature,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        document_id = (
            payload.get("document_id")
            or payload.get("id")
            or payload.get("number")
        )
        if not document_id:
            raise RuntimeError(f"True API не вернул document id для {document_type}")
        logger.info(
            "True API: документ создан, type=%s, document_id=%s",
            document_type,
            document_id,
        )
        return str(document_id)

    def wait_true_api_document(
        self,
        *,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
        product_group: str,
        document_id: str,
    ) -> Dict[str, Any]:
        deadline = time.monotonic() + self.document_timeout_seconds
        last_status = ""
        last_logged_status = ""
        last_payload: Dict[str, Any] = {}
        logger.info(
            "True API: ожидаем обработки документа document_id=%s, product_group=%s, timeout=%ss",
            document_id,
            product_group,
            self.document_timeout_seconds,
        )
        while time.monotonic() <= deadline:
            token = self.get_true_api_token(cert, sign_text_func)
            response = self._true_api_get(
                f"{self.true_api_info_base_url}/doc/{document_id}/info",
                params={"pg": product_group},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=30,
            )
            if response.status_code == 404 and self.true_api_info_base_url != self.true_api_base_url:
                response = self._true_api_get(
                    f"{self.true_api_base_url}/doc/{document_id}/info",
                    params={"pg": product_group},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/json",
                    },
                    timeout=30,
                )
            response.raise_for_status()
            payload = response.json()
            last_payload = payload[0] if isinstance(payload, list) and payload else payload
            if not isinstance(last_payload, dict):
                raise RuntimeError("Некорректный ответ True API при проверке документа")

            last_status = str(
                last_payload.get("status")
                or last_payload.get("statusCode")
                or ""
            ).upper()
            if last_status != last_logged_status:
                logger.info(
                    "True API: документ %s сменил статус на %s",
                    document_id,
                    last_status or "UNKNOWN",
                )
                last_logged_status = last_status
            if last_status == "CHECKED_OK":
                return last_payload
            if (
                last_status in FAILED_DOCUMENT_STATUSES
                or last_status.endswith("_ERROR")
                or last_status.endswith("_NOT_OK")
            ):
                raise RuntimeError(
                    f"Документ {document_id} завершился со статусом {last_status}"
                )
            if self.poll_interval_seconds > 0:
                self.sleep_func(self.poll_interval_seconds)

        raise TimeoutError(
            f"Не дождались обработки документа {document_id} в True API, последний статус: {last_status or 'UNKNOWN'}"
        )

    def wait_until_parents_cleared(
        self,
        *,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
        product_group: str,
        raw_codes: Sequence[str],
        current_aggregate_code: str,
        log: Callable[[str], None],
    ) -> None:
        deadline = time.monotonic() + self.parent_clear_timeout_seconds
        last_foreign_parents: Optional[List[str]] = None
        logger.info(
            "True API: ожидаем отвязки КМ от чужих АК, current_aggregate_code=%s, product_group=%s, timeout=%ss",
            current_aggregate_code,
            product_group,
            self.parent_clear_timeout_seconds,
        )
        while time.monotonic() <= deadline:
            states = self.fetch_code_states(
                cert=cert,
                sign_text_func=sign_text_func,
                product_group=product_group,
                raw_codes=raw_codes,
            )
            foreign_parents = sorted({
                (state.parent or "").strip()
                for state in states
                if state.parent and state.parent.strip() and state.parent.strip() != current_aggregate_code
            })
            if last_foreign_parents != foreign_parents:
                logger.info(
                    "True API: статус отвязки для АК %s, remaining_foreign_parents=%s",
                    current_aggregate_code,
                    _preview_items(foreign_parents),
                )
                last_foreign_parents = list(foreign_parents)
            if not foreign_parents:
                log("✅ КМ успешно отвязаны от чужих АК")
                return
            if self.poll_interval_seconds > 0:
                self.sleep_func(self.poll_interval_seconds)

        raise TimeoutError(
            "Не дождались отвязки КМ от чужих АК в True API"
        )

    def send_aggregate_for_approve(
        self,
        *,
        kontur_session: requests.Session,
        aggregate: AggregateInfo,
        cert: Any,
        sign_base64_func: Callable[[Any, str, bool], str],
    ) -> AggregateInfo:
        detail = self.fetch_aggregate_detail(kontur_session, aggregate.document_id)
        if detail.status != "readyForSend":
            raise RuntimeError(
                f"АК {aggregate.aggregate_code} больше не readyForSend, текущий статус {detail.status}"
            )

        sign_content = self.fetch_content_for_sign(kontur_session, aggregate.document_id)
        signature = sign_base64_func(cert, sign_content.base64_content, True)
        logger.info(
            "Контур: отправляем АК %s в send, document_id=%s",
            aggregate.aggregate_code,
            aggregate.document_id,
        )
        response = kontur_session.post(
            f"{self.kontur_base_url}/api/v1/aggregates/{aggregate.document_id}/send",
            json={"signedContent": signature},
            timeout=30,
        )
        response.raise_for_status()

        deadline = time.monotonic() + self.kontur_send_timeout_seconds
        last_detail = detail
        last_status = detail.status
        while time.monotonic() <= deadline:
            last_detail = self.fetch_aggregate_detail(kontur_session, aggregate.document_id)
            if last_detail.status != last_status:
                logger.info(
                    "Контур: АК %s сменил статус %s -> %s после send",
                    aggregate.aggregate_code,
                    last_status,
                    last_detail.status,
                )
                last_status = last_detail.status
            if last_detail.status in {"approved", "sentForApprove"}:
                return last_detail
            if last_detail.status not in {"readyForSend", "returnedToTsd"}:
                raise RuntimeError(
                    f"Контур вернул неожиданный статус {last_detail.status} после отправки АК"
                )
            if self.poll_interval_seconds > 0:
                self.sleep_func(self.poll_interval_seconds)

        raise TimeoutError(
            f"Контур не перевёл АК {aggregate.aggregate_code} в sentForApprove, последний статус {last_detail.status}"
        )

    @staticmethod
    def _serialize_document(document_body: Dict[str, Any]) -> str:
        return json.dumps(
            document_body,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _true_api_get(self, url: str, **kwargs) -> requests.Response:
        try:
            return self.true_api_session.get(url, **kwargs)
        except requests.Timeout as exc:
            logger.warning(
                "True API GET timeout: host=%s, path=%s, timeout=%s",
                self._extract_host(url),
                urlparse(url).path,
                kwargs.get("timeout"),
            )
            raise TrueApiConnectivityError(
                f"True API ({self._extract_host(url)}) не ответил вовремя"
            ) from exc
        except requests.exceptions.SSLError as exc:
            logger.warning(
                "True API GET SSL error: host=%s, path=%s, details=%s",
                self._extract_host(url),
                urlparse(url).path,
                exc,
            )
            raise TrueApiConnectivityError(
                self._format_true_api_ssl_error(url)
            ) from exc
        except requests.ConnectionError as exc:
            logger.warning(
                "True API GET connection error: host=%s, path=%s, details=%s",
                self._extract_host(url),
                urlparse(url).path,
                exc,
            )
            raise TrueApiConnectivityError(
                self._format_true_api_connection_error(url, exc)
            ) from exc

    def _true_api_post(self, url: str, **kwargs) -> requests.Response:
        try:
            return self.true_api_session.post(url, **kwargs)
        except requests.Timeout as exc:
            logger.warning(
                "True API POST timeout: host=%s, path=%s, timeout=%s",
                self._extract_host(url),
                urlparse(url).path,
                kwargs.get("timeout"),
            )
            raise TrueApiConnectivityError(
                f"True API ({self._extract_host(url)}) не ответил вовремя"
            ) from exc
        except requests.exceptions.SSLError as exc:
            logger.warning(
                "True API POST SSL error: host=%s, path=%s, details=%s",
                self._extract_host(url),
                urlparse(url).path,
                exc,
            )
            raise TrueApiConnectivityError(
                self._format_true_api_ssl_error(url)
            ) from exc
        except requests.ConnectionError as exc:
            logger.warning(
                "True API POST connection error: host=%s, path=%s, details=%s",
                self._extract_host(url),
                urlparse(url).path,
                exc,
            )
            raise TrueApiConnectivityError(
                self._format_true_api_connection_error(url, exc)
            ) from exc

    @staticmethod
    def _extract_host(url: str) -> str:
        return urlparse(url).netloc or "True API"

    def _format_true_api_connection_error(
        self,
        url: str,
        exc: requests.ConnectionError,
    ) -> str:
        host = self._extract_host(url)
        details = str(exc)
        hint = self._true_api_host_hint(host)
        if (
            "NameResolutionError" in details
            or "Failed to resolve" in details
            or "getaddrinfo failed" in details
        ):
            return (
                f"Не удалось подключиться к True API ({host}): DNS-имя не разрешается. "
                f"Проверьте интернет, DNS/VPN или доступ к домену {host}.{hint}"
            )
        return (
            f"Не удалось подключиться к True API ({host}). "
            f"Проверьте интернет, VPN, прокси или доступ к домену.{hint}"
        )

    def _format_true_api_ssl_error(self, url: str) -> str:
        host = self._extract_host(url)
        return (
            f"Не удалось установить TLS-соединение с True API ({host}). "
            f"Проверьте корректность хоста, VPN/прокси и сертификаты.{self._true_api_host_hint(host)}"
        )

    @staticmethod
    def _true_api_host_hint(host: str) -> str:
        if host.endswith(".crptech.ru") and "sandbox" not in host:
            return (
                " Для production используйте "
                f"{DEFAULT_TRUE_API_PRODUCTION_BASE_URL}, а для sandbox - "
                f"{DEFAULT_TRUE_API_SANDBOX_BASE_URL}"
            )
        if host == "markirovka.sandbox.crpt.ru":
            return (
                " Для sandbox используйте "
                f"{DEFAULT_TRUE_API_SANDBOX_BASE_URL}"
            )
        return ""

    def _warn_if_true_api_base_url_looks_suspicious(self) -> None:
        host = self._extract_host(self.true_api_base_url)
        hint = self._true_api_host_hint(host)
        if hint:
            logger.warning(
                "True API base URL выглядит подозрительно: %s (source=%s).%s",
                self.true_api_base_url,
                self.true_api_base_url_source,
                hint,
            )

    @staticmethod
    def _build_info_base_url(base_url: str) -> str:
        if "/api/v3/true-api" in base_url:
            return base_url.replace("/api/v3/true-api", "/api/v4/true-api", 1)
        return base_url

    @staticmethod
    def _clean_optional_string(value: Any) -> Optional[str]:
        cleaned = str(value or "").strip()
        return cleaned or None

    @staticmethod
    def _extract_result_error(result: Dict[str, Any]) -> Optional[str]:
        if result.get("errorMessage"):
            return str(result["errorMessage"])
        if result.get("error"):
            if isinstance(result["error"], dict):
                return json.dumps(result["error"], ensure_ascii=False)
            return str(result["error"])
        errors = result.get("errors")
        if isinstance(errors, list) and errors:
            return "; ".join(str(item) for item in errors)
        if not result.get("status"):
            return "True API не вернул статус КМ"
        return None

    @staticmethod
    def _resolve_true_product_group(kontur_product_group: str) -> str:
        return KONTUR_TO_TRUE_PRODUCT_GROUP.get(
            kontur_product_group,
            DEFAULT_TRUE_API_PRODUCT_GROUP,
        )

    @staticmethod
    def _normalize_comment_filter(value: Optional[str]) -> Optional[str]:
        normalized = str(value or "").strip().lower()
        return normalized or None

    @staticmethod
    def _matches_comment_filter(comment: str, normalized_filter: str) -> bool:
        return normalized_filter in str(comment or "").strip().lower()
