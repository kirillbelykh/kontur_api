import base64
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests

from logger import logger


DEFAULT_KONTUR_BASE_URL = "https://mk.kontur.ru"
DEFAULT_WAREHOUSE_ID = "59739360-7d62-434b-ad13-4617c87a6d13"
DEFAULT_TRUE_API_BASE_URL = "https://markirovka.crptech.ru/api/v3/true-api"
DEFAULT_TRUE_API_PRODUCT_GROUP = "wheelchairs"
DEFAULT_KONTUR_PRODUCT_GROUP = "wheelChairs"

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
        sleep_func: Callable[[float], None] = time.sleep,
        true_api_session: Optional[requests.Session] = None,
    ):
        self.kontur_base_url = (
            kontur_base_url or os.getenv("BASE_URL", DEFAULT_KONTUR_BASE_URL)
        ).rstrip("/")
        self.warehouse_id = warehouse_id or os.getenv("WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)
        self.true_api_base_url = (
            true_api_base_url or os.getenv("TRUE_API_BASE_URL", DEFAULT_TRUE_API_BASE_URL)
        ).rstrip("/")
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
        self.sleep_func = sleep_func
        self.true_api_session = true_api_session or requests.Session()
        self._true_api_token: Optional[str] = None
        self._true_api_token_expires_at = 0.0

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
    ) -> BulkAggregationSummary:
        cert = cert_provider()
        if not cert:
            raise RuntimeError("Сертификат для подписи не найден")

        log = log_callback or (lambda message: None)
        progress = progress_callback or (lambda processed, total: None)
        confirm = confirm_callback or (lambda title, message: False)
        summary = BulkAggregationSummary()

        offset = 0
        while True:
            aggregates, total = self.fetch_ready_aggregates_page(kontur_session, offset)
            if total is not None:
                summary.ready_found = total

            if not aggregates:
                break

            for aggregate in aggregates:
                summary.processed += 1
                progress(summary.processed, summary.ready_found or summary.processed)
                try:
                    self._process_aggregate(
                        kontur_session=kontur_session,
                        aggregate=aggregate,
                        cert=cert,
                        sign_base64_func=sign_base64_func,
                        sign_text_func=sign_text_func,
                        log=log,
                        confirm=confirm,
                        summary=summary,
                    )
                except Exception as exc:
                    summary.errors += 1
                    logger.exception("Ошибка массового проведения АК %s", aggregate.aggregate_code)
                    log(f"❌ {aggregate.aggregate_code}: {exc}")

            if len(aggregates) < self.page_size:
                break
            offset += self.page_size

        progress(summary.ready_found or summary.processed, summary.ready_found or summary.processed or 1)
        return summary

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

        detail = self.fetch_aggregate_detail(kontur_session, aggregate.document_id)
        if detail.status != "readyForSend":
            summary.skipped_not_ready += 1
            log(f"⏭️ {aggregate.aggregate_code}: статус уже изменился на {detail.status}")
            return

        raw_codes, reaggregation_codes = self.fetch_aggregate_codes(kontur_session, aggregate.document_id)
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
        states = self.fetch_code_states(
            cert=cert,
            sign_text_func=sign_text_func,
            product_group=self._resolve_true_product_group(aggregate.product_group),
            raw_codes=raw_codes,
        )

        errored = [state for state in states if state.api_error]
        if errored:
            raise RuntimeError(
                "True API вернул ошибки по кодам: "
                + "; ".join(
                    f"{state.sntin}: {state.api_error}"
                    for state in errored[:5]
                )
            )

        foreign_parents = sorted({
            (state.parent or "").strip()
            for state in states
            if state.parent and state.parent.strip() and state.parent.strip() != aggregate.aggregate_code
        })
        not_introduced = [state for state in states if state.status != "INTRODUCED"]

        if not_introduced:
            summary.skipped_due_to_status += 1
            log(
                f"⏭️ {aggregate.aggregate_code}: КМ не готовы к проведению "
                f"({_status_counts(not_introduced)})"
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
                    log(f"ℹ️ {aggregate.aggregate_code}: расформирование отменено пользователем")
            return

        if foreign_parents:
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

        final_detail = self.send_aggregate_for_approve(
            kontur_session=kontur_session,
            aggregate=aggregate,
            cert=cert,
            sign_base64_func=sign_base64_func,
        )
        summary.sent_for_approve += 1
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

        for chunk in _chunks(unique_codes, self.batch_size):
            token = self.get_true_api_token(cert, sign_text_func)
            response = self.true_api_session.post(
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
        return states

    def get_true_api_token(
        self,
        cert: Any,
        sign_text_func: Callable[[Any, str, bool], str],
    ) -> str:
        now = time.monotonic()
        if self._true_api_token and now < self._true_api_token_expires_at:
            return self._true_api_token

        auth_key_response = self.true_api_session.get(
            f"{self.true_api_base_url}/auth/key",
            timeout=15,
        )
        auth_key_response.raise_for_status()
        auth_key = auth_key_response.json()
        signature = sign_text_func(cert, str(auth_key["data"]), False)
        token_response = self.true_api_session.post(
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
            return sign_content.participant_id

        owner_inns = sorted({
            state.owner_inn
            for state in states
            if state.owner_inn
        })
        if len(owner_inns) == 1:
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
        response = self.true_api_session.post(
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
        last_payload: Dict[str, Any] = {}
        while time.monotonic() <= deadline:
            token = self.get_true_api_token(cert, sign_text_func)
            response = self.true_api_session.get(
                f"{self.true_api_info_base_url}/doc/{document_id}/info",
                params={"pg": product_group},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=30,
            )
            if response.status_code == 404 and self.true_api_info_base_url != self.true_api_base_url:
                response = self.true_api_session.get(
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
        response = kontur_session.post(
            f"{self.kontur_base_url}/api/v1/aggregates/{aggregate.document_id}/send",
            json={"signedContent": signature},
            timeout=30,
        )
        response.raise_for_status()

        deadline = time.monotonic() + self.kontur_send_timeout_seconds
        last_detail = detail
        while time.monotonic() <= deadline:
            last_detail = self.fetch_aggregate_detail(kontur_session, aggregate.document_id)
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
