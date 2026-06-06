"""
이벤트 라우터.

1) SQS Record 의 이중 파싱
   - record["body"] 자체는 SNS 객체 (json string)
   - SNS 객체의 "Message" 필드를 다시 json.loads 해야 도메인 페이로드가 나온다.
2) event_type 별 분기 (match-case)
3) 각 이벤트의 수신 대상 결정 후 SES 발송

수신 대상 테이블:
    MentoringRequestSubmitted    -> 운영자
    MentorAssigned               -> 멘토
    AssignmentApproved           -> 멘티 + 운영자
    AssignmentRejected           -> 운영자
    RequestFinallyRejected       -> 멘티
    ConsultationCompleted        -> 멘토 + 운영자
    ConsultationRecordSubmitted  -> 멘티

멱등성:
    SQS at-least-once 특성상 동일 messageId 가 재전달될 수 있다.
    본 람다는 in-process 단위로 처리한 messageId set 을 캐싱해
    같은 컨테이너 안에서의 즉시 재전달은 한 번 더 발송하지 않는다.
    완전한 멱등성은 별도의 처리 이력 테이블 (예: DynamoDB) 로 보강 가능.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from lib.dynamodb import get_user_email
from lib.logger import get_logger
from lib.ses import EmailSendError, send_email


logger = get_logger(__name__)


_ADMIN_EMAILS = [
    e.strip()
    for e in (os.environ.get("ADMIN_EMAILS") or "").split(",")
    if e.strip()
]


class ParseError(Exception):
    """SQS Record 또는 SNS 페이로드 파싱 실패."""


@dataclass(frozen=True)
class EventEnvelope:
    """파싱이 끝난 도메인 이벤트."""

    event_type: str
    data: dict[str, Any]
    sns_message_id: str | None
    raw_message: dict[str, Any]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


class EventRouter:
    """
    SQS Record 를 받아 도메인 이벤트로 변환하고 알림을 발송한다.

    in-process 멱등성 캐시 (lambda warm 컨테이너 동안 유효).
    """

    # 처리 완료한 messageId 들. 컨테이너 재사용 시 누적되지만 큰 문제 없음.
    # 메모리 폭증 방지를 위해 LRU 형태로 단순 제한.
    _MAX_CACHE_SIZE = 1000

    def __init__(self) -> None:
        self._processed_ids: list[str] = []
        self._processed_set: set[str] = set()

    # ------------------------------------------------------------------
    # 파싱
    # ------------------------------------------------------------------

    def parse_record(self, record: dict[str, Any]) -> EventEnvelope:
        """
        SQS Record → EventEnvelope.

        ★ 이중 파싱:
            sns_message = json.loads(record["body"])
            payload     = json.loads(sns_message["Message"])
        """
        body_str = record.get("body")
        if not isinstance(body_str, str) or not body_str:
            raise ParseError("SQS record 'body' is empty or not a string")

        # 1차: SQS body → SNS envelope
        try:
            sns_message = json.loads(body_str)
        except json.JSONDecodeError as exc:
            raise ParseError(f"Failed to parse SQS body as JSON: {exc}") from exc

        if not isinstance(sns_message, dict):
            raise ParseError("Parsed SQS body is not an object")

        sns_message_id = sns_message.get("MessageId")
        message_str = sns_message.get("Message")
        if not isinstance(message_str, str):
            raise ParseError("SNS envelope is missing 'Message' string")

        # 2차: SNS Message → 도메인 페이로드
        try:
            payload = json.loads(message_str)
        except json.JSONDecodeError as exc:
            raise ParseError(f"Failed to parse SNS Message as JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise ParseError("Domain payload is not a JSON object")

        # 우리 시스템의 publish 규약: {"event_type": "...", "data": {...}}
        event_type = payload.get("event_type")
        data = payload.get("data") or {}

        # SNS MessageAttributes 의 event_type 으로도 보강
        if not event_type:
            attrs = sns_message.get("MessageAttributes") or {}
            attr_event = (attrs.get("event_type") or {}).get("Value")
            if isinstance(attr_event, str):
                event_type = attr_event

        if not event_type or not isinstance(event_type, str):
            raise ParseError("event_type is missing in payload")

        if not isinstance(data, dict):
            raise ParseError("'data' must be an object")

        return EventEnvelope(
            event_type=event_type,
            data=data,
            sns_message_id=sns_message_id,
            raw_message=payload,
        )

    # ------------------------------------------------------------------
    # 디스패치
    # ------------------------------------------------------------------

    def dispatch(self, envelope: EventEnvelope, *, message_id: str) -> None:
        """이벤트 타입별 핸들러로 분기."""
        # in-process 멱등성: 같은 messageId 가 곧장 재전달되었을 때 차단
        if message_id and message_id in self._processed_set:
            logger.warning(
                "duplicate_message_skipped",
                extra={
                    "message_id": message_id,
                    "event_type": envelope.event_type,
                },
            )
            return

        match envelope.event_type:
            case "MentoringRequestSubmitted":
                _notify_request_submitted(envelope, message_id=message_id)
            case "MentorAssigned":
                _notify_mentor_assigned(envelope, message_id=message_id)
            case "AssignmentApproved":
                _notify_assignment_approved(envelope, message_id=message_id)
            case "AssignmentRejected":
                _notify_assignment_rejected(envelope, message_id=message_id)
            case "RequestFinallyRejected":
                _notify_request_finally_rejected(envelope, message_id=message_id)
            case "ConsultationCompleted":
                _notify_consultation_completed(envelope, message_id=message_id)
            case "ConsultationRecordSubmitted":
                _notify_record_submitted(envelope, message_id=message_id)
            case _:
                # 미지원 이벤트는 ack 로 처리하되 경고 로그.
                # 재전송해도 결과가 같으므로 batchItemFailures 에 넣지 않는다.
                logger.warning(
                    "unsupported_event_type",
                    extra={
                        "message_id": message_id,
                        "event_type": envelope.event_type,
                    },
                )
                return

        self._mark_processed(message_id)

    def _mark_processed(self, message_id: str) -> None:
        if not message_id:
            return
        if message_id in self._processed_set:
            return
        self._processed_ids.append(message_id)
        self._processed_set.add(message_id)
        if len(self._processed_ids) > self._MAX_CACHE_SIZE:
            evicted = self._processed_ids.pop(0)
            self._processed_set.discard(evicted)


# ---------------------------------------------------------------------------
# 수신자 해석 헬퍼
# ---------------------------------------------------------------------------


def _resolve_email(user_id: str | None, *, role_for_log: str, message_id: str) -> str | None:
    """user_id → email. 없으면 ERROR 로그 후 None."""
    if not user_id:
        logger.error(
            "recipient_user_id_missing",
            extra={"message_id": message_id, "role": role_for_log},
        )
        return None
    email = get_user_email(user_id)
    if not email:
        logger.error(
            "recipient_email_not_found",
            extra={
                "message_id": message_id,
                "role": role_for_log,
                "user_id": user_id,
            },
        )
    return email


def _admin_recipients(message_id: str) -> list[str]:
    if not _ADMIN_EMAILS:
        logger.warning(
            "admin_emails_not_configured",
            extra={"message_id": message_id},
        )
    return list(_ADMIN_EMAILS)


def _send_with_logging(
    *,
    to_addresses: list[str],
    subject: str,
    body_text: str,
    event_type: str,
    message_id: str,
    role: str,
) -> None:
    """SES 발송 + 도메인 로그. 발송할 수신자 없으면 스킵."""
    if not to_addresses:
        logger.warning(
            "notification_skipped_no_recipient",
            extra={
                "message_id": message_id,
                "event_type": event_type,
                "role": role,
            },
        )
        return

    try:
        send_email(
            to_addresses=to_addresses,
            subject=subject,
            body_text=body_text,
            message_id=message_id,
        )
    except EmailSendError:
        # SES 실패는 lambda_handler 의 except 블록에서 batchItemFailures 로 처리
        # → 동일 메시지가 다시 들어오면 재시도, 결국 DLQ 이동
        raise

    logger.info(
        "notification_sent",
        extra={
            "message_id": message_id,
            "event_type": event_type,
            "role": role,
            "recipients_count": len(to_addresses),
        },
    )


# ---------------------------------------------------------------------------
# 이벤트별 핸들러
# ---------------------------------------------------------------------------


def _notify_request_submitted(envelope: EventEnvelope, *, message_id: str) -> None:
    """MentoringRequestSubmitted → 운영자 알림 (신규 신청 접수)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"
    topic = data.get("topic") or "(주제 미입력)"
    interest_field = data.get("interest_field") or "?"

    subject = f"[멘토링] 신규 신청 접수 ({request_id})"
    body = (
        f"새로운 멘토링 신청이 접수되었습니다.\n\n"
        f"- request_id   : {request_id}\n"
        f"- 관심분야     : {interest_field}\n"
        f"- 주제         : {topic}\n"
        f"- 멘티 user_id : {data.get('mentee_user_id') or '?'}\n"
        f"- 접수 시각    : {data.get('created_at') or '?'}\n\n"
        f"운영자 콘솔에서 멘토 배정을 진행해 주세요."
    )

    _send_with_logging(
        to_addresses=_admin_recipients(message_id),
        subject=subject,
        body_text=body,
        event_type=envelope.event_type,
        message_id=message_id,
        role="ADMIN",
    )


def _notify_mentor_assigned(envelope: EventEnvelope, *, message_id: str) -> None:
    """MentorAssigned → 멘토 알림 (배정 요청)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"
    mentor_user_id = data.get("mentor_user_id")

    mentor_email = _resolve_email(
        mentor_user_id, role_for_log="MENTOR", message_id=message_id
    )

    subject = f"[멘토링] 새로운 멘토 배정 요청 ({request_id})"
    body = (
        f"새로운 멘토 배정이 요청되었습니다.\n\n"
        f"- request_id : {request_id}\n"
        f"- 배정 시각  : {data.get('assigned_at') or '?'}\n\n"
        f"멘토 페이지에서 승인 또는 반려를 처리해 주세요."
    )

    _send_with_logging(
        to_addresses=[mentor_email] if mentor_email else [],
        subject=subject,
        body_text=body,
        event_type=envelope.event_type,
        message_id=message_id,
        role="MENTOR",
    )


def _notify_assignment_approved(envelope: EventEnvelope, *, message_id: str) -> None:
    """AssignmentApproved → 멘티 + 운영자 알림 (일정 확정)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"
    schedule = data.get("schedule") or {}
    schedule_text = (
        f"{schedule.get('day_of_week', '?')} "
        f"{schedule.get('start_time', '?')}~{schedule.get('end_time', '?')} "
        f"({schedule.get('meeting_type', '?')})"
    )

    mentee_email = _resolve_email(
        data.get("mentee_user_id"), role_for_log="MENTEE", message_id=message_id
    )

    # 멘티에게 발송
    if mentee_email:
        _send_with_logging(
            to_addresses=[mentee_email],
            subject=f"[멘토링] 상담 일정이 확정되었습니다 ({request_id})",
            body_text=(
                f"신청하신 멘토링의 일정이 확정되었습니다.\n\n"
                f"- request_id : {request_id}\n"
                f"- 일정       : {schedule_text}\n"
                f"- 확정 시각  : {data.get('approved_at') or '?'}\n"
            ),
            event_type=envelope.event_type,
            message_id=message_id,
            role="MENTEE",
        )

    # 운영자에게도 발송
    _send_with_logging(
        to_addresses=_admin_recipients(message_id),
        subject=f"[멘토링] 배정 승인 / 일정 확정 ({request_id})",
        body_text=(
            f"멘토가 배정 요청을 승인했습니다.\n\n"
            f"- request_id     : {request_id}\n"
            f"- 멘토 user_id   : {data.get('mentor_user_id') or '?'}\n"
            f"- 멘티 user_id   : {data.get('mentee_user_id') or '?'}\n"
            f"- 확정 일정      : {schedule_text}\n"
        ),
        event_type=envelope.event_type,
        message_id=message_id,
        role="ADMIN",
    )


def _notify_assignment_rejected(envelope: EventEnvelope, *, message_id: str) -> None:
    """AssignmentRejected → 운영자 알림 (반려 / 재배정 필요)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"

    subject = f"[멘토링] 멘토 배정 반려 ({request_id})"
    body = (
        f"배정한 멘토가 요청을 반려했습니다. 재배정 또는 최종 반려가 필요합니다.\n\n"
        f"- request_id     : {request_id}\n"
        f"- 멘토 user_id   : {data.get('mentor_user_id') or '?'}\n"
        f"- 반려 사유      : {data.get('reason') or '(없음)'}\n"
        f"- 반려 시각      : {data.get('rejected_at') or '?'}\n"
    )

    _send_with_logging(
        to_addresses=_admin_recipients(message_id),
        subject=subject,
        body_text=body,
        event_type=envelope.event_type,
        message_id=message_id,
        role="ADMIN",
    )


def _notify_request_finally_rejected(envelope: EventEnvelope, *, message_id: str) -> None:
    """RequestFinallyRejected → 멘티 알림 (최종 반려)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"

    mentee_email = _resolve_email(
        data.get("mentee_user_id"), role_for_log="MENTEE", message_id=message_id
    )

    subject = f"[멘토링] 신청이 최종 반려되었습니다 ({request_id})"
    body = (
        f"신청하신 멘토링이 최종 반려되었습니다.\n\n"
        f"- request_id : {request_id}\n"
        f"- 사유       : {data.get('reason') or '(미기재)'}\n"
        f"- 처리 시각  : {data.get('reviewed_at') or '?'}\n\n"
        f"다른 시간대 / 멘토를 선택해 다시 신청해 주세요."
    )

    _send_with_logging(
        to_addresses=[mentee_email] if mentee_email else [],
        subject=subject,
        body_text=body,
        event_type=envelope.event_type,
        message_id=message_id,
        role="MENTEE",
    )


def _notify_consultation_completed(envelope: EventEnvelope, *, message_id: str) -> None:
    """ConsultationCompleted → 멘토 + 운영자 알림 (기록 작성 유도/완료 통지)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"
    mentor_user_id = data.get("mentor_user_id")

    mentor_email = _resolve_email(
        mentor_user_id, role_for_log="MENTOR", message_id=message_id
    )

    # 멘토에게 발송 (기록 작성 유도)
    if mentor_email:
        _send_with_logging(
            to_addresses=[mentor_email],
            subject=f"[멘토링] 상담이 완료되었습니다. 기록을 작성해 주세요 ({request_id})",
            body_text=(
                f"상담이 완료 처리되었습니다. 멘토 페이지에서 상담 기록을 작성해 주세요.\n\n"
                f"- request_id  : {request_id}\n"
                f"- 완료 시각   : {data.get('completed_at') or '?'}\n"
            ),
            event_type=envelope.event_type,
            message_id=message_id,
            role="MENTOR",
        )

    # 운영자에게도 발송 (현황 통지)
    _send_with_logging(
        to_addresses=_admin_recipients(message_id),
        subject=f"[멘토링] 상담 완료 통지 ({request_id})",
        body_text=(
            f"상담이 완료 처리되었습니다.\n\n"
            f"- request_id     : {request_id}\n"
            f"- 멘토 user_id   : {data.get('mentor_user_id') or '?'}\n"
            f"- 멘티 user_id   : {data.get('mentee_user_id') or '?'}\n"
            f"- 완료 시각      : {data.get('completed_at') or '?'}\n"
        ),
        event_type=envelope.event_type,
        message_id=message_id,
        role="ADMIN",
    )


def _notify_record_submitted(envelope: EventEnvelope, *, message_id: str) -> None:
    """ConsultationRecordSubmitted → 멘티 알림 (기록 공개)."""
    data = envelope.data
    request_id = data.get("request_id") or "?"

    mentee_email = _resolve_email(
        data.get("mentee_user_id"), role_for_log="MENTEE", message_id=message_id
    )

    subject = f"[멘토링] 상담 기록이 공개되었습니다 ({request_id})"
    body = (
        f"멘토가 상담 기록을 최종 제출하여 확인하실 수 있습니다.\n\n"
        f"- request_id : {request_id}\n"
        f"- 제출 시각  : {data.get('submitted_at') or '?'}\n\n"
        f"멘티 페이지에서 상세 내용을 확인해 주세요."
    )

    _send_with_logging(
        to_addresses=[mentee_email] if mentee_email else [],
        subject=subject,
        body_text=body,
        event_type=envelope.event_type,
        message_id=message_id,
        role="MENTEE",
    )
