"""
boto3 SES 이메일 전송 래퍼.

- 모듈 레벨에서 SES client 1회 생성 (cold start 최소화)
- send_email() 은 일시 오류 시 ClientError 를 그대로 raise →
  호출 측(handlers/event_router) 에서 batchItemFailures 로 잡아 재시도하게 한다.
- SES 샌드박스 환경에서는 verify 안 된 수신자로 보내면 실패하므로
  실패 사유를 ERROR 로그로 상세히 남긴다.
"""

from __future__ import annotations

import os
from typing import Any, Iterable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from lib.logger import get_logger


logger = get_logger(__name__)

_SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "")
_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

_BOTO_CONFIG = Config(
    retries={"max_attempts": 2, "mode": "standard"},
    connect_timeout=2,
    read_timeout=5,
)

_ses_client = boto3.client("ses", region_name=_REGION, config=_BOTO_CONFIG)


class EmailSendError(Exception):
    """SES 전송 실패 (호출 측에서 batchItemFailures 로 처리할 수 있도록 raise)."""


def get_sender() -> str:
    return _SENDER_EMAIL


def send_email(
    *,
    to_addresses: Iterable[str],
    subject: str,
    body_text: str,
    body_html: str | None = None,
    message_id: str | None = None,
) -> str:
    """
    SES 로 이메일 전송.

    Args:
        to_addresses: 수신자 이메일 목록
        subject:      메일 제목
        body_text:    텍스트 본문 (필수)
        body_html:    HTML 본문 (선택)
        message_id:   로깅용 SQS messageId

    Returns:
        SES MessageId

    Raises:
        EmailSendError: 발신자 미설정 / 수신자 비어있음 / SES API 실패
    """
    if not _SENDER_EMAIL:
        raise EmailSendError("SENDER_EMAIL is not configured")

    recipients = [a for a in to_addresses if a]
    if not recipients:
        raise EmailSendError("No valid recipients to send")

    body: dict[str, Any] = {"Text": {"Data": body_text, "Charset": "UTF-8"}}
    if body_html:
        body["Html"] = {"Data": body_html, "Charset": "UTF-8"}

    try:
        response = _ses_client.send_email(
            Source=_SENDER_EMAIL,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": body,
            },
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        logger.error(
            "ses_send_failed",
            extra={
                "message_id": message_id,
                "error_code": code,
                "recipients_count": len(recipients),
                "subject": subject,
            },
            exc_info=True,
        )
        # 호출 측에서 batchItemFailures 로 잡을 수 있도록 re-raise
        raise EmailSendError(f"SES send_email failed: {code}") from exc

    ses_message_id = response.get("MessageId", "")
    logger.info(
        "ses_send_success",
        extra={
            "message_id": message_id,
            "ses_message_id": ses_message_id,
            "recipients_count": len(recipients),
            "subject": subject,
        },
    )
    return ses_message_id
