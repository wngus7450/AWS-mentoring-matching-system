"""
Notification Lambda - Entry Point

역할:
    SQS(NotificationQueue) 메시지를 소비해 실제 알림을 발송하는 비동기 워커.

트리거:
    Amazon SQS (NotificationQueue)
    ※ 본 람다는 API Gateway 를 거치지 않는다.

런타임:
    Python 3.12

[중요] SQS + SNS 결합 시 함정과 해결책
---------------------------------------
1) 이중 파싱:
   SNS → SQS 로 넘어올 때 SNS 가 자신의 메타데이터로 원본 메시지를 감싼다.
   따라서 record.body 자체는 SNS 객체이며, 실제 도메인 페이로드는
   sns_message["Message"] 를 한 번 더 json.loads 해야 얻을 수 있다.
   → handlers/event_router.py 의 parse_record() 에서 처리.

2) 배치 부분 실패 (ReportBatchItemFailures):
   배치 안의 한 메시지가 실패했다고 raise 하면 SQS 는 배치 전체를 재시도하므로
   성공한 메시지도 중복 발송된다.
   → 본 핸들러는 실패 메시지의 messageId 만 batchItemFailures 에 담아 반환한다.
   ※ Lambda 이벤트 소스 매핑에서 "Report batch item failures" 옵션을
     반드시 활성화해야 한다 (FunctionResponseTypes=["ReportBatchItemFailures"]).

DLQ:
    batchItemFailures 로 반환된 메시지는 SQS 큐의 redrive policy 에 따라
    maxReceiveCount 초과 시 NotificationDeadLetterQueue 로 이동한다.
"""

from __future__ import annotations

from typing import Any

from handlers.event_router import EventRouter, ParseError
from lib.logger import get_logger


logger = get_logger(__name__)
_router = EventRouter()


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    SQS 배치 이벤트 진입점.

    Args:
        event: {"Records": [ <SQS message>, ... ]}
        context: Lambda context

    Returns:
        {"batchItemFailures": [{"itemIdentifier": "<messageId>"}, ...]}
    """
    records: list[dict[str, Any]] = event.get("Records") or []
    aws_request_id = getattr(context, "aws_request_id", "unknown") if context else "unknown"

    logger.info(
        "sqs_batch_received",
        extra={
            "aws_request_id": aws_request_id,
            "batch_size": len(records),
        },
    )

    failed_messages: list[dict[str, str]] = []

    # ★ 반드시 record 단위로 try/except 처리.
    # 한 건이 raise 하면 전체 배치가 재시도되어 중복 알림이 발생한다.
    for record in records:
        message_id = record.get("messageId") or "unknown"

        try:
            envelope = _router.parse_record(record)
            _router.dispatch(envelope, message_id=message_id)

            logger.info(
                "sqs_message_processed",
                extra={
                    "message_id": message_id,
                    "event_type": envelope.event_type,
                },
            )
        except ParseError as exc:
            # 페이로드가 깨진 메시지는 재시도해도 동일 실패 → 그대로 실패 처리.
            # SQS redrive policy 에 따라 maxReceiveCount 초과 시 DLQ 로 이동.
            logger.error(
                "sqs_message_parse_failed",
                extra={
                    "message_id": message_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            failed_messages.append({"itemIdentifier": message_id})
        except Exception as exc:  # noqa: BLE001
            # 알 수 없는 일시 오류 → 해당 메시지만 실패 처리, 나머지는 성공 ack.
            logger.error(
                "sqs_message_handling_failed",
                extra={
                    "message_id": message_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            failed_messages.append({"itemIdentifier": message_id})

    logger.info(
        "sqs_batch_completed",
        extra={
            "aws_request_id": aws_request_id,
            "batch_size": len(records),
            "failed_count": len(failed_messages),
        },
    )

    return {"batchItemFailures": failed_messages}
