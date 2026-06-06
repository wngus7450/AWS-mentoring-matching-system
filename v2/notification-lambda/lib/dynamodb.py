"""
boto3 DynamoDB 헬퍼 (Users 테이블).

- 모듈 레벨 싱글톤 boto3 resource → cold start 최소화
- get_user_email(user_id): user_id 로 이메일/닉네임 조회
- get_user_emails([user_ids]): BatchGetItem 으로 다건 조회 (운영자 알림용 등)
"""

from __future__ import annotations

import os
from typing import Any

import boto3
from botocore.config import Config


_TABLE_NAME = os.environ.get("TABLE_NAME", "Users")
_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

_BOTO_CONFIG = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=2,
    read_timeout=5,
)

_dynamodb = boto3.resource("dynamodb", region_name=_REGION, config=_BOTO_CONFIG)
_users_table = _dynamodb.Table(_TABLE_NAME)


def get_users_table() -> Any:
    return _users_table


def get_user(user_id: str) -> dict[str, Any] | None:
    """
    user_id 로 Users 항목 조회.
    필요한 필드만 ProjectionExpression 으로 가져온다.
    """
    if not user_id:
        return None
    response = _users_table.get_item(
        Key={"user_id": user_id},
        ProjectionExpression="user_id, email, nickname, role, is_active",
    )
    return response.get("Item")


def get_user_email(user_id: str) -> str | None:
    """user_id 로 이메일만 빠르게 조회. 없으면 None."""
    user = get_user(user_id)
    if not user:
        return None
    email = user.get("email")
    return str(email) if email else None
