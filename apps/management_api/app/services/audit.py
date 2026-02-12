from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models import AuditLog


def add_audit_entry(
    db: Session,
    actor_user_id: str,
    action: str,
    resource_type: str,
    resource_id: str,
    details: dict[str, Any] | None = None,
) -> AuditLog:
    audit = AuditLog(
        actor_user_id=actor_user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details or {},
    )
    db.add(audit)
    return audit
