"""initial management plane schema

Revision ID: 0001_initial_management_plane
Revises:
Create Date: 2026-02-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_initial_management_plane"
down_revision = None
branch_labels = None
depends_on = None


role_enum = postgresql.ENUM("INFRA_ADMIN", "PIPELINE_DEV", "AIOPS_ENGINEER", name="rolename")
access_level_enum = postgresql.ENUM("READ", "WRITE", "OWNER", name="accesslevel")
pipeline_version_status_enum = postgresql.ENUM(
    "DRAFT", "IN_REVIEW", "PUBLISHED", "REJECTED", name="pipelineversionstatus"
)
pipeline_run_status_enum = postgresql.ENUM(
    "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "STOPPED", name="pipelinerunstatus"
)
review_decision_enum = postgresql.ENUM("APPROVED", "REJECTED", name="reviewdecision")
incident_severity_enum = postgresql.ENUM("LOW", "MEDIUM", "HIGH", "CRITICAL", name="incidentseverity")
incident_status_enum = postgresql.ENUM("OPEN", "INVESTIGATING", "RESOLVED", name="incidentstatus")


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False, unique=True),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("hashed_password", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.create_table(
        "roles",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", role_enum, nullable=False, unique=True),
        sa.Column("description", sa.String(length=255), nullable=False),
    )

    op.create_table(
        "user_roles",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("user_id", sa.String(length=36), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role_id", sa.String(length=36), sa.ForeignKey("roles.id", ondelete="CASCADE"), nullable=False),
        sa.UniqueConstraint("user_id", "role_id", name="uq_user_role"),
    )
    op.create_index("ix_user_roles_user_id", "user_roles", ["user_id"])
    op.create_index("ix_user_roles_role_id", "user_roles", ["role_id"])

    op.create_table(
        "teams",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("description", sa.String(length=500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "team_members",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("team_id", sa.String(length=36), sa.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", sa.String(length=36), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.UniqueConstraint("team_id", "user_id", name="uq_team_member"),
    )
    op.create_index("ix_team_members_team_id", "team_members", ["team_id"])
    op.create_index("ix_team_members_user_id", "team_members", ["user_id"])

    op.create_table(
        "pipelines",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("external_id", sa.String(length=128), nullable=False, unique=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.Column("owner_user_id", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("owner_team_id", sa.String(length=36), sa.ForeignKey("teams.id"), nullable=True),
        sa.Column("metadata_links", sa.JSON(), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_pipelines_external_id", "pipelines", ["external_id"], unique=True)
    op.create_index("ix_pipelines_name", "pipelines", ["name"])
    op.create_index("ix_pipelines_owner_user_id", "pipelines", ["owner_user_id"])
    op.create_index("ix_pipelines_owner_team_id", "pipelines", ["owner_team_id"])

    op.create_table(
        "pipeline_shares",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_id", sa.String(length=36), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("team_id", sa.String(length=36), sa.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False),
        sa.Column("access_level", access_level_enum, nullable=False),
        sa.UniqueConstraint("pipeline_id", "team_id", name="uq_pipeline_team_share"),
    )
    op.create_index("ix_pipeline_shares_pipeline_id", "pipeline_shares", ["pipeline_id"])
    op.create_index("ix_pipeline_shares_team_id", "pipeline_shares", ["team_id"])

    op.create_table(
        "pipeline_versions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_id", sa.String(length=36), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("status", pipeline_version_status_enum, nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("spec_json", sa.JSON(), nullable=False),
        sa.Column("change_summary", sa.Text(), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("review_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("pipeline_id", "version_number", name="uq_pipeline_version_number"),
    )
    op.create_index("ix_pipeline_versions_pipeline_id", "pipeline_versions", ["pipeline_id"])
    op.create_index("ix_pipeline_versions_is_active", "pipeline_versions", ["is_active"])

    op.create_table(
        "pipeline_reviews",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_version_id",
            sa.String(length=36),
            sa.ForeignKey("pipeline_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("reviewer_id", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("decision", review_decision_enum, nullable=False),
        sa.Column("comments", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_pipeline_reviews_pipeline_version_id", "pipeline_reviews", ["pipeline_version_id"])
    op.create_index("ix_pipeline_reviews_reviewer_id", "pipeline_reviews", ["reviewer_id"])

    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_id", sa.String(length=36), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "pipeline_version_id",
            sa.String(length=36),
            sa.ForeignKey("pipeline_versions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", pipeline_run_status_enum, nullable=False),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.Column("trigger_type", sa.String(length=64), nullable=False),
        sa.Column("initiated_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("artifact_pointers", sa.JSON(), nullable=False),
        sa.Column("metrics_summary", sa.JSON(), nullable=False),
        sa.Column("stop_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_pipeline_runs_pipeline_id", "pipeline_runs", ["pipeline_id"])
    op.create_index("ix_pipeline_runs_pipeline_version_id", "pipeline_runs", ["pipeline_version_id"])
    op.create_index("ix_pipeline_runs_status", "pipeline_runs", ["status"])
    op.create_index("ix_pipeline_runs_initiated_by", "pipeline_runs", ["initiated_by"])

    op.create_table(
        "run_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "run_id", sa.String(length=36), sa.ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("stage_id", sa.String(length=128), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_run_events_run_id", "run_events", ["run_id"])
    op.create_index("ix_run_events_event_type", "run_events", ["event_type"])
    op.create_index("ix_run_events_stage_id", "run_events", ["stage_id"])
    op.create_index("ix_run_events_created_at", "run_events", ["created_at"])

    op.create_table(
        "run_logs_index",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "run_id", sa.String(length=36), sa.ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("storage_type", sa.String(length=64), nullable=False),
        sa.Column("pointer", sa.String(length=512), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_run_logs_index_run_id", "run_logs_index", ["run_id"])

    op.create_table(
        "connections",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("connection_type", sa.String(length=64), nullable=False),
        sa.Column("endpoint", sa.String(length=512), nullable=False),
        sa.Column("secret_ref", sa.String(length=512), nullable=True),
        sa.Column("encrypted_secret", sa.Text(), nullable=True),
        sa.Column("extra_json", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_connections_connection_type", "connections", ["connection_type"])

    op.create_table(
        "worker_pools",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("ray_endpoint", sa.String(length=255), nullable=False),
        sa.Column("min_workers", sa.Integer(), nullable=False),
        sa.Column("max_workers", sa.Integer(), nullable=False),
        sa.Column("concurrency_limit", sa.Integer(), nullable=False),
        sa.Column("extra_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "retention_policies",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_id", sa.String(length=36), sa.ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=True
        ),
        sa.Column("run_days", sa.Integer(), nullable=False),
        sa.Column("log_days", sa.Integer(), nullable=False),
        sa.Column("event_days", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("actor_user_id", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("resource_type", sa.String(length=128), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_audit_log_actor_user_id", "audit_log", ["actor_user_id"])
    op.create_index("ix_audit_log_action", "audit_log", ["action"])
    op.create_index("ix_audit_log_resource_type", "audit_log", ["resource_type"])
    op.create_index("ix_audit_log_resource_id", "audit_log", ["resource_id"])

    op.create_table(
        "incidents",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "pipeline_id", sa.String(length=36), sa.ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True
        ),
        sa.Column(
            "run_id", sa.String(length=36), sa.ForeignKey("pipeline_runs.id", ondelete="SET NULL"), nullable=True
        ),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("severity", incident_severity_enum, nullable=False),
        sa.Column("status", incident_status_enum, nullable=False),
        sa.Column("notes", sa.Text(), nullable=False),
        sa.Column("created_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("updated_by", sa.String(length=36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("incidents")

    op.drop_index("ix_audit_log_resource_id", table_name="audit_log")
    op.drop_index("ix_audit_log_resource_type", table_name="audit_log")
    op.drop_index("ix_audit_log_action", table_name="audit_log")
    op.drop_index("ix_audit_log_actor_user_id", table_name="audit_log")
    op.drop_table("audit_log")

    op.drop_table("retention_policies")
    op.drop_table("worker_pools")

    op.drop_index("ix_connections_connection_type", table_name="connections")
    op.drop_table("connections")

    op.drop_index("ix_run_logs_index_run_id", table_name="run_logs_index")
    op.drop_table("run_logs_index")

    op.drop_index("ix_run_events_created_at", table_name="run_events")
    op.drop_index("ix_run_events_stage_id", table_name="run_events")
    op.drop_index("ix_run_events_event_type", table_name="run_events")
    op.drop_index("ix_run_events_run_id", table_name="run_events")
    op.drop_table("run_events")

    op.drop_index("ix_pipeline_runs_initiated_by", table_name="pipeline_runs")
    op.drop_index("ix_pipeline_runs_status", table_name="pipeline_runs")
    op.drop_index("ix_pipeline_runs_pipeline_version_id", table_name="pipeline_runs")
    op.drop_index("ix_pipeline_runs_pipeline_id", table_name="pipeline_runs")
    op.drop_table("pipeline_runs")

    op.drop_index("ix_pipeline_reviews_reviewer_id", table_name="pipeline_reviews")
    op.drop_index("ix_pipeline_reviews_pipeline_version_id", table_name="pipeline_reviews")
    op.drop_table("pipeline_reviews")

    op.drop_index("ix_pipeline_versions_is_active", table_name="pipeline_versions")
    op.drop_index("ix_pipeline_versions_pipeline_id", table_name="pipeline_versions")
    op.drop_table("pipeline_versions")

    op.drop_index("ix_pipeline_shares_team_id", table_name="pipeline_shares")
    op.drop_index("ix_pipeline_shares_pipeline_id", table_name="pipeline_shares")
    op.drop_table("pipeline_shares")

    op.drop_index("ix_pipelines_owner_team_id", table_name="pipelines")
    op.drop_index("ix_pipelines_owner_user_id", table_name="pipelines")
    op.drop_index("ix_pipelines_name", table_name="pipelines")
    op.drop_index("ix_pipelines_external_id", table_name="pipelines")
    op.drop_table("pipelines")

    op.drop_index("ix_team_members_user_id", table_name="team_members")
    op.drop_index("ix_team_members_team_id", table_name="team_members")
    op.drop_table("team_members")

    op.drop_table("teams")

    op.drop_index("ix_user_roles_role_id", table_name="user_roles")
    op.drop_index("ix_user_roles_user_id", table_name="user_roles")
    op.drop_table("user_roles")

    op.drop_table("roles")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")

    bind = op.get_bind()
    incident_status_enum.drop(bind, checkfirst=True)
    incident_severity_enum.drop(bind, checkfirst=True)
    review_decision_enum.drop(bind, checkfirst=True)
    pipeline_run_status_enum.drop(bind, checkfirst=True)
    pipeline_version_status_enum.drop(bind, checkfirst=True)
    access_level_enum.drop(bind, checkfirst=True)
    role_enum.drop(bind, checkfirst=True)
