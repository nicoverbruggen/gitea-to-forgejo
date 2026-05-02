#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SUPPORTED_ACTIVITY_OP_TYPES = {1, 2, 5, 6, 8, 9, 10, 12, 16, 17, 18, 19, 20, 24}


@dataclass
class ValidationFailure:
    check: str
    detail: str


def normalize_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_int(value: Any) -> int:
    return int(value or 0)


def sample_values(values: Iterable[Any], limit: int = 10) -> str:
    items = [str(value) for value in values]
    if not items:
        return "(none)"
    if len(items) <= limit:
        return ", ".join(items)
    head = ", ".join(items[:limit])
    return f"{head}, ... (+{len(items) - limit} more)"


class Validator:
    def __init__(
        self,
        source_db: Path,
        forgejo_db: Path,
        backup_root: Path,
        forgejo_root: Path,
        state_path: Path,
        report_path: Path,
    ) -> None:
        self.source_db = source_db
        self.forgejo_db = forgejo_db
        self.backup_root = backup_root
        self.forgejo_root = forgejo_root
        self.state_path = state_path
        self.report_path = report_path

        self.failures: list[ValidationFailure] = []
        self.notes: list[str] = []

        self.source = sqlite3.connect(source_db)
        self.source.row_factory = sqlite3.Row

        self.target = sqlite3.connect(f"file:{forgejo_db}?mode=ro", uri=True)
        self.target.row_factory = sqlite3.Row

        self.warning_repo_keys = self.load_warning_repo_keys()
        self.pruned_source_package_version_count = 0
        self.pruned_source_package_file_count = 0
        self.pruned_source_package_blob_count = 0
        (
            self.retained_source_package_versions,
            self.retained_source_package_files,
            self.retained_source_package_blobs,
            self.retained_source_package_properties,
        ) = self.compute_retained_source_package_rows()

    def load_warning_repo_keys(self) -> set[tuple[str, str]]:
        if not self.state_path.exists():
            return set()

        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        repo_keys: set[tuple[str, str]] = set()
        for warning in payload.get("warnings", []):
            owner = normalize_text(warning.get("owner")).lower()
            name = normalize_text(warning.get("name")).lower()
            if owner and name:
                repo_keys.add((owner, name))
        return repo_keys

    def compute_retained_source_package_rows(
        self,
    ) -> tuple[list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row]]:
        package_rows = self.fetch_all(self.source, "select * from package order by id")
        version_rows = self.fetch_all(self.source, "select * from package_version order by id")
        file_rows = self.fetch_all(self.source, "select * from package_file order by id")
        blob_rows = self.fetch_all(self.source, "select * from package_blob order by id")
        property_rows = self.fetch_all(self.source, "select * from package_property order by id")

        package_ids = {row["id"] for row in package_rows}
        referenced_digests = {
            normalize_text(row["value"])
            for row in property_rows
            if normalize_int(row["ref_type"]) == 0 and normalize_text(row["name"]) == "container.manifest.reference"
        }
        kept_versions = [
            row
            for row in version_rows
            if not normalize_text(row["version"]).startswith("sha256:")
            or normalize_text(row["version"]) in referenced_digests
        ]
        kept_version_ids = {row["id"] for row in kept_versions}
        kept_files = [row for row in file_rows if row["version_id"] in kept_version_ids]
        kept_file_ids = {row["id"] for row in kept_files}
        kept_blob_ids = {row["blob_id"] for row in kept_files}
        kept_blobs = [row for row in blob_rows if row["id"] in kept_blob_ids]
        kept_properties = [
            row
            for row in property_rows
            if (
                normalize_int(row["ref_type"]) == 0
                and normalize_int(row["ref_id"]) in kept_version_ids
            )
            or (
                normalize_int(row["ref_type"]) == 1
                and normalize_int(row["ref_id"]) in kept_file_ids
            )
            or (
                normalize_int(row["ref_type"]) == 2
                and normalize_int(row["ref_id"]) in package_ids
            )
        ]

        self.pruned_source_package_version_count = len(version_rows) - len(kept_versions)
        self.pruned_source_package_file_count = len(file_rows) - len(kept_files)
        self.pruned_source_package_blob_count = len(blob_rows) - len(kept_blobs)
        return kept_versions, kept_files, kept_blobs, kept_properties

    def fetch_all(self, connection: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return list(connection.execute(query, params).fetchall())

    def add_failure(self, check: str, detail: str) -> None:
        self.failures.append(ValidationFailure(check=check, detail=detail))

    def add_note(self, message: str) -> None:
        self.notes.append(message)

    def compare_values(
        self,
        check: str,
        label: str,
        source_value: Any,
        target_value: Any,
    ) -> None:
        if source_value != target_value:
            self.add_failure(
                check,
                f"{label}: expected {source_value!r}, found {target_value!r}",
            )

    def compare_key_sets(
        self,
        check: str,
        source_keys: set[Any],
        target_keys: set[Any],
    ) -> None:
        missing = sorted(source_keys - target_keys)
        extra = sorted(target_keys - source_keys)
        if missing:
            self.add_failure(check, f"Missing entries: {sample_values(missing)}")
        if extra:
            self.add_failure(check, f"Unexpected extra entries: {sample_values(extra)}")

    def sha256_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def git_head_text(self, repo_path: Path) -> str:
        head_path = repo_path / "HEAD"
        return head_path.read_text(encoding="utf-8").strip() if head_path.exists() else ""

    def git_ref_map(self, repo_path: Path) -> dict[str, str]:
        result = subprocess.run(
            [
                "git",
                "--git-dir",
                str(repo_path),
                "for-each-ref",
                "--format=%(refname)%09%(objectname)",
                "refs",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        refs: dict[str, str] = {}
        for line in result.stdout.splitlines():
            if not line:
                continue
            ref_name, object_name = line.split("\t", 1)
            refs[ref_name] = object_name
        return refs

    def git_fsck(self, repo_path: Path) -> None:
        result = subprocess.run(
            ["git", "--git-dir", str(repo_path), "fsck", "--strict", "--no-progress"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = (result.stdout + result.stderr).strip() or "git fsck failed without output"
            self.add_failure("git-fsck", f"{repo_path}: {detail}")

    def run(self) -> int:
        self.validate_database()
        self.validate_users()
        self.validate_organizations()
        self.validate_org_memberships()
        self.validate_teams()
        self.validate_repositories()
        self.validate_project_and_social_data()
        self.validate_git_repositories()
        self.validate_ssh_keys()
        self.validate_avatars()
        self.validate_pull_mirrors()
        self.validate_push_mirrors()
        self.validate_packages()
        self.validate_activity_feed()
        self.write_report()
        return 0 if not self.failures else 1

    def validate_database(self) -> None:
        integrity = self.target.execute("pragma integrity_check").fetchone()[0]
        journal_mode = normalize_text(self.target.execute("pragma journal_mode").fetchone()[0]).lower()

        if integrity != "ok":
            self.add_failure("database", f"SQLite integrity_check returned {integrity!r}")
        if journal_mode != "delete":
            self.add_failure("database", f"Expected journal_mode 'delete', found {journal_mode!r}")

        wal_path = self.forgejo_db.with_name(f"{self.forgejo_db.name}-wal")
        shm_path = self.forgejo_db.with_name(f"{self.forgejo_db.name}-shm")
        self.add_note(f"SQLite journal mode: `{journal_mode}`")
        self.add_note(f"SQLite sidecar files present: wal={wal_path.exists()}, shm={shm_path.exists()}")

    def validate_users(self) -> None:
        check = "users"
        source_rows = {
            row["name"]: row
            for row in self.fetch_all(self.source, "select * from user where type = 0 order by id")
        }
        target_rows = {
            row["name"]: row
            for row in self.fetch_all(self.target, "select * from user where type = 0 order by id")
        }
        self.compare_key_sets(check, set(source_rows), set(target_rows))

        fields = [
            "email",
            "full_name",
            "location",
            "website",
            "language",
            "description",
            "visibility",
            "is_admin",
            "is_active",
            "is_restricted",
            "allow_git_hook",
            "allow_import_local",
            "allow_create_organization",
            "prohibit_login",
            "avatar",
            "avatar_email",
            "use_custom_avatar",
            "diff_view_style",
            "theme",
            "keep_activity_private",
        ]
        for username in sorted(set(source_rows) & set(target_rows)):
            source_row = source_rows[username]
            target_row = target_rows[username]
            for field in fields:
                self.compare_values(check, f"{username}.{field}", source_row[field], target_row[field])

        source_emails = self.user_email_map(self.source, source_rows)
        target_emails = self.user_email_map(self.target, target_rows)
        for username in sorted(set(source_rows) & set(target_rows)):
            if source_emails[username] != target_emails[username]:
                self.add_failure(
                    check,
                    f"{username}.emails mismatch: expected {source_emails[username]!r}, found {target_emails[username]!r}",
                )

        self.add_note(f"Validated {len(source_rows)} user records and email sets")

    def user_email_map(
        self,
        connection: sqlite3.Connection,
        user_rows: dict[str, sqlite3.Row],
    ) -> dict[str, list[tuple[str, int, int]]]:
        mapping: dict[str, list[tuple[str, int, int]]] = {}
        for username, row in user_rows.items():
            email_rows = list(
                connection.execute(
                    """
                    select email, is_primary, is_activated
                    from email_address
                    where uid = ?
                    order by lower_email, is_primary desc, is_activated desc
                    """,
                    (row["id"],),
                ).fetchall()
            )
            emails = [
                (
                    normalize_text(email_row["email"]).lower(),
                    normalize_int(email_row["is_primary"]),
                    normalize_int(email_row["is_activated"]),
                )
                for email_row in email_rows
            ]
            primary_email = normalize_text(row["email"]).lower()
            if primary_email and not any(email == primary_email for email, _, _ in emails):
                emails.insert(0, (primary_email, 1, 1))
            mapping[username] = sorted(set(emails))
        return mapping

    def validate_organizations(self) -> None:
        check = "organizations"
        source_rows = {
            row["name"]: row
            for row in self.fetch_all(self.source, "select * from user where type = 1 order by id")
        }
        target_rows = {
            row["name"]: row
            for row in self.fetch_all(self.target, "select * from user where type = 1 order by id")
        }
        self.compare_key_sets(check, set(source_rows), set(target_rows))

        fields = [
            "full_name",
            "email",
            "location",
            "website",
            "description",
            "visibility",
            "avatar",
            "avatar_email",
            "use_custom_avatar",
            "repo_admin_change_team_access",
        ]
        for org_name in sorted(set(source_rows) & set(target_rows)):
            source_row = source_rows[org_name]
            target_row = target_rows[org_name]
            for field in fields:
                self.compare_values(check, f"{org_name}.{field}", source_row[field], target_row[field])

        self.add_note(f"Validated {len(source_rows)} organization records")

    def validate_org_memberships(self) -> None:
        check = "org-memberships"
        source_rows = {
            (
                row["org_name"],
                row["member_name"],
                normalize_int(row["is_public"]),
            )
            for row in self.fetch_all(
                self.source,
                """
                select org.name as org_name, member.name as member_name, org_user.is_public
                from org_user
                join user org on org.id = org_user.org_id
                join user member on member.id = org_user.uid
                order by org.name, member.name
                """,
            )
        }
        target_rows = {
            (
                row["org_name"],
                row["member_name"],
                normalize_int(row["is_public"]),
            )
            for row in self.fetch_all(
                self.target,
                """
                select org.name as org_name, member.name as member_name, org_user.is_public
                from org_user
                join user org on org.id = org_user.org_id
                join user member on member.id = org_user.uid
                order by org.name, member.name
                """,
            )
        }
        self.compare_key_sets(check, source_rows, target_rows)
        self.add_note(f"Validated {len(source_rows)} organization membership rows")

    def validate_teams(self) -> None:
        team_check = "teams"
        source_teams = {
            (
                row["org_name"],
                row["team_name"],
                normalize_text(row["description"]),
                normalize_int(row["includes_all_repositories"]),
                normalize_int(row["can_create_org_repo"]),
            )
            for row in self.fetch_all(
                self.source,
                """
                select org.name as org_name,
                       team.name as team_name,
                       team.description,
                       team.includes_all_repositories,
                       team.can_create_org_repo
                from team
                join user org on org.id = team.org_id
                order by org.name, team.name
                """,
            )
        }
        target_teams = {
            (
                row["org_name"],
                row["team_name"],
                normalize_text(row["description"]),
                normalize_int(row["includes_all_repositories"]),
                normalize_int(row["can_create_org_repo"]),
            )
            for row in self.fetch_all(
                self.target,
                """
                select org.name as org_name,
                       team.name as team_name,
                       team.description,
                       team.includes_all_repositories,
                       team.can_create_org_repo
                from team
                join user org on org.id = team.org_id
                order by org.name, team.name
                """,
            )
        }
        self.compare_key_sets(team_check, source_teams, target_teams)

        membership_check = "team-memberships"
        source_memberships = {
            (row["org_name"], row["team_name"], row["member_name"])
            for row in self.fetch_all(
                self.source,
                """
                select org.name as org_name, team.name as team_name, member.name as member_name
                from team_user
                join team on team.id = team_user.team_id
                join user org on org.id = team.org_id
                join user member on member.id = team_user.uid
                order by org.name, team.name, member.name
                """,
            )
        }
        target_memberships = {
            (row["org_name"], row["team_name"], row["member_name"])
            for row in self.fetch_all(
                self.target,
                """
                select org.name as org_name, team.name as team_name, member.name as member_name
                from team_user
                join team on team.id = team_user.team_id
                join user org on org.id = team.org_id
                join user member on member.id = team_user.uid
                order by org.name, team.name, member.name
                """,
            )
        }
        self.compare_key_sets(membership_check, source_memberships, target_memberships)

        unit_check = "team-units"
        source_units = {
            (row["org_name"], row["team_name"], normalize_int(row["type"]), normalize_int(row["access_mode"]))
            for row in self.fetch_all(
                self.source,
                """
                select org.name as org_name, team.name as team_name, team_unit.type, team_unit.access_mode
                from team_unit
                join team on team.id = team_unit.team_id
                join user org on org.id = team.org_id
                order by org.name, team.name, team_unit.type
                """,
            )
        }
        target_units = {
            (row["org_name"], row["team_name"], normalize_int(row["type"]), normalize_int(row["access_mode"]))
            for row in self.fetch_all(
                self.target,
                """
                select org.name as org_name, team.name as team_name, team_unit.type, team_unit.access_mode
                from team_unit
                join team on team.id = team_unit.team_id
                join user org on org.id = team.org_id
                order by org.name, team.name, team_unit.type
                """,
            )
        }
        self.compare_key_sets(unit_check, source_units, target_units)
        self.add_note(
            f"Validated {len(source_teams)} teams, {len(source_memberships)} team memberships, and {len(source_units)} team unit rows"
        )

    def validate_repositories(self) -> None:
        check = "repositories"
        source_rows = {
            (row["owner_name"], row["lower_name"]): row
            for row in self.fetch_all(self.source, "select * from repository order by owner_name, lower_name")
        }
        target_rows = {
            (row["owner_name"], row["lower_name"]): row
            for row in self.fetch_all(
                self.target,
                """
                select repository.*, owner.name as owner_name
                from repository
                join user owner on owner.id = repository.owner_id
                order by owner.name, repository.lower_name
                """,
            )
        }
        self.compare_key_sets(check, set(source_rows), set(target_rows))

        fields = [
            "name",
            "description",
            "website",
            "original_service_type",
            "original_url",
            "default_branch",
            "num_watches",
            "num_stars",
            "num_forks",
            "num_milestones",
            "num_closed_milestones",
            "num_projects",
            "num_closed_projects",
            "is_private",
            "is_empty",
            "is_archived",
            "is_mirror",
            "status",
            "is_fork",
            "fork_id",
            "is_template",
            "template_id",
            "object_format_name",
            "trust_model",
            "avatar",
        ]
        for repo_key in sorted(set(source_rows) & set(target_rows)):
            source_row = source_rows[repo_key]
            target_row = target_rows[repo_key]
            label = f"{repo_key[0]}/{source_row['name']}"
            for field in fields:
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

        self.add_note(f"Validated {len(source_rows)} repository metadata rows")

    def validate_project_and_social_data(self) -> None:
        check = "project-social"

        source_user_names = {
            row["id"]: row["name"]
            for row in self.fetch_all(self.source, "select id, name from user order by id")
        }
        target_user_names = {
            row["id"]: row["name"]
            for row in self.fetch_all(self.target, "select id, name from user order by id")
        }
        source_repo_keys = {
            row["id"]: (row["owner_name"], row["lower_name"])
            for row in self.fetch_all(self.source, "select id, owner_name, lower_name from repository order by id")
        }
        target_repo_keys = {
            row["id"]: (row["owner_name"], row["lower_name"])
            for row in self.fetch_all(
                self.target,
                """
                select repository.id, owner.name as owner_name, repository.lower_name
                from repository
                join user owner on owner.id = repository.owner_id
                order by repository.id
                """,
            )
        }

        def compare_entity(entity: str, source_rows: dict[int, tuple[Any, ...]], target_rows: dict[int, tuple[Any, ...]]) -> None:
            entity_check = f"{check}:{entity}"
            self.compare_key_sets(entity_check, set(source_rows), set(target_rows))
            for row_id in sorted(set(source_rows) & set(target_rows)):
                if source_rows[row_id] != target_rows[row_id]:
                    self.add_failure(
                        entity_check,
                        f"{entity} {row_id} mismatch: expected {source_rows[row_id]!r}, found {target_rows[row_id]!r}",
                    )

        source_labels = {
            row["id"]: (
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                source_user_names.get(normalize_int(row["org_id"]), ""),
                normalize_text(row["name"]),
                normalize_int(row["exclusive"]),
                normalize_text(row["description"]),
                normalize_text(row["color"]),
                normalize_int(row["num_issues"]),
                normalize_int(row["num_closed_issues"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["archived_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from label order by id")
        }
        target_labels = {
            row["id"]: (
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                target_user_names.get(normalize_int(row["org_id"]), ""),
                normalize_text(row["name"]),
                normalize_int(row["exclusive"]),
                normalize_text(row["description"]),
                normalize_text(row["color"]),
                normalize_int(row["num_issues"]),
                normalize_int(row["num_closed_issues"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["archived_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from label order by id")
        }
        compare_entity("labels", source_labels, target_labels)

        source_milestones = {
            row["id"]: (
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_text(row["name"]),
                normalize_text(row["content"]),
                normalize_int(row["is_closed"]),
                normalize_int(row["num_issues"]),
                normalize_int(row["num_closed_issues"]),
                normalize_int(row["completeness"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["deadline_unix"]),
                normalize_int(row["closed_date_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from milestone order by id")
        }
        target_milestones = {
            row["id"]: (
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_text(row["name"]),
                normalize_text(row["content"]),
                normalize_int(row["is_closed"]),
                normalize_int(row["num_issues"]),
                normalize_int(row["num_closed_issues"]),
                normalize_int(row["completeness"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["deadline_unix"]),
                normalize_int(row["closed_date_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from milestone order by id")
        }
        compare_entity("milestones", source_milestones, target_milestones)

        source_issues = {
            row["id"]: (
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["index"]),
                source_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_text(row["original_author"]),
                source_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["name"]),
                normalize_text(row["content"]),
                normalize_int(row["content_version"]),
                normalize_int(row["milestone_id"]),
                normalize_int(row["priority"]),
                normalize_int(row["is_closed"]),
                normalize_int(row["is_pull"]),
                normalize_int(row["num_comments"]),
                normalize_text(row["ref"]),
                normalize_int(row["deadline_unix"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["closed_unix"]),
                normalize_int(row["is_locked"]),
            )
            for row in self.fetch_all(self.source, "select * from issue order by id")
        }
        target_issues = {
            row["id"]: (
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["index"]),
                target_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_text(row["original_author"]),
                target_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["name"]),
                normalize_text(row["content"]),
                normalize_int(row["content_version"]),
                normalize_int(row["milestone_id"]),
                normalize_int(row["priority"]),
                normalize_int(row["is_closed"]),
                normalize_int(row["is_pull"]),
                normalize_int(row["num_comments"]),
                normalize_text(row["ref"]),
                normalize_int(row["deadline_unix"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_int(row["closed_unix"]),
                normalize_int(row["is_locked"]),
            )
            for row in self.fetch_all(self.target, "select * from issue order by id")
        }
        compare_entity("issues", source_issues, target_issues)

        source_issue_labels = {
            row["id"]: (normalize_int(row["issue_id"]), normalize_int(row["label_id"]))
            for row in self.fetch_all(self.source, "select * from issue_label order by id")
        }
        target_issue_labels = {
            row["id"]: (normalize_int(row["issue_id"]), normalize_int(row["label_id"]))
            for row in self.fetch_all(self.target, "select * from issue_label order by id")
        }
        compare_entity("issue-labels", source_issue_labels, target_issue_labels)

        source_issue_assignees = {
            row["id"]: (
                source_user_names.get(normalize_int(row["assignee_id"]), ""),
                normalize_int(row["issue_id"]),
            )
            for row in self.fetch_all(self.source, "select * from issue_assignees order by id")
        }
        target_issue_assignees = {
            row["id"]: (
                target_user_names.get(normalize_int(row["assignee_id"]), ""),
                normalize_int(row["issue_id"]),
            )
            for row in self.fetch_all(self.target, "select * from issue_assignees order by id")
        }
        compare_entity("issue-assignees", source_issue_assignees, target_issue_assignees)

        source_issue_users = {
            row["id"]: (
                source_user_names.get(normalize_int(row["uid"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["is_read"]),
                normalize_int(row["is_mentioned"]),
            )
            for row in self.fetch_all(self.source, "select * from issue_user order by id")
        }
        target_issue_users = {
            row["id"]: (
                target_user_names.get(normalize_int(row["uid"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["is_read"]),
                normalize_int(row["is_mentioned"]),
            )
            for row in self.fetch_all(self.target, "select * from issue_user order by id")
        }
        compare_entity("issue-users", source_issue_users, target_issue_users)

        source_comments = {
            row["id"]: (
                normalize_int(row["type"]),
                source_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_text(row["original_author"]),
                source_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["label_id"]),
                normalize_int(row["old_project_id"]),
                normalize_int(row["project_id"]),
                normalize_int(row["old_milestone_id"]),
                normalize_int(row["milestone_id"]),
                normalize_int(row["time_id"]),
                source_user_names.get(normalize_int(row["assignee_id"]), ""),
                normalize_int(row["removed_assignee"]),
                normalize_int(row["assignee_team_id"]),
                source_user_names.get(normalize_int(row["resolve_doer_id"]), ""),
                normalize_text(row["old_title"]),
                normalize_text(row["new_title"]),
                normalize_text(row["old_ref"]),
                normalize_text(row["new_ref"]),
                normalize_int(row["dependent_issue_id"]),
                normalize_int(row["commit_id"]),
                normalize_int(row["line"]),
                normalize_text(row["tree_path"]),
                normalize_text(row["content"]),
                normalize_text(row["patch"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_text(row["commit_sha"]),
                normalize_int(row["review_id"]),
                normalize_int(row["invalidated"]),
                source_repo_keys.get(normalize_int(row["ref_repo_id"]), ("", "")),
                normalize_int(row["ref_issue_id"]),
                normalize_int(row["ref_comment_id"]),
                normalize_int(row["ref_action"]),
                normalize_int(row["ref_is_pull"]),
                normalize_int(row["content_version"]),
            )
            for row in self.fetch_all(self.source, "select * from comment order by id")
        }
        target_comments = {
            row["id"]: (
                normalize_int(row["type"]),
                target_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_text(row["original_author"]),
                target_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["label_id"]),
                normalize_int(row["old_project_id"]),
                normalize_int(row["project_id"]),
                normalize_int(row["old_milestone_id"]),
                normalize_int(row["milestone_id"]),
                normalize_int(row["time_id"]),
                target_user_names.get(normalize_int(row["assignee_id"]), ""),
                normalize_int(row["removed_assignee"]),
                normalize_int(row["assignee_team_id"]),
                target_user_names.get(normalize_int(row["resolve_doer_id"]), ""),
                normalize_text(row["old_title"]),
                normalize_text(row["new_title"]),
                normalize_text(row["old_ref"]),
                normalize_text(row["new_ref"]),
                normalize_int(row["dependent_issue_id"]),
                normalize_int(row["commit_id"]),
                normalize_int(row["line"]),
                normalize_text(row["tree_path"]),
                normalize_text(row["content"]),
                normalize_text(row["patch"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
                normalize_text(row["commit_sha"]),
                normalize_int(row["review_id"]),
                normalize_int(row["invalidated"]),
                target_repo_keys.get(normalize_int(row["ref_repo_id"]), ("", "")),
                normalize_int(row["ref_issue_id"]),
                normalize_int(row["ref_comment_id"]),
                normalize_int(row["ref_action"]),
                normalize_int(row["ref_is_pull"]),
                normalize_int(row["content_version"]),
            )
            for row in self.fetch_all(self.target, "select * from comment order by id")
        }
        compare_entity("comments", source_comments, target_comments)

        source_pull_requests = {
            row["id"]: (
                normalize_int(row["type"]),
                normalize_int(row["status"]),
                normalize_text(row["conflicted_files"]),
                normalize_int(row["commits_ahead"]),
                normalize_int(row["commits_behind"]),
                normalize_text(row["changed_protected_files"]),
                normalize_int(row["issue_id"]),
                normalize_int(row["index"]),
                source_repo_keys.get(normalize_int(row["head_repo_id"]), ("", "")),
                source_repo_keys.get(normalize_int(row["base_repo_id"]), ("", "")),
                normalize_text(row["head_branch"]),
                normalize_text(row["base_branch"]),
                normalize_text(row["merge_base"]),
                normalize_int(row["allow_maintainer_edit"]),
                normalize_int(row["has_merged"]),
                normalize_text(row["merged_commit_id"]),
                source_user_names.get(normalize_int(row["merger_id"]), ""),
                normalize_int(row["merged_unix"]),
                normalize_int(row["flow"]),
            )
            for row in self.fetch_all(self.source, "select * from pull_request order by id")
        }
        target_pull_requests = {
            row["id"]: (
                normalize_int(row["type"]),
                normalize_int(row["status"]),
                normalize_text(row["conflicted_files"]),
                normalize_int(row["commits_ahead"]),
                normalize_int(row["commits_behind"]),
                normalize_text(row["changed_protected_files"]),
                normalize_int(row["issue_id"]),
                normalize_int(row["index"]),
                target_repo_keys.get(normalize_int(row["head_repo_id"]), ("", "")),
                target_repo_keys.get(normalize_int(row["base_repo_id"]), ("", "")),
                normalize_text(row["head_branch"]),
                normalize_text(row["base_branch"]),
                normalize_text(row["merge_base"]),
                normalize_int(row["allow_maintainer_edit"]),
                normalize_int(row["has_merged"]),
                normalize_text(row["merged_commit_id"]),
                target_user_names.get(normalize_int(row["merger_id"]), ""),
                normalize_int(row["merged_unix"]),
                normalize_int(row["flow"]),
            )
            for row in self.fetch_all(self.target, "select * from pull_request order by id")
        }
        compare_entity("pull-requests", source_pull_requests, target_pull_requests)

        source_issue_history = {
            row["id"]: (
                source_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["comment_id"]),
                normalize_int(row["edited_unix"]),
                normalize_text(row["content_text"]),
                normalize_int(row["is_first_created"]),
                normalize_int(row["is_deleted"]),
            )
            for row in self.fetch_all(self.source, "select * from issue_content_history order by id")
        }
        target_issue_history = {
            row["id"]: (
                target_user_names.get(normalize_int(row["poster_id"]), ""),
                normalize_int(row["issue_id"]),
                normalize_int(row["comment_id"]),
                normalize_int(row["edited_unix"]),
                normalize_text(row["content_text"]),
                normalize_int(row["is_first_created"]),
                normalize_int(row["is_deleted"]),
            )
            for row in self.fetch_all(self.target, "select * from issue_content_history order by id")
        }
        compare_entity("issue-history", source_issue_history, target_issue_history)

        source_reactions = {
            row["id"]: (
                normalize_text(row["type"]),
                normalize_int(row["issue_id"]),
                normalize_int(row["comment_id"]),
                source_user_names.get(normalize_int(row["user_id"]), ""),
                source_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["original_author"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from reaction order by id")
        }
        target_reactions = {
            row["id"]: (
                normalize_text(row["type"]),
                normalize_int(row["issue_id"]),
                normalize_int(row["comment_id"]),
                target_user_names.get(normalize_int(row["user_id"]), ""),
                target_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["original_author"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from reaction order by id")
        }
        compare_entity("reactions", source_reactions, target_reactions)

        source_releases = {
            row["id"]: (
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                source_user_names.get(normalize_int(row["publisher_id"]), ""),
                normalize_text(row["tag_name"]),
                normalize_text(row["original_author"]),
                source_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["lower_tag_name"]),
                normalize_text(row["target"]),
                normalize_text(row["title"]),
                normalize_text(row["sha1"]),
                normalize_int(row["num_commits"]),
                normalize_text(row["note"]),
                normalize_int(row["is_draft"]),
                normalize_int(row["is_prerelease"]),
                normalize_int(row["is_tag"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from release order by id")
        }
        target_releases = {
            row["id"]: (
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                target_user_names.get(normalize_int(row["publisher_id"]), ""),
                normalize_text(row["tag_name"]),
                normalize_text(row["original_author"]),
                target_user_names.get(normalize_int(row["original_author_id"]), ""),
                normalize_text(row["lower_tag_name"]),
                normalize_text(row["target"]),
                normalize_text(row["title"]),
                normalize_text(row["sha1"]),
                normalize_int(row["num_commits"]),
                normalize_text(row["note"]),
                normalize_int(row["is_draft"]),
                normalize_int(row["is_prerelease"]),
                normalize_int(row["is_tag"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from release order by id")
        }
        compare_entity("releases", source_releases, target_releases)

        source_uploads = {
            row["id"]: (normalize_text(row["uuid"]), normalize_text(row["name"]))
            for row in self.fetch_all(self.source, "select * from upload order by id")
        }
        target_uploads = {
            row["id"]: (normalize_text(row["uuid"]), normalize_text(row["name"]))
            for row in self.fetch_all(self.target, "select * from upload order by id")
        }
        compare_entity("uploads", source_uploads, target_uploads)

        source_attachments = {
            row["id"]: (
                normalize_text(row["uuid"]),
                source_user_names.get(normalize_int(row["uploader_id"]), ""),
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["issue_id"]),
                normalize_int(row["release_id"]),
                normalize_int(row["comment_id"]),
                normalize_text(row["name"]),
                normalize_int(row["download_count"]),
                normalize_int(row["size"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from attachment order by id")
        }
        target_attachments = {
            row["id"]: (
                normalize_text(row["uuid"]),
                target_user_names.get(normalize_int(row["uploader_id"]), ""),
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["issue_id"]),
                normalize_int(row["release_id"]),
                normalize_int(row["comment_id"]),
                normalize_text(row["name"]),
                normalize_int(row["download_count"]),
                normalize_int(row["size"]),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from attachment order by id")
        }
        compare_entity("attachments", source_attachments, target_attachments)

        for row in self.fetch_all(self.source, "select uuid, name from attachment order by id"):
            uuid = normalize_text(row["uuid"])
            if not uuid:
                continue
            source_path = self.backup_root / "data" / "attachments" / uuid[0] / uuid[1] / uuid
            target_path = self.forgejo_root / "data" / "attachments" / uuid[0] / uuid[1] / uuid
            self.compare_file_contents(check, f"attachment:{normalize_text(row['name'])}", source_path, target_path)

        source_stars = {
            row["id"]: (
                source_user_names.get(normalize_int(row["uid"]), ""),
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from star order by id")
        }
        target_stars = {
            row["id"]: (
                target_user_names.get(normalize_int(row["uid"]), ""),
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["created_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from star order by id")
        }
        compare_entity("stars", source_stars, target_stars)

        source_watches = {
            row["id"]: (
                source_user_names.get(normalize_int(row["user_id"]), ""),
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["mode"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from watch order by id")
        }
        target_watches = {
            row["id"]: (
                target_user_names.get(normalize_int(row["user_id"]), ""),
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["mode"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from watch order by id")
        }
        compare_entity("watches", source_watches, target_watches)

        source_collaboration = {
            row["id"]: (
                source_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                source_user_names.get(normalize_int(row["user_id"]), ""),
                normalize_int(row["mode"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
            )
            for row in self.fetch_all(self.source, "select * from collaboration order by id")
        }
        target_collaboration = {
            row["id"]: (
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                target_user_names.get(normalize_int(row["user_id"]), ""),
                normalize_int(row["mode"]),
                normalize_int(row["created_unix"]),
                normalize_int(row["updated_unix"]),
            )
            for row in self.fetch_all(self.target, "select * from collaboration order by id")
        }
        compare_entity("collaboration", source_collaboration, target_collaboration)

        self.add_note(
            "Validated "
            f"{len(source_issues)} issues, "
            f"{len(source_comments)} comments, "
            f"{len(source_pull_requests)} pull requests, "
            f"{len(source_releases)} releases, "
            f"{len(source_attachments)} attachments, "
            f"{len(source_stars)} stars, "
            f"{len(source_watches)} watches, and "
            f"{len(source_collaboration)} collaborators"
        )

    def validate_git_repositories(self) -> None:
        check = "git-repositories"
        source_rows = self.fetch_all(self.source, "select owner_name, lower_name, name from repository order by owner_name, lower_name")
        validated = 0
        total_refs = 0

        for row in source_rows:
            owner_name = row["owner_name"]
            lower_name = row["lower_name"]
            repo_label = f"{owner_name}/{row['name']}"

            source_path = self.backup_root / "repos" / owner_name / f"{lower_name}.git"
            target_path = self.forgejo_root / "data" / "forgejo-repositories" / owner_name / f"{lower_name}.git"

            if not source_path.exists():
                self.add_failure(check, f"{repo_label}: missing source bare repository at {source_path}")
                continue
            if not target_path.exists():
                self.add_failure(check, f"{repo_label}: missing target bare repository at {target_path}")
                continue

            source_head = self.git_head_text(source_path)
            target_head = self.git_head_text(target_path)
            self.compare_values(check, f"{repo_label}.HEAD", source_head, target_head)

            try:
                source_refs = self.git_ref_map(source_path)
                target_refs = self.git_ref_map(target_path)
            except subprocess.CalledProcessError as exc:
                self.add_failure(check, f"{repo_label}: failed to enumerate refs: {exc.stderr.strip() or exc.stdout.strip()}")
                continue

            if source_refs != target_refs:
                missing_refs = sorted(set(source_refs) - set(target_refs))
                extra_refs = sorted(set(target_refs) - set(source_refs))
                mismatched_refs = sorted(
                    ref_name
                    for ref_name in set(source_refs) & set(target_refs)
                    if source_refs[ref_name] != target_refs[ref_name]
                )
                if missing_refs:
                    self.add_failure(check, f"{repo_label}: missing refs {sample_values(missing_refs)}")
                if extra_refs:
                    self.add_failure(check, f"{repo_label}: extra refs {sample_values(extra_refs)}")
                if mismatched_refs:
                    sample = mismatched_refs[:5]
                    detail = ", ".join(
                        f"{ref_name} ({source_refs[ref_name]} != {target_refs[ref_name]})" for ref_name in sample
                    )
                    suffix = "" if len(mismatched_refs) <= 5 else f", ... (+{len(mismatched_refs) - 5} more)"
                    self.add_failure(check, f"{repo_label}: mismatched refs {detail}{suffix}")

            self.git_fsck(target_path)
            validated += 1
            total_refs += len(source_refs)

        self.add_note(f"Validated Git data for {validated} repositories and {total_refs} refs")

    def validate_ssh_keys(self) -> None:
        check = "ssh-keys"
        source_keys = {
            (row["owner_name"], row["key_name"], row["content"])
            for row in self.fetch_all(
                self.source,
                """
                select user.name as owner_name, public_key.name as key_name, public_key.content
                from public_key
                join user on user.id = public_key.owner_id
                order by user.name, public_key.name, public_key.id
                """,
            )
        }
        target_keys = {
            (row["owner_name"], row["key_name"], row["content"])
            for row in self.fetch_all(
                self.target,
                """
                select user.name as owner_name, public_key.name as key_name, public_key.content
                from public_key
                join user on user.id = public_key.owner_id
                order by user.name, public_key.name, public_key.id
                """,
            )
        }
        self.compare_key_sets(check, source_keys, target_keys)
        self.add_note(f"Validated {len(source_keys)} SSH public keys")

    def validate_avatars(self) -> None:
        check = "avatars"
        validated = 0

        for row in self.fetch_all(self.source, "select name, type, avatar, use_custom_avatar from user order by id"):
            avatar_name = normalize_text(row["avatar"])
            should_compare = avatar_name and normalize_int(row["use_custom_avatar"]) == 1
            if not should_compare:
                continue

            source_path = self.backup_root / "data" / "avatars" / avatar_name
            target_path = self.forgejo_root / "data" / "avatars" / avatar_name
            label = f"user-avatar:{row['name']}"
            self.compare_file_contents(check, label, source_path, target_path)
            validated += 1

        for row in self.fetch_all(self.source, "select owner_name, name, avatar from repository order by id"):
            avatar_name = normalize_text(row["avatar"])
            if not avatar_name:
                continue

            source_path = self.backup_root / "data" / "repo-avatars" / avatar_name
            target_path = self.forgejo_root / "data" / "repo-avatars" / avatar_name
            label = f"repo-avatar:{row['owner_name']}/{row['name']}"
            self.compare_file_contents(check, label, source_path, target_path)
            validated += 1

        self.add_note(f"Validated {validated} avatar files")

    def compare_file_contents(self, check: str, label: str, source_path: Path, target_path: Path) -> None:
        if not source_path.exists():
            self.add_failure(check, f"{label}: missing source file {source_path}")
            return
        if not target_path.exists():
            self.add_failure(check, f"{label}: missing target file {target_path}")
            return

        source_hash = self.sha256_file(source_path)
        target_hash = self.sha256_file(target_path)
        if source_hash != target_hash:
            self.add_failure(check, f"{label}: SHA-256 mismatch ({source_hash} != {target_hash})")

    def validate_pull_mirrors(self) -> None:
        check = "pull-mirrors"
        source_rows = self.fetch_all(
            self.source,
            """
            select repository.owner_name,
                   repository.name,
                   repository.lower_name,
                   mirror.interval,
                   mirror.enable_prune,
                   mirror.updated_unix,
                   mirror.next_update_unix,
                   mirror.lfs_enabled,
                   mirror.lfs_endpoint
            from mirror
            join repository on repository.id = mirror.repo_id
            order by repository.owner_name, repository.lower_name
            """,
        )
        expected_rows = {
            (row["owner_name"], row["lower_name"]): row
            for row in source_rows
            if (normalize_text(row["owner_name"]).lower(), normalize_text(row["name"]).lower()) not in self.warning_repo_keys
        }
        target_rows = {
            (row["owner_name"], row["lower_name"]): row
            for row in self.fetch_all(
                self.target,
                """
                select owner.name as owner_name,
                       repository.name,
                       repository.lower_name,
                       mirror.interval,
                       mirror.enable_prune,
                       mirror.updated_unix,
                       mirror.next_update_unix,
                       mirror.lfs_enabled,
                       mirror.lfs_endpoint
                from mirror
                join repository on repository.id = mirror.repo_id
                join user owner on owner.id = repository.owner_id
                order by owner.name, repository.lower_name
                """,
            )
        }
        self.compare_key_sets(check, set(expected_rows), set(target_rows))

        for repo_key in sorted(set(expected_rows) & set(target_rows)):
            source_row = expected_rows[repo_key]
            target_row = target_rows[repo_key]
            label = f"{repo_key[0]}/{source_row['name']}"
            for field in ("interval", "enable_prune", "updated_unix", "next_update_unix", "lfs_enabled", "lfs_endpoint"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

        self.add_note(
            f"Validated {len(expected_rows)} active pull mirror rows with {len(self.warning_repo_keys)} expected fallbacks"
        )

    def validate_push_mirrors(self) -> None:
        check = "push-mirrors"
        source_rows = {
            (row["owner_name"], row["lower_name"], row["remote_name"], row["remote_address"]): row
            for row in self.fetch_all(
                self.source,
                """
                select repository.owner_name,
                       repository.name,
                       repository.lower_name,
                       push_mirror.remote_name,
                       push_mirror.remote_address,
                       push_mirror.sync_on_commit,
                       push_mirror.interval,
                       push_mirror.created_unix,
                       push_mirror.last_update,
                       push_mirror.last_error
                from push_mirror
                join repository on repository.id = push_mirror.repo_id
                order by repository.owner_name, repository.lower_name, push_mirror.id
                """,
            )
        }
        target_rows = {
            (row["owner_name"], row["lower_name"], row["remote_name"], row["remote_address"]): row
            for row in self.fetch_all(
                self.target,
                """
                select owner.name as owner_name,
                       repository.name,
                       repository.lower_name,
                       push_mirror.remote_name,
                       push_mirror.remote_address,
                       push_mirror.sync_on_commit,
                       push_mirror.interval,
                       push_mirror.created_unix,
                       push_mirror.last_update,
                       push_mirror.last_error
                from push_mirror
                join repository on repository.id = push_mirror.repo_id
                join user owner on owner.id = repository.owner_id
                order by owner.name, repository.lower_name, push_mirror.id
                """,
            )
        }
        self.compare_key_sets(check, set(source_rows), set(target_rows))

        for mirror_key in sorted(set(source_rows) & set(target_rows)):
            source_row = source_rows[mirror_key]
            target_row = target_rows[mirror_key]
            label = f"{mirror_key[0]}/{source_row['name']}:{mirror_key[2]}"
            for field in ("sync_on_commit", "interval", "created_unix", "last_update", "last_error"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

        self.add_note(f"Validated {len(source_rows)} push mirror rows")

    def validate_packages(self) -> None:
        check = "packages"
        source_packages = {
            (row["owner_name"], row["package_name"], row["package_type"]): row
            for row in self.fetch_all(
                self.source,
                """
                select owner.name as owner_name,
                       package.id,
                       package.repo_id,
                       package.type as package_type,
                       package.name as package_name,
                       package.lower_name,
                       package.semver_compatible,
                       package.is_internal,
                       repository.owner_name as repo_owner_name,
                       repository.lower_name as repo_lower_name
                from package
                join user owner on owner.id = package.owner_id
                left join repository on repository.id = package.repo_id
                order by owner.name, package.lower_name
                """,
            )
        }
        target_packages = {
            (row["owner_name"], row["package_name"], row["package_type"]): row
            for row in self.fetch_all(
                self.target,
                """
                select owner.name as owner_name,
                       package.id,
                       package.repo_id,
                       package.type as package_type,
                       package.name as package_name,
                       package.lower_name,
                       package.semver_compatible,
                       package.is_internal,
                       repo_owner.name as repo_owner_name,
                       repository.lower_name as repo_lower_name
                from package
                join user owner on owner.id = package.owner_id
                left join repository on repository.id = package.repo_id
                left join user repo_owner on repo_owner.id = repository.owner_id
                order by owner.name, package.lower_name
                """,
            )
        }
        self.compare_key_sets(check, set(source_packages), set(target_packages))

        package_id_map: dict[int, int] = {}
        for package_key in sorted(set(source_packages) & set(target_packages)):
            source_row = source_packages[package_key]
            target_row = target_packages[package_key]
            package_id_map[source_row["id"]] = target_row["id"]
            label = f"{package_key[0]}/{package_key[1]}"
            for field in ("lower_name", "semver_compatible", "is_internal"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])
            self.compare_values(
                check,
                f"{label}.repo",
                (normalize_text(source_row["repo_owner_name"]), normalize_text(source_row["repo_lower_name"])),
                (normalize_text(target_row["repo_owner_name"]), normalize_text(target_row["repo_lower_name"])),
            )

        source_versions = {
            (
                row["owner_name"],
                row["package_name"],
                row["version"],
            ): row
            for row in [
                row
                for row in self.fetch_all(
                    self.source,
                    """
                    select package_version.*,
                           owner.name as owner_name,
                           package.name as package_name
                    from package_version
                    join package on package.id = package_version.package_id
                    join user owner on owner.id = package.owner_id
                    order by package_version.id
                    """,
                )
                if row["id"] in {version_row["id"] for version_row in self.retained_source_package_versions}
            ]
        }
        target_versions = {
            (
                row["owner_name"],
                row["package_name"],
                row["version"],
            ): row
            for row in self.fetch_all(
                self.target,
                """
                select package_version.*,
                       owner.name as owner_name,
                       package.name as package_name,
                       creator.name as creator_name
                from package_version
                join package on package.id = package_version.package_id
                join user owner on owner.id = package.owner_id
                left join user creator on creator.id = package_version.creator_id
                order by package_version.id
                """,
            )
        }
        self.compare_key_sets(check, set(source_versions), set(target_versions))

        version_id_map: dict[int, int] = {}
        source_user_names = {
            row["id"]: row["name"]
            for row in self.fetch_all(self.source, "select id, name from user order by id")
        }
        for version_key in sorted(set(source_versions) & set(target_versions)):
            source_row = source_versions[version_key]
            target_row = target_versions[version_key]
            version_id_map[source_row["id"]] = target_row["id"]
            label = f"{version_key[0]}/{version_key[1]}:{version_key[2]}"
            self.compare_values(check, f"{label}.package", package_id_map[source_row["package_id"]], target_row["package_id"])
            self.compare_values(
                check,
                f"{label}.creator",
                normalize_text(source_user_names.get(normalize_int(source_row["creator_id"]), "")),
                normalize_text(target_row["creator_name"]),
            )
            for field in ("lower_version", "created_unix", "is_internal", "metadata_json", "download_count"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

        source_blobs = {
            row["hash_sha256"]: row
            for row in self.retained_source_package_blobs
        }
        target_blobs = {
            row["hash_sha256"]: row
            for row in self.fetch_all(self.target, "select * from package_blob order by id")
        }
        self.compare_key_sets(check, set(source_blobs), set(target_blobs))

        blob_id_map: dict[int, int] = {}
        for sha256_hash in sorted(set(source_blobs) & set(target_blobs)):
            source_row = source_blobs[sha256_hash]
            target_row = target_blobs[sha256_hash]
            blob_id_map[source_row["id"]] = target_row["id"]
            label = f"blob:{sha256_hash[:16]}"
            for field in ("size", "hash_md5", "hash_sha1", "hash_sha256", "hash_sha512", "created_unix"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

            rel = Path(sha256_hash[:2]) / sha256_hash[2:4] / sha256_hash
            source_path = self.backup_root / "data" / "packages" / rel
            target_path = self.forgejo_root / "data" / "packages" / rel
            self.compare_file_contents(check, f"package-blob:{sha256_hash[:16]}", source_path, target_path)

        source_files = {
            (
                row["owner_name"],
                row["package_name"],
                row["version"],
                row["name"],
            ): row
            for row in [
                row
                for row in self.fetch_all(
                    self.source,
                    """
                    select package_file.*,
                           owner.name as owner_name,
                           package.name as package_name,
                           package_version.version
                    from package_file
                    join package_version on package_version.id = package_file.version_id
                    join package on package.id = package_version.package_id
                    join user owner on owner.id = package.owner_id
                    order by package_file.id
                    """,
                )
                if row["id"] in {file_row["id"] for file_row in self.retained_source_package_files}
            ]
        }
        target_files = {
            (
                row["owner_name"],
                row["package_name"],
                row["version"],
                row["name"],
            ): row
            for row in self.fetch_all(
                self.target,
                """
                select package_file.*,
                       owner.name as owner_name,
                       package.name as package_name,
                       package_version.version
                from package_file
                join package_version on package_version.id = package_file.version_id
                join package on package.id = package_version.package_id
                join user owner on owner.id = package.owner_id
                order by package_file.id
                """,
            )
        }
        self.compare_key_sets(check, set(source_files), set(target_files))

        file_id_map: dict[int, int] = {}
        for file_key in sorted(set(source_files) & set(target_files)):
            source_row = source_files[file_key]
            target_row = target_files[file_key]
            file_id_map[source_row["id"]] = target_row["id"]
            label = f"{file_key[0]}/{file_key[1]}:{file_key[2]}/{file_key[3]}"
            self.compare_values(check, f"{label}.version_id", version_id_map[source_row["version_id"]], target_row["version_id"])
            self.compare_values(check, f"{label}.blob_id", blob_id_map[source_row["blob_id"]], target_row["blob_id"])
            for field in ("lower_name", "composite_key", "is_lead", "created_unix"):
                self.compare_values(check, f"{label}.{field}", source_row[field], target_row[field])

        retained_source_property_ids = {row["id"] for row in self.retained_source_package_properties}
        source_properties = {
            (
                normalize_int(row["ref_type"]),
                normalize_text(row["name"]),
                normalize_text(row["value"]),
                self.map_property_ref_key(row, package_id_map, version_id_map, file_id_map, source=True),
            )
            for row in self.fetch_all(self.source, "select * from package_property order by id")
            if row["id"] in retained_source_property_ids
        }
        target_properties = {
            (
                normalize_int(row["ref_type"]),
                normalize_text(row["name"]),
                normalize_text(row["value"]),
                self.map_property_ref_key(row, package_id_map, version_id_map, file_id_map, source=False),
            )
            for row in self.fetch_all(self.target, "select * from package_property order by id")
        }
        self.compare_key_sets(check, source_properties, target_properties)

        source_cleanup = {
            (
                row["owner_name"],
                row["type"],
                normalize_int(row["enabled"]),
                normalize_int(row["keep_count"]),
                normalize_text(row["keep_pattern"]),
                normalize_int(row["remove_days"]),
                normalize_text(row["remove_pattern"]),
                normalize_int(row["match_full_name"]),
            )
            for row in self.fetch_all(
                self.source,
                """
                select package_cleanup_rule.*, user.name as owner_name
                from package_cleanup_rule
                join user on user.id = package_cleanup_rule.owner_id
                order by package_cleanup_rule.id
                """,
            )
        }
        target_cleanup = {
            (
                row["owner_name"],
                row["type"],
                normalize_int(row["enabled"]),
                normalize_int(row["keep_count"]),
                normalize_text(row["keep_pattern"]),
                normalize_int(row["remove_days"]),
                normalize_text(row["remove_pattern"]),
                normalize_int(row["match_full_name"]),
            )
            for row in self.fetch_all(
                self.target,
                """
                select package_cleanup_rule.*, user.name as owner_name
                from package_cleanup_rule
                join user on user.id = package_cleanup_rule.owner_id
                order by package_cleanup_rule.id
                """,
            )
        }
        self.compare_key_sets(check, source_cleanup, target_cleanup)

        self.add_note(
            f"Validated {len(source_packages)} packages, {len(source_versions)} package versions, {len(source_files)} package files, and {len(source_blobs)} package blobs"
        )
        self.add_note(
            f"Forgejo package compatibility pruning accounted for {self.pruned_source_package_version_count} package versions, {self.pruned_source_package_file_count} package files, and {self.pruned_source_package_blob_count} package blobs"
        )

    def map_property_ref_key(
        self,
        row: sqlite3.Row,
        package_id_map: dict[int, int],
        version_id_map: dict[int, int],
        file_id_map: dict[int, int],
        *,
        source: bool,
    ) -> Any:
        ref_type = normalize_int(row["ref_type"])
        ref_id = normalize_int(row["ref_id"])
        if ref_type == 0:
            return ("version", version_id_map.get(ref_id, ref_id) if source else ref_id)
        if ref_type == 1:
            return ("file", file_id_map.get(ref_id, ref_id) if source else ref_id)
        if ref_type == 2:
            return ("package", package_id_map.get(ref_id, ref_id) if source else ref_id)
        return ("other", ref_type, ref_id)

    def validate_activity_feed(self) -> None:
        check = "activity-feed"
        source_user_names = {
            row["id"]: row["name"]
            for row in self.fetch_all(self.source, "select id, name from user order by id")
        }
        source_repo_keys = {
            row["id"]: (row["owner_name"], row["lower_name"])
            for row in self.fetch_all(self.source, "select id, owner_name, lower_name from repository order by id")
        }
        target_user_names = {
            row["id"]: row["name"]
            for row in self.fetch_all(self.target, "select id, name from user order by id")
        }
        target_repo_keys = {
            row["id"]: (row["owner_name"], row["lower_name"])
            for row in self.fetch_all(
                self.target,
                """
                select repository.id, owner.name as owner_name, repository.lower_name
                from repository
                join user owner on owner.id = repository.owner_id
                order by repository.id
                """,
            )
        }
        imported_comment_ids = {
            row["id"] for row in self.fetch_all(self.target, "select id from comment order by id")
        }

        expected_actions: dict[int, tuple[Any, ...]] = {}
        skipped = 0
        for row in self.fetch_all(self.source, "select * from action order by id"):
            op_type = normalize_int(row["op_type"])
            user_id = normalize_int(row["user_id"])
            act_user_id = normalize_int(row["act_user_id"])
            repo_id = normalize_int(row["repo_id"])
            comment_id = normalize_int(row["comment_id"])
            is_deleted = normalize_int(row["is_deleted"])

            if (
                op_type not in SUPPORTED_ACTIVITY_OP_TYPES
                or is_deleted != 0
                or (user_id and user_id not in source_user_names)
                or (act_user_id and act_user_id not in source_user_names)
                or (repo_id and repo_id not in source_repo_keys)
                or (comment_id and comment_id not in imported_comment_ids)
            ):
                skipped += 1
                continue

            expected_actions[row["id"]] = (
                op_type,
                source_user_names.get(user_id, ""),
                source_user_names.get(act_user_id, ""),
                source_repo_keys.get(repo_id, ("", "")),
                comment_id,
                normalize_text(row["ref_name"]),
                normalize_int(row["is_private"]),
                normalize_text(row["content"]),
                row["created_unix"],
            )

        actual_actions: dict[int, tuple[Any, ...]] = {}
        for row in self.fetch_all(self.target, "select * from action order by id"):
            actual_actions[row["id"]] = (
                normalize_int(row["op_type"]),
                target_user_names.get(normalize_int(row["user_id"]), ""),
                target_user_names.get(normalize_int(row["act_user_id"]), ""),
                target_repo_keys.get(normalize_int(row["repo_id"]), ("", "")),
                normalize_int(row["comment_id"]),
                normalize_text(row["ref_name"]),
                normalize_int(row["is_private"]),
                normalize_text(row["content"]),
                row["created_unix"],
            )

        self.compare_key_sets(check, set(expected_actions), set(actual_actions))
        for action_id in sorted(set(expected_actions) & set(actual_actions)):
            if expected_actions[action_id] != actual_actions[action_id]:
                self.add_failure(
                    check,
                    f"Action {action_id} mismatch: expected {expected_actions[action_id]!r}, found {actual_actions[action_id]!r}",
                )

        self.add_note(
            f"Validated {len(expected_actions)} activity rows and confirmed {skipped} skipped source rows after dependency remapping"
        )

    def write_report(self) -> None:
        status = "PASS" if not self.failures else "FAIL"
        lines = [
            "# Validation Report",
            "",
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
            "",
            f"Status: **{status}**",
            "",
            "## Summary",
            "",
            f"- Failures: {len(self.failures)}",
            f"- Notes: {len(self.notes)}",
            "",
            "## Notes",
            "",
        ]

        if self.notes:
            for note in self.notes:
                lines.append(f"- {note}")
        else:
            lines.append("- None")

        lines.extend(["", "## Failures", ""])
        if self.failures:
            for failure in self.failures:
                lines.append(f"- `{failure.check}`: {failure.detail}")
        else:
            lines.append("- None")

        lines.append("")
        self.report_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate that migrated Forgejo data matches the Gitea backup")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--forgejo-db", required=True, type=Path)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--forgejo-root", required=True, type=Path)
    parser.add_argument("--state-path", required=True, type=Path)
    parser.add_argument("--report-path", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validator = Validator(
        source_db=args.source_db,
        forgejo_db=args.forgejo_db,
        backup_root=args.backup_root,
        forgejo_root=args.forgejo_root,
        state_path=args.state_path,
        report_path=args.report_path,
    )
    exit_code = validator.run()
    if exit_code != 0:
        print(f"Validation failed. See {args.report_path}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
