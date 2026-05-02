#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ImportErrorWithContext(RuntimeError):
    pass


class ForgejoAPIError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        super().__init__(f"{method} {path} failed with {status}: {body}")
        self.method = method
        self.path = path
        self.status = status
        self.body = body


# Shared normalization helpers used across both migration phases.


def log(message: str) -> None:
    print(f"[import] {message}")


def visibility_from_int(value: Any) -> str:
    mapping = {0: "public", 1: "limited", 2: "private"}
    return mapping.get(int(value or 0), "public")


def bool_value(value: Any) -> bool:
    return bool(int(value or 0))


def nullable_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_int(value: Any) -> int:
    return int(value or 0)


def format_duration_from_ns(value: Any) -> str:
    total_seconds = int((value or 0)) // 1_000_000_000
    if total_seconds <= 0:
        return "0s"

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return "".join(parts)


def path_join(*segments: str) -> str:
    return "/".join(urllib.parse.quote(segment, safe="") for segment in segments)


SUPPORTED_ACTIVITY_OP_TYPES = {1, 2, 5, 6, 8, 9, 10, 12, 16, 17, 18, 19, 20, 24}


# Warnings are persisted between the API and finalize phases so later steps can
# distinguish expected mirror fallbacks from real migration mismatches.
@dataclass
class RepoWarning:
    owner: str
    name: str
    reason: str


def repo_warning_key(owner: Any, name: Any) -> tuple[str, str]:
    return (normalize_text(owner).lower(), normalize_text(name).lower())


# Thin API wrapper for the online/bootstrap phase.
class ForgejoAPI:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200, 201, 204),
    ) -> Any:
        data = None
        headers = {"Authorization": f"token {self.token}"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read()
                if response.status not in expected:
                    raise ForgejoAPIError(method, path, response.status, raw.decode("utf-8", "replace"))
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise ForgejoAPIError(method, path, exc.code, body) from exc


class Importer:
    def __init__(
        self,
        mode: str,
        source_db: Path,
        forgejo_db: Path,
        backup_root: Path,
        forgejo_root: Path,
        admin_username: str,
        password_mode: str,
        report_path: Path,
        state_path: Path,
        base_url: str | None = None,
        token: str | None = None,
    ) -> None:
        # Load the source snapshot up front so both phases operate on the same
        # view of the backup and the finalize phase can work offline.
        self.mode = mode
        self.source_db = source_db
        self.forgejo_db = forgejo_db
        self.backup_root = backup_root
        self.forgejo_root = forgejo_root
        self.admin_username = admin_username
        self.password_mode = password_mode
        self.report_path = report_path
        self.state_path = state_path
        self.api = ForgejoAPI(base_url, token) if base_url and token else None
        self.warnings: list[RepoWarning] = []
        self.imported_activity_count = 0
        self.skipped_activity_count = 0
        self.pruned_package_version_count = 0
        self.pruned_package_file_count = 0
        self.pruned_package_blob_count = 0

        self.source = sqlite3.connect(source_db)
        self.source.row_factory = sqlite3.Row

        self.target: sqlite3.Connection | None = None
        if self.mode == "finalize":
            self.target = sqlite3.connect(forgejo_db, timeout=60)
            self.target.row_factory = sqlite3.Row
            self.target.execute("PRAGMA busy_timeout = 60000")
            self.load_state()

        self.source_users = self.fetch_all("select * from user where type = 0 order by id")
        self.source_orgs = self.fetch_all("select * from user where type = 1 order by id")
        self.source_org_names = {row["name"] for row in self.source_orgs}
        self.source_emails = self.fetch_grouped("select * from email_address order by uid, id", "uid")
        self.source_keys = self.fetch_grouped("select * from public_key order by owner_id, id", "owner_id")
        self.source_teams = self.fetch_all("select * from team order by org_id, id")
        self.source_teams_by_org = self.group_rows(self.source_teams, "org_id")
        self.source_team_users = self.fetch_grouped("select * from team_user order by team_id, uid", "team_id")
        self.source_team_units = self.fetch_grouped("select * from team_unit order by team_id, type", "team_id")
        self.source_org_users = self.fetch_grouped("select * from org_user order by org_id, uid", "org_id")
        self.source_repositories = self.fetch_all("select * from repository order by id")
        self.source_repo_units = self.fetch_all("select * from repo_unit order by repo_id, type, id")
        self.source_labels = self.fetch_all("select * from label order by id")
        self.source_milestones = self.fetch_all("select * from milestone order by id")
        self.source_issues = self.fetch_all("select * from issue order by id")
        self.source_issue_labels = self.fetch_all("select * from issue_label order by id")
        self.source_issue_assignees = self.fetch_all("select * from issue_assignees order by id")
        self.source_issue_users = self.fetch_all("select * from issue_user order by id")
        self.source_issue_watches = self.fetch_all("select * from issue_watch order by id")
        self.source_comments = self.fetch_all("select * from comment order by id")
        self.source_pull_requests = self.fetch_all("select * from pull_request order by id")
        self.source_reviews = self.fetch_all("select * from review order by id")
        self.source_review_states = self.fetch_all("select * from review_state order by id")
        self.source_issue_content_history = self.fetch_all("select * from issue_content_history order by id")
        self.source_reactions = self.fetch_all("select * from reaction order by id")
        self.source_releases = self.fetch_all("select * from release order by id")
        self.source_uploads = self.fetch_all("select * from upload order by id")
        self.source_attachments = self.fetch_all("select * from attachment order by id")
        self.source_notifications = self.fetch_all("select * from notification order by id")
        self.source_stars = self.fetch_all("select * from star order by id")
        self.source_watches = self.fetch_all("select * from watch order by id")
        self.source_follows = self.fetch_all("select * from follow order by id")
        self.source_collaborations = self.fetch_all("select * from collaboration order by id")
        self.source_mirrors = {
            row["repo_id"]: row for row in self.fetch_all("select * from mirror order by repo_id")
        }
        self.source_push_mirrors = self.fetch_grouped(
            "select * from push_mirror order by repo_id, id",
            "repo_id",
        )
        self.source_packages = self.fetch_all("select * from package order by id")
        self.source_package_versions = self.fetch_all("select * from package_version order by id")
        self.source_package_files = self.fetch_all("select * from package_file order by id")
        self.source_package_blobs = self.fetch_all("select * from package_blob order by id")
        self.source_package_properties = self.fetch_all("select * from package_property order by id")
        self.source_package_cleanup_rules = self.fetch_all("select * from package_cleanup_rule order by id")
        (
            self.kept_source_package_versions,
            self.kept_source_package_files,
            self.kept_source_package_blobs,
            self.kept_source_package_properties,
        ) = self.compute_retained_package_rows()

    def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        cursor = self.source.execute(query, params)
        return list(cursor.fetchall())

    def fetch_grouped(self, query: str, key: str) -> dict[Any, list[sqlite3.Row]]:
        return self.group_rows(self.fetch_all(query), key)

    @staticmethod
    def group_rows(rows: list[sqlite3.Row], key: str) -> dict[Any, list[sqlite3.Row]]:
        grouped: dict[Any, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[row[key]].append(row)
        return grouped

    # Phase entrypoints.
    def run(self) -> None:
        if self.mode == "api":
            self.run_api_phase()
        else:
            self.run_finalize_phase()

    def run_api_phase(self) -> None:
        if self.api is None:
            raise ImportErrorWithContext("API mode requires --base-url and --token-file")

        log("Patching bootstrap users via the Forgejo API")
        for source_user in self.source_users:
            self.api.request(
                "PATCH",
                f"/api/v1/admin/users/{path_join(source_user['name'])}",
                {
                    "active": bool_value(source_user["is_active"]),
                    "admin": bool_value(source_user["is_admin"]),
                    "allow_create_organization": bool_value(source_user["allow_create_organization"]),
                    "allow_git_hook": bool_value(source_user["allow_git_hook"]),
                    "allow_import_local": bool_value(source_user["allow_import_local"]),
                    "description": nullable_text(source_user["description"]),
                    "email": nullable_text(source_user["email"]),
                    "full_name": nullable_text(source_user["full_name"]),
                    "hide_email": bool_value(source_user["keep_email_private"]),
                    "location": nullable_text(source_user["location"]),
                    "max_repo_creation": int(source_user["max_repo_creation"] or -1),
                    "must_change_password": False,
                    "prohibit_login": bool_value(source_user["prohibit_login"]),
                    "restricted": bool_value(source_user["is_restricted"]),
                    "visibility": visibility_from_int(source_user["visibility"]),
                    "website": nullable_text(source_user["website"]),
                },
            )

            for key_row in self.source_keys.get(source_user["id"], []):
                self.api.request(
                    "POST",
                    f"/api/v1/admin/users/{path_join(source_user['name'])}/keys",
                    {"title": key_row["name"], "key": key_row["content"], "read_only": False},
                )

        log("Creating organizations and teams via the Forgejo API")
        for source_org in self.source_orgs:
            self.api.request(
                "POST",
                f"/api/v1/admin/users/{path_join(self.admin_username)}/orgs",
                {
                    "username": source_org["name"],
                    "full_name": nullable_text(source_org["full_name"]),
                    "email": nullable_text(source_org["email"]),
                    "location": nullable_text(source_org["location"]),
                    "description": nullable_text(source_org["description"]),
                    "website": nullable_text(source_org["website"]),
                    "visibility": visibility_from_int(source_org["visibility"]),
                    "repo_admin_change_team_access": bool_value(
                        source_org["repo_admin_change_team_access"]
                    ),
                },
            )

            self.api.request(
                "PATCH",
                f"/api/v1/orgs/{path_join(source_org['name'])}",
                {
                    "full_name": nullable_text(source_org["full_name"]),
                    "email": nullable_text(source_org["email"]),
                    "location": nullable_text(source_org["location"]),
                    "description": nullable_text(source_org["description"]),
                    "website": nullable_text(source_org["website"]),
                    "visibility": visibility_from_int(source_org["visibility"]),
                    "repo_admin_change_team_access": bool_value(
                        source_org["repo_admin_change_team_access"]
                    ),
                },
            )

            for source_team in self.source_teams_by_org[source_org["id"]]:
                if source_team["name"] != "Owners":
                    self.api.request(
                        "POST",
                        f"/api/v1/orgs/{path_join(source_org['name'])}/teams",
                        {
                            "name": source_team["name"],
                            "permission": "read",
                            "includes_all_repositories": bool_value(
                                source_team["includes_all_repositories"]
                            ),
                            "can_create_org_repo": bool_value(source_team["can_create_org_repo"]),
                            "description": nullable_text(source_team["description"]),
                            "units_map": self.placeholder_units_map(),
                        },
                    )

                team_id = self.get_team_id(source_org["name"], source_team["name"])
                for membership in self.source_team_users.get(source_team["id"], []):
                    member = self.source.execute(
                        "select name from user where id = ?",
                        (membership["uid"],),
                    ).fetchone()
                    if member is None:
                        continue
                    try:
                        self.api.request(
                            "PUT",
                            f"/api/v1/teams/{team_id}/members/{path_join(member['name'])}",
                            expected=(204,),
                        )
                    except ForgejoAPIError as exc:
                        if exc.status != 422:
                            raise

        log("Creating repositories and mirrors via the Forgejo API")
        for repo in self.source_repositories:
            log(f"Creating repository shell for {repo['owner_name']}/{repo['name']}")
            mirror_row = self.source_mirrors.get(repo["id"])
            if mirror_row is not None:
                if not self.try_create_pull_mirror(repo, mirror_row):
                    self.create_normal_repository(repo)
            else:
                self.create_normal_repository(repo)

        self.write_state()

    # API-phase helpers.
    def get_team_id(self, org_name: str, team_name: str) -> int:
        assert self.api is not None
        search = self.api.request(
            "GET",
            f"/api/v1/orgs/{path_join(org_name)}/teams/search?q={urllib.parse.quote(team_name)}&limit=50",
        )
        for team in search.get("data", []):
            if team["name"] == team_name:
                return int(team["id"])
        raise ImportErrorWithContext(f"Could not find team {org_name}/{team_name} after API creation")

    @staticmethod
    def placeholder_units_map() -> dict[str, str]:
        # Forgejo 15.0 validates that a team payload includes unit permissions.
        # The offline finalize phase later replaces these placeholders with the
        # exact team_unit values from the source backup.
        return {
            "repo.actions": "read",
            "repo.code": "read",
            "repo.issues": "read",
            "repo.ext_issues": "none",
            "repo.wiki": "read",
            "repo.ext_wiki": "none",
            "repo.pulls": "read",
            "repo.releases": "read",
            "repo.projects": "read",
            "repo.packages": "read",
        }

    def create_normal_repository(self, repo: sqlite3.Row) -> None:
        assert self.api is not None
        payload = {
            "name": repo["name"],
            "private": bool_value(repo["is_private"]),
            "description": nullable_text(repo["description"]),
            "default_branch": nullable_text(repo["default_branch"]) or "main",
            "object_format_name": nullable_text(repo["object_format_name"]) or "sha1",
        }

        if repo["owner_name"] in self.source_org_names:
            path = f"/api/v1/orgs/{path_join(repo['owner_name'])}/repos"
        else:
            path = f"/api/v1/admin/users/{path_join(repo['owner_name'])}/repos"

        self.api.request("POST", path, payload)

    def try_create_pull_mirror(self, repo: sqlite3.Row, mirror_row: sqlite3.Row) -> bool:
        assert self.api is not None
        clone_addr = self.source_repo_origin_url(repo) or mirror_row["remote_address"]
        payload = {
            "service": "git",
            "repo_name": repo["name"],
            "repo_owner": repo["owner_name"],
            "clone_addr": clone_addr,
            "description": nullable_text(repo["description"]),
            "private": bool_value(repo["is_private"]),
            "mirror": True,
            "mirror_interval": format_duration_from_ns(mirror_row["interval"]),
            "issues": False,
            "labels": False,
            "milestones": False,
            "pull_requests": False,
            "releases": False,
            "wiki": False,
            "lfs": False,
        }

        try:
            self.api.request("POST", "/api/v1/repos/migrate", payload)
            return True
        except ForgejoAPIError as exc:
            if self.repo_exists(repo["owner_name"], repo["lower_name"]):
                self.warnings.append(
                    RepoWarning(
                        owner=repo["owner_name"],
                        name=repo["name"],
                        reason=f"Pull mirror API returned an error after creating the repository shell; keeping the created mirror shell: {exc.body}",
                    )
                )
                return True
            self.warnings.append(
                RepoWarning(
                    owner=repo["owner_name"],
                    name=repo["name"],
                    reason=f"Pull mirror activation failed, imported as a normal repository instead: {exc.body}",
                )
            )
            return False

    def discard_warning(self, owner: str, name: str) -> None:
        warning_key = repo_warning_key(owner, name)
        self.warnings = [
            warning for warning in self.warnings if repo_warning_key(warning.owner, warning.name) != warning_key
        ]

    def source_repo_origin_url(self, repo: sqlite3.Row) -> str:
        source_path = self.backup_root / "repos" / repo["owner_name"] / f"{repo['lower_name']}.git"
        if not source_path.exists():
            return ""

        try:
            return subprocess.check_output(
                ["git", "--git-dir", str(source_path), "config", "--get", "remote.origin.url"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except subprocess.CalledProcessError:
            return ""

    def repo_exists(self, owner: str, repo_name: str) -> bool:
        assert self.api is not None
        try:
            self.api.request("GET", f"/api/v1/repos/{path_join(owner, repo_name)}", expected=(200,))
            return True
        except ForgejoAPIError as exc:
            if exc.status == 404:
                return False
            raise

    def run_finalize_phase(self) -> None:
        if self.target is None:
            raise ImportErrorWithContext("Finalize mode requires a target Forgejo database")

        repo_id_map = self.build_repo_id_map()

        log("Finalizing user metadata offline")
        for source_user in self.source_users:
            target_user = self.find_target_user(source_user["name"])
            self.sync_user_emails(source_user, target_user["id"])
            if self.password_mode == "preserve":
                password_sql = """
                    passwd = ?,
                    passwd_hash_algo = ?,
                    must_change_password = ?,
                    rands = ?,
                    salt = ?,
                """
                password_params: tuple[Any, ...] = (
                    source_user["passwd"],
                    source_user["passwd_hash_algo"],
                    source_user["must_change_password"],
                    source_user["rands"],
                    source_user["salt"],
                )
            else:
                password_sql = ""
                password_params = ()
            self.target.execute(
                f"""
                update user
                set email = ?,
                    full_name = ?,
                    location = ?,
                    website = ?,
                    language = ?,
                    description = ?,
                    created_unix = ?,
                    updated_unix = ?,
                    last_login_unix = ?,
                    last_repo_visibility = ?,
                    max_repo_creation = ?,
                    is_active = ?,
                    is_admin = ?,
                    is_restricted = ?,
                    allow_git_hook = ?,
                    allow_import_local = ?,
                    allow_create_organization = ?,
                    prohibit_login = ?,
                    avatar = ?,
                    avatar_email = ?,
                    use_custom_avatar = ?,
                    visibility = ?,
                    diff_view_style = ?,
                    {password_sql}
                    keep_activity_private = ?
                where id = ?
                """,
                (
                    source_user["email"],
                    source_user["full_name"],
                    source_user["location"],
                    source_user["website"],
                    source_user["language"],
                    source_user["description"],
                    source_user["created_unix"],
                    source_user["updated_unix"],
                    source_user["last_login_unix"],
                    source_user["last_repo_visibility"],
                    source_user["max_repo_creation"],
                    source_user["is_active"],
                    source_user["is_admin"],
                    source_user["is_restricted"],
                    source_user["allow_git_hook"],
                    source_user["allow_import_local"],
                    source_user["allow_create_organization"],
                    source_user["prohibit_login"],
                    source_user["avatar"],
                    source_user["avatar_email"],
                    source_user["use_custom_avatar"],
                    source_user["visibility"],
                    source_user["diff_view_style"],
                    *password_params,
                    source_user["keep_activity_private"],
                    target_user["id"],
                ),
            )
            self.sync_avatar_file("avatars", source_user["avatar"], bool_value(source_user["use_custom_avatar"]))

        log("Finalizing organizations and teams offline")
        for source_org in self.source_orgs:
            target_org = self.find_target_user(source_org["name"])
            self.target.execute(
                """
                update user
                set full_name = ?,
                    email = ?,
                    location = ?,
                    website = ?,
                    description = ?,
                    created_unix = ?,
                    updated_unix = ?,
                    avatar = ?,
                    avatar_email = ?,
                    use_custom_avatar = ?,
                    visibility = ?,
                    repo_admin_change_team_access = ?
                where id = ?
                """,
                (
                    source_org["full_name"],
                    source_org["email"],
                    source_org["location"],
                    source_org["website"],
                    source_org["description"],
                    source_org["created_unix"],
                    source_org["updated_unix"],
                    source_org["avatar"],
                    source_org["avatar_email"],
                    source_org["use_custom_avatar"],
                    source_org["visibility"],
                    source_org["repo_admin_change_team_access"],
                    target_org["id"],
                ),
            )
            self.sync_avatar_file("avatars", source_org["avatar"], bool_value(source_org["use_custom_avatar"]))

            target_teams = {
                row["name"]: row
                for row in self.target.execute(
                    "select * from team where org_id = ? order by id",
                    (target_org["id"],),
                ).fetchall()
            }

            for source_team in self.source_teams_by_org[source_org["id"]]:
                target_team = target_teams[source_team["name"]]
                self.target.execute(
                    """
                    update team
                    set lower_name = ?,
                        name = ?,
                        description = ?,
                        authorize = ?,
                        num_repos = ?,
                        num_members = ?,
                        includes_all_repositories = ?,
                        can_create_org_repo = ?
                    where id = ?
                    """,
                    (
                        source_team["lower_name"],
                        source_team["name"],
                        source_team["description"],
                        source_team["authorize"],
                        source_team["num_repos"],
                        source_team["num_members"],
                        source_team["includes_all_repositories"],
                        source_team["can_create_org_repo"],
                        target_team["id"],
                    ),
                )
                self.target.execute("delete from team_unit where team_id = ?", (target_team["id"],))
                for team_unit in self.source_team_units.get(source_team["id"], []):
                    self.target.execute(
                        """
                        insert into team_unit (org_id, team_id, type, access_mode)
                        values (?, ?, ?, ?)
                        """,
                        (
                            target_org["id"],
                            target_team["id"],
                            team_unit["type"],
                            team_unit["access_mode"],
                        ),
                    )

            for org_member in self.source_org_users.get(source_org["id"], []):
                source_member = self.source.execute(
                    "select name from user where id = ?",
                    (org_member["uid"],),
                ).fetchone()
                if source_member is None:
                    continue
                target_member = self.find_target_user(source_member["name"])
                self.target.execute(
                    """
                    update org_user
                    set is_public = ?
                    where uid = ? and org_id = ?
                    """,
                    (org_member["is_public"], target_member["id"], target_org["id"]),
                )

        log("Replacing repository data and restoring metadata offline")
        for repo in self.source_repositories:
            target_repo = self.find_target_repo(repo["owner_name"], repo["lower_name"])
            self.copy_repository_data(repo)
            mirror_row = self.source_mirrors.get(repo["id"])
            # Forgejo rejects credentialed clone URLs during API migration, but
            # can recover pull-mirror credentials later from the copied repo
            # config if the mirror row exists in the database.
            fallback_pull_mirror = bool(mirror_row) and any(
                repo_warning_key(warning.owner, warning.name) == repo_warning_key(repo["owner_name"], repo["name"])
                for warning in self.warnings
            )
            target_fork_id = repo_id_map.get(normalize_int(repo["fork_id"]), 0) if repo["fork_id"] else 0
            target_template_id = repo_id_map.get(normalize_int(repo["template_id"]), 0) if repo["template_id"] else 0
            self.target.execute(
                """
                update repository
                set description = ?,
                    website = ?,
                    original_service_type = ?,
                    original_url = ?,
                    default_branch = ?,
                    wiki_branch = ?,
                    num_watches = ?,
                    num_stars = ?,
                    num_forks = ?,
                    num_milestones = ?,
                    num_closed_milestones = ?,
                    num_projects = ?,
                    num_closed_projects = ?,
                    is_private = ?,
                    is_empty = ?,
                    is_archived = ?,
                    is_mirror = ?,
                    status = ?,
                    is_fork = ?,
                    fork_id = ?,
                    is_template = ?,
                    template_id = ?,
                    size = ?,
                    git_size = ?,
                    lfs_size = ?,
                    is_fsck_enabled = ?,
                    close_issues_via_commit_in_any_branch = ?,
                    topics = ?,
                    object_format_name = ?,
                    trust_model = ?,
                    avatar = ?,
                    created_unix = ?,
                    updated_unix = ?,
                    archived_unix = ?
                where id = ?
                """,
                (
                    repo["description"],
                    repo["website"],
                    repo["original_service_type"],
                    repo["original_url"],
                    repo["default_branch"],
                    repo["default_wiki_branch"],
                    repo["num_watches"],
                    repo["num_stars"],
                    repo["num_forks"],
                    repo["num_milestones"],
                    repo["num_closed_milestones"],
                    repo["num_projects"],
                    repo["num_closed_projects"],
                    repo["is_private"],
                    repo["is_empty"],
                    repo["is_archived"],
                    1 if fallback_pull_mirror else repo["is_mirror"],
                    repo["status"],
                    repo["is_fork"],
                    target_fork_id,
                    repo["is_template"],
                    target_template_id,
                    repo["size"],
                    repo["git_size"],
                    repo["lfs_size"],
                    repo["is_fsck_enabled"],
                    repo["close_issues_via_commit_in_any_branch"],
                    nullable_text(repo["topics"]),
                    nullable_text(repo["object_format_name"]) or "sha1",
                    repo["trust_model"],
                    repo["avatar"],
                    repo["created_unix"],
                    repo["updated_unix"],
                    repo["archived_unix"],
                    target_repo["id"],
                ),
            )

            target_mirror = self.target.execute(
                "select repo_id from mirror where repo_id = ?",
                (target_repo["id"],),
            ).fetchone()
            if mirror_row is not None and target_mirror is not None:
                self.target.execute(
                    """
                    update mirror
                    set interval = ?,
                        enable_prune = ?,
                        updated_unix = ?,
                        next_update_unix = ?,
                        lfs_enabled = ?,
                        lfs_endpoint = ?
                    where repo_id = ?
                    """,
                    (
                        mirror_row["interval"],
                        mirror_row["enable_prune"],
                        mirror_row["updated_unix"],
                        mirror_row["next_update_unix"],
                        mirror_row["lfs_enabled"],
                        mirror_row["lfs_endpoint"],
                        target_repo["id"],
                    ),
                )
            elif mirror_row is not None and fallback_pull_mirror:
                self.target.execute(
                    """
                    insert into mirror (
                        repo_id,
                        interval,
                        enable_prune,
                        updated_unix,
                        next_update_unix,
                        lfs_enabled,
                        lfs_endpoint,
                        encrypted_remote_address
                    ) values (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        target_repo["id"],
                        mirror_row["interval"],
                        mirror_row["enable_prune"],
                        mirror_row["updated_unix"],
                        mirror_row["next_update_unix"],
                        mirror_row["lfs_enabled"],
                        mirror_row["lfs_endpoint"],
                    ),
                )
                self.discard_warning(repo["owner_name"], repo["name"])

            self.sync_avatar_file("repo-avatars", repo["avatar"], bool(repo["avatar"]))
            self.target.execute("delete from push_mirror where repo_id = ?", (target_repo["id"],))
            for push_mirror in self.source_push_mirrors.get(repo["id"], []):
                self.target.execute(
                    """
                    insert into push_mirror (
                        repo_id,
                        remote_name,
                        remote_address,
                        branch_filter,
                        public_key,
                        private_key,
                        sync_on_commit,
                        interval,
                        created_unix,
                        last_update,
                        last_error
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_repo["id"],
                        push_mirror["remote_name"],
                        push_mirror["remote_address"],
                        "",
                        None,
                        None,
                        push_mirror["sync_on_commit"],
                        push_mirror["interval"],
                        push_mirror["created_unix"],
                        push_mirror["last_update"],
                        push_mirror["last_error"],
                    ),
                )

        self.import_repo_units()
        self.import_project_and_social_data()
        self.import_packages()
        self.import_activity_actions()

        self.target.execute("PRAGMA journal_mode=DELETE")
        self.target.commit()
        self.write_state()
        self.write_report()

    # Finalize-phase lookup and remapping helpers.
    def find_target_user(self, username: str) -> sqlite3.Row:
        assert self.target is not None
        row = self.target.execute(
            "select * from user where lower_name = ?",
            (username.lower(),),
        ).fetchone()
        if row is None:
            raise ImportErrorWithContext(f"Target Forgejo user not found: {username}")
        return row

    def find_target_repo(self, owner: str, lower_name: str) -> sqlite3.Row:
        assert self.target is not None
        row = self.target.execute(
            """
            select repository.*
            from repository
            join user on user.id = repository.owner_id
            where user.lower_name = ? and repository.lower_name = ?
            """,
            (owner.lower(), lower_name.lower()),
        ).fetchone()
        if row is None:
            raise ImportErrorWithContext(f"Target Forgejo repo not found: {owner}/{lower_name}")
        return row

    def build_user_id_map(self) -> dict[int, int]:
        assert self.target is not None
        source_user_rows = {
            row["id"]: row["name"]
            for row in self.fetch_all("select id, name from user order by id")
        }
        target_user_rows = {
            row["name"]: row["id"]
            for row in self.target.execute("select id, name from user order by id").fetchall()
        }
        return {
            source_id: target_user_rows[name]
            for source_id, name in source_user_rows.items()
            if name in target_user_rows
        }

    def build_repo_id_map(self) -> dict[int, int]:
        assert self.target is not None
        source_repo_rows = {
            row["id"]: (row["owner_name"], row["lower_name"])
            for row in self.source_repositories
        }
        target_repo_rows = {
            (row["owner_name"], row["lower_name"]): row["id"]
            for row in self.target.execute(
                """
                select repository.id, owner.name as owner_name, repository.lower_name
                from repository
                join user owner on owner.id = repository.owner_id
                order by repository.id
                """,
            ).fetchall()
        }
        return {
            source_id: target_repo_rows[key]
            for source_id, key in source_repo_rows.items()
            if key in target_repo_rows
        }

    def reset_sqlite_sequences(self, table_names: tuple[str, ...]) -> None:
        assert self.target is not None
        for table_name in table_names:
            max_id = self.target.execute(f"select coalesce(max(id), 0) from {table_name}").fetchone()[0]
            self.target.execute("delete from sqlite_sequence where name = ?", (table_name,))
            self.target.execute(
                "insert into sqlite_sequence (name, seq) values (?, ?)",
                (table_name, max_id),
            )

    # Finalize-phase filesystem and table import helpers.
    def copy_attachment_files(self) -> None:
        source_attachments_dir = self.backup_root / "data" / "attachments"
        target_attachments_dir = self.forgejo_root / "data" / "attachments"
        if target_attachments_dir.exists():
            shutil.rmtree(target_attachments_dir)
        if source_attachments_dir.exists():
            shutil.copytree(source_attachments_dir, target_attachments_dir, symlinks=True)
        else:
            target_attachments_dir.mkdir(parents=True, exist_ok=True)

    def import_repo_units(self) -> None:
        assert self.target is not None

        repo_id_map = self.build_repo_id_map()
        self.target.execute("delete from repo_unit")
        for repo_unit in self.source_repo_units:
            target_repo_id = repo_id_map.get(repo_unit["repo_id"])
            if target_repo_id is None:
                continue
            self.target.execute(
                """
                insert into repo_unit (
                    id,
                    repo_id,
                    type,
                    config,
                    created_unix,
                    default_permissions
                )
                values (?, ?, ?, ?, ?, ?)
                """,
                (
                    repo_unit["id"],
                    target_repo_id,
                    repo_unit["type"],
                    repo_unit["config"],
                    repo_unit["created_unix"],
                    repo_unit["everyone_access_mode"],
                ),
            )

        self.reset_sqlite_sequences(("repo_unit",))

    def import_project_and_social_data(self) -> None:
        assert self.target is not None

        log("Replacing issues, notifications, follows, releases, stars, watches, and collaborators offline")
        self.copy_attachment_files()

        user_id_map = self.build_user_id_map()
        repo_id_map = self.build_repo_id_map()
        label_ids = {row["id"] for row in self.source_labels}
        milestone_ids = {row["id"] for row in self.source_milestones}
        issue_ids = {row["id"] for row in self.source_issues}
        comment_ids = {row["id"] for row in self.source_comments}
        review_ids = {row["id"] for row in self.source_reviews}
        pull_request_ids = {row["id"] for row in self.source_pull_requests}
        release_ids = {row["id"] for row in self.source_releases}

        for table_name in (
            "issue_content_history",
            "reaction",
            "review_state",
            "review",
            "pull_request",
            "comment",
            "issue_assignees",
            "issue_user",
            "issue_watch",
            "issue_label",
            "attachment",
            "upload",
            "release",
            "notification",
            "follow",
            "star",
            "watch",
            "collaboration",
            "issue",
            "milestone",
            "label",
        ):
            self.target.execute(f"delete from {table_name}")

        for label in self.source_labels:
            self.target.execute(
                """
                insert into label (
                    id,
                    repo_id,
                    org_id,
                    name,
                    exclusive,
                    description,
                    color,
                    num_issues,
                    num_closed_issues,
                    created_unix,
                    updated_unix,
                    archived_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    label["id"],
                    repo_id_map.get(label["repo_id"], 0) if label["repo_id"] else 0,
                    user_id_map.get(label["org_id"], 0) if label["org_id"] else 0,
                    label["name"],
                    label["exclusive"],
                    label["description"],
                    label["color"],
                    label["num_issues"],
                    label["num_closed_issues"],
                    label["created_unix"],
                    label["updated_unix"],
                    label["archived_unix"],
                ),
            )

        for milestone in self.source_milestones:
            self.target.execute(
                """
                insert into milestone (
                    id,
                    repo_id,
                    name,
                    content,
                    is_closed,
                    num_issues,
                    num_closed_issues,
                    completeness,
                    created_unix,
                    updated_unix,
                    deadline_unix,
                    closed_date_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    milestone["id"],
                    repo_id_map.get(milestone["repo_id"], 0),
                    milestone["name"],
                    milestone["content"],
                    milestone["is_closed"],
                    milestone["num_issues"],
                    milestone["num_closed_issues"],
                    milestone["completeness"],
                    milestone["created_unix"],
                    milestone["updated_unix"],
                    milestone["deadline_unix"],
                    milestone["closed_date_unix"],
                ),
            )

        for issue in self.source_issues:
            self.target.execute(
                """
                insert into issue (
                    id,
                    repo_id,
                    "index",
                    poster_id,
                    original_author,
                    original_author_id,
                    name,
                    content,
                    content_version,
                    milestone_id,
                    priority,
                    is_closed,
                    is_pull,
                    num_comments,
                    ref,
                    pin_order,
                    deadline_unix,
                    created,
                    created_unix,
                    updated_unix,
                    closed_unix,
                    is_locked
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    issue["id"],
                    repo_id_map.get(issue["repo_id"], 0),
                    issue["index"],
                    user_id_map.get(issue["poster_id"], 0),
                    issue["original_author"],
                    user_id_map.get(issue["original_author_id"], 0) if issue["original_author_id"] else 0,
                    issue["name"],
                    issue["content"],
                    issue["content_version"],
                    issue["milestone_id"] if issue["milestone_id"] in milestone_ids else 0,
                    issue["priority"],
                    issue["is_closed"],
                    issue["is_pull"],
                    issue["num_comments"],
                    issue["ref"],
                    0,
                    issue["deadline_unix"],
                    issue["created_unix"],
                    issue["created_unix"],
                    issue["updated_unix"],
                    issue["closed_unix"],
                    issue["is_locked"],
                ),
            )

        for issue_label in self.source_issue_labels:
            if issue_label["issue_id"] not in issue_ids or issue_label["label_id"] not in label_ids:
                continue
            self.target.execute(
                "insert into issue_label (id, issue_id, label_id) values (?, ?, ?)",
                (issue_label["id"], issue_label["issue_id"], issue_label["label_id"]),
            )

        for assignee in self.source_issue_assignees:
            if assignee["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                "insert into issue_assignees (id, assignee_id, issue_id) values (?, ?, ?)",
                (
                    assignee["id"],
                    user_id_map.get(assignee["assignee_id"], 0),
                    assignee["issue_id"],
                ),
            )

        for issue_user in self.source_issue_users:
            if issue_user["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into issue_user (id, uid, issue_id, is_read, is_mentioned)
                values (?, ?, ?, ?, ?)
                """,
                (
                    issue_user["id"],
                    user_id_map.get(issue_user["uid"], 0),
                    issue_user["issue_id"],
                    issue_user["is_read"],
                    issue_user["is_mentioned"],
                ),
            )

        for issue_watch in self.source_issue_watches:
            if issue_watch["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into issue_watch (id, user_id, issue_id, is_watching, created_unix, updated_unix)
                values (?, ?, ?, ?, ?, ?)
                """,
                (
                    issue_watch["id"],
                    user_id_map.get(issue_watch["user_id"], 0),
                    issue_watch["issue_id"],
                    issue_watch["is_watching"],
                    issue_watch["created_unix"],
                    issue_watch["updated_unix"],
                ),
            )

        for comment in self.source_comments:
            if comment["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into comment (
                    id,
                    type,
                    poster_id,
                    original_author,
                    original_author_id,
                    issue_id,
                    label_id,
                    old_project_id,
                    project_id,
                    old_milestone_id,
                    milestone_id,
                    time_id,
                    assignee_id,
                    removed_assignee,
                    assignee_team_id,
                    resolve_doer_id,
                    old_title,
                    new_title,
                    old_ref,
                    new_ref,
                    dependent_issue_id,
                    commit_id,
                    line,
                    tree_path,
                    content,
                    content_version,
                    patch,
                    created_unix,
                    updated_unix,
                    commit_sha,
                    review_id,
                    invalidated,
                    ref_repo_id,
                    ref_issue_id,
                    ref_comment_id,
                    ref_action,
                    ref_is_pull
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    comment["id"],
                    comment["type"],
                    user_id_map.get(comment["poster_id"], 0),
                    comment["original_author"],
                    user_id_map.get(comment["original_author_id"], 0) if comment["original_author_id"] else 0,
                    comment["issue_id"],
                    comment["label_id"] if comment["label_id"] in label_ids else 0,
                    comment["old_project_id"],
                    comment["project_id"],
                    comment["old_milestone_id"],
                    comment["milestone_id"] if comment["milestone_id"] in milestone_ids else 0,
                    comment["time_id"],
                    user_id_map.get(comment["assignee_id"], 0) if comment["assignee_id"] else 0,
                    comment["removed_assignee"],
                    comment["assignee_team_id"],
                    user_id_map.get(comment["resolve_doer_id"], 0) if comment["resolve_doer_id"] else 0,
                    comment["old_title"],
                    comment["new_title"],
                    comment["old_ref"],
                    comment["new_ref"],
                    comment["dependent_issue_id"] if comment["dependent_issue_id"] in issue_ids else 0,
                    comment["commit_id"],
                    comment["line"],
                    comment["tree_path"],
                    comment["content"],
                    comment["content_version"],
                    comment["patch"],
                    comment["created_unix"],
                    comment["updated_unix"],
                    comment["commit_sha"],
                    comment["review_id"] if comment["review_id"] in review_ids else 0,
                    comment["invalidated"],
                    repo_id_map.get(comment["ref_repo_id"], 0) if comment["ref_repo_id"] else 0,
                    comment["ref_issue_id"] if comment["ref_issue_id"] in issue_ids else 0,
                    comment["ref_comment_id"] if comment["ref_comment_id"] in comment_ids else 0,
                    comment["ref_action"],
                    comment["ref_is_pull"],
                ),
            )

        for pull_request in self.source_pull_requests:
            if pull_request["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into pull_request (
                    id,
                    type,
                    status,
                    conflicted_files,
                    commits_ahead,
                    commits_behind,
                    changed_protected_files,
                    issue_id,
                    "index",
                    head_repo_id,
                    base_repo_id,
                    head_branch,
                    base_branch,
                    merge_base,
                    allow_maintainer_edit,
                    has_merged,
                    merged_commit_id,
                    merger_id,
                    merged_unix,
                    flow
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pull_request["id"],
                    pull_request["type"],
                    pull_request["status"],
                    pull_request["conflicted_files"],
                    pull_request["commits_ahead"],
                    pull_request["commits_behind"],
                    pull_request["changed_protected_files"],
                    pull_request["issue_id"],
                    pull_request["index"],
                    repo_id_map.get(pull_request["head_repo_id"], 0) if pull_request["head_repo_id"] else 0,
                    repo_id_map.get(pull_request["base_repo_id"], 0) if pull_request["base_repo_id"] else 0,
                    pull_request["head_branch"],
                    pull_request["base_branch"],
                    pull_request["merge_base"],
                    pull_request["allow_maintainer_edit"],
                    pull_request["has_merged"],
                    pull_request["merged_commit_id"],
                    user_id_map.get(pull_request["merger_id"], 0) if pull_request["merger_id"] else 0,
                    pull_request["merged_unix"],
                    pull_request["flow"],
                ),
            )

        for review in self.source_reviews:
            if review["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into review (
                    id,
                    type,
                    reviewer_id,
                    reviewer_team_id,
                    original_author,
                    original_author_id,
                    issue_id,
                    content,
                    official,
                    commit_id,
                    stale,
                    dismissed,
                    created_unix,
                    updated_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review["id"],
                    review["type"],
                    user_id_map.get(review["reviewer_id"], 0) if review["reviewer_id"] else 0,
                    review["reviewer_team_id"],
                    review["original_author"],
                    user_id_map.get(review["original_author_id"], 0) if review["original_author_id"] else 0,
                    review["issue_id"],
                    review["content"],
                    review["official"],
                    review["commit_id"],
                    review["stale"],
                    review["dismissed"],
                    review["created_unix"],
                    review["updated_unix"],
                ),
            )

        for review_state in self.source_review_states:
            if review_state["pull_id"] not in pull_request_ids:
                continue
            self.target.execute(
                """
                insert into review_state (id, user_id, pull_id, commit_sha, updated_files, updated_unix)
                values (?, ?, ?, ?, ?, ?)
                """,
                (
                    review_state["id"],
                    user_id_map.get(review_state["user_id"], 0),
                    review_state["pull_id"],
                    review_state["commit_sha"],
                    review_state["updated_files"],
                    review_state["updated_unix"],
                ),
            )

        for history_row in self.source_issue_content_history:
            if history_row["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into issue_content_history (
                    id,
                    poster_id,
                    issue_id,
                    comment_id,
                    edited_unix,
                    content_text,
                    is_first_created,
                    is_deleted
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    history_row["id"],
                    user_id_map.get(history_row["poster_id"], 0) if history_row["poster_id"] else 0,
                    history_row["issue_id"],
                    history_row["comment_id"] if history_row["comment_id"] in comment_ids else 0,
                    history_row["edited_unix"],
                    history_row["content_text"],
                    history_row["is_first_created"],
                    history_row["is_deleted"],
                ),
            )

        for reaction in self.source_reactions:
            if reaction["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into reaction (
                    id,
                    type,
                    issue_id,
                    comment_id,
                    user_id,
                    original_author_id,
                    original_author,
                    created_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reaction["id"],
                    reaction["type"],
                    reaction["issue_id"],
                    reaction["comment_id"] if reaction["comment_id"] in comment_ids else 0,
                    user_id_map.get(reaction["user_id"], 0),
                    user_id_map.get(reaction["original_author_id"], 0)
                    if reaction["original_author_id"]
                    else 0,
                    reaction["original_author"],
                    reaction["created_unix"],
                ),
            )

        for release in self.source_releases:
            self.target.execute(
                """
                insert into release (
                    id,
                    repo_id,
                    publisher_id,
                    tag_name,
                    original_author,
                    original_author_id,
                    lower_tag_name,
                    target,
                    title,
                    sha1,
                    hide_archive_links,
                    num_commits,
                    note,
                    is_draft,
                    is_prerelease,
                    is_tag,
                    created_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    release["id"],
                    repo_id_map.get(release["repo_id"], 0),
                    user_id_map.get(release["publisher_id"], 0) if release["publisher_id"] else 0,
                    release["tag_name"],
                    release["original_author"],
                    user_id_map.get(release["original_author_id"], 0)
                    if release["original_author_id"]
                    else 0,
                    release["lower_tag_name"],
                    release["target"],
                    release["title"],
                    release["sha1"],
                    0,
                    release["num_commits"],
                    release["note"],
                    release["is_draft"],
                    release["is_prerelease"],
                    release["is_tag"],
                    release["created_unix"],
                ),
            )

        for upload in self.source_uploads:
            self.target.execute(
                "insert into upload (id, uuid, name) values (?, ?, ?)",
                (upload["id"], upload["uuid"], upload["name"]),
            )

        for attachment in self.source_attachments:
            self.target.execute(
                """
                insert into attachment (
                    id,
                    uuid,
                    uploader_id,
                    repo_id,
                    issue_id,
                    release_id,
                    comment_id,
                    name,
                    download_count,
                    size,
                    created_unix,
                    external_url
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attachment["id"],
                    attachment["uuid"],
                    user_id_map.get(attachment["uploader_id"], 0) if attachment["uploader_id"] else 0,
                    repo_id_map.get(attachment["repo_id"], 0) if attachment["repo_id"] else 0,
                    attachment["issue_id"] if attachment["issue_id"] in issue_ids else 0,
                    attachment["release_id"] if attachment["release_id"] in release_ids else 0,
                    attachment["comment_id"] if attachment["comment_id"] in comment_ids else 0,
                    attachment["name"],
                    attachment["download_count"],
                    attachment["size"],
                    attachment["created_unix"],
                    None,
                ),
            )

        for notification in self.source_notifications:
            if notification["issue_id"] not in issue_ids:
                continue
            self.target.execute(
                """
                insert into notification (
                    id,
                    user_id,
                    repo_id,
                    status,
                    source,
                    issue_id,
                    comment_id,
                    created_unix,
                    updated_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    notification["id"],
                    user_id_map.get(notification["user_id"], 0),
                    repo_id_map.get(notification["repo_id"], 0),
                    notification["status"],
                    notification["source"],
                    notification["issue_id"],
                    notification["comment_id"] if notification["comment_id"] in comment_ids else 0,
                    notification["created_unix"],
                    notification["updated_unix"],
                ),
            )

        for star in self.source_stars:
            self.target.execute(
                "insert into star (id, uid, repo_id, created_unix) values (?, ?, ?, ?)",
                (
                    star["id"],
                    user_id_map.get(star["uid"], 0),
                    repo_id_map.get(star["repo_id"], 0),
                    star["created_unix"],
                ),
            )

        for follow in self.source_follows:
            self.target.execute(
                "insert into follow (id, user_id, follow_id, created_unix) values (?, ?, ?, ?)",
                (
                    follow["id"],
                    user_id_map.get(follow["user_id"], 0),
                    user_id_map.get(follow["follow_id"], 0),
                    follow["created_unix"],
                ),
            )

        for watch in self.source_watches:
            self.target.execute(
                """
                insert into watch (id, user_id, repo_id, mode, created_unix, updated_unix)
                values (?, ?, ?, ?, ?, ?)
                """,
                (
                    watch["id"],
                    user_id_map.get(watch["user_id"], 0),
                    repo_id_map.get(watch["repo_id"], 0),
                    watch["mode"],
                    watch["created_unix"],
                    watch["updated_unix"],
                ),
            )

        for collaboration in self.source_collaborations:
            self.target.execute(
                """
                insert into collaboration (id, repo_id, user_id, mode, created_unix, updated_unix)
                values (?, ?, ?, ?, ?, ?)
                """,
                (
                    collaboration["id"],
                    repo_id_map.get(collaboration["repo_id"], 0),
                    user_id_map.get(collaboration["user_id"], 0),
                    collaboration["mode"],
                    collaboration["created_unix"],
                    collaboration["updated_unix"],
                ),
            )

        self.reset_sqlite_sequences(
            (
                "attachment",
                "collaboration",
                "comment",
                "follow",
                "issue",
                "issue_assignees",
                "issue_content_history",
                "issue_label",
                "issue_user",
                "issue_watch",
                "label",
                "milestone",
                "notification",
                "pull_request",
                "reaction",
                "release",
                "review",
                "review_state",
                "star",
                "upload",
                "watch",
            )
        )

    # Per-user and per-repository file copy helpers.
    def sync_user_emails(self, source_user: sqlite3.Row, target_user_id: int) -> None:
        assert self.target is not None
        desired_rows = list(self.source_emails.get(source_user["id"], []))
        if not any(row["email"].lower() == source_user["email"].lower() for row in desired_rows):
            desired_rows.insert(
                0,
                {
                    "email": source_user["email"],
                    "is_activated": 1,
                    "is_primary": 1,
                },
            )

        self.target.execute("delete from email_address where uid = ?", (target_user_id,))
        for row in desired_rows:
            self.target.execute(
                """
                insert into email_address (uid, email, lower_email, is_activated, is_primary)
                values (?, ?, ?, ?, ?)
                """,
                (
                    target_user_id,
                    row["email"],
                    row["email"].lower(),
                    int(row["is_activated"] if row["is_activated"] is not None else 1),
                    int(row["is_primary"] if row["is_primary"] is not None else 0),
                ),
            )

    def sync_avatar_file(self, folder: str, name: Any, should_copy: bool) -> None:
        avatar_name = nullable_text(name)
        if not avatar_name or not should_copy:
            return

        source_path = self.backup_root / "data" / folder / avatar_name
        target_path = self.forgejo_root / "data" / folder / avatar_name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.exists():
            shutil.copy2(source_path, target_path)
            os.chmod(target_path, 0o644)

    def copy_repository_data(self, repo: sqlite3.Row) -> None:
        source_path = self.backup_root / "repos" / repo["owner_name"] / f"{repo['lower_name']}.git"
        target_path = (
            self.forgejo_root
            / "data"
            / "forgejo-repositories"
            / repo["owner_name"]
            / f"{repo['lower_name']}.git"
        )

        if not source_path.exists():
            raise ImportErrorWithContext(f"Missing source repository: {source_path}")

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            shutil.rmtree(target_path)
        shutil.copytree(source_path, target_path, symlinks=True)

    # Package compatibility helpers.
    def compute_retained_package_rows(
        self,
    ) -> tuple[list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row]]:
        package_ids = {row["id"] for row in self.source_packages}
        referenced_digests = {
            normalize_text(row["value"])
            for row in self.source_package_properties
            if normalize_int(row["ref_type"]) == 0 and normalize_text(row["name"]) == "container.manifest.reference"
        }

        kept_versions = [
            row
            for row in self.source_package_versions
            if not normalize_text(row["version"]).startswith("sha256:")
            or normalize_text(row["version"]) in referenced_digests
        ]
        kept_version_ids = {row["id"] for row in kept_versions}

        kept_files = [row for row in self.source_package_files if row["version_id"] in kept_version_ids]
        kept_file_ids = {row["id"] for row in kept_files}
        kept_blob_ids = {row["blob_id"] for row in kept_files}
        kept_blobs = [row for row in self.source_package_blobs if row["id"] in kept_blob_ids]

        kept_properties = [
            row
            for row in self.source_package_properties
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

        self.pruned_package_version_count = len(self.source_package_versions) - len(kept_versions)
        self.pruned_package_file_count = len(self.source_package_files) - len(kept_files)
        self.pruned_package_blob_count = len(self.source_package_blobs) - len(kept_blobs)

        return kept_versions, kept_files, kept_blobs, kept_properties

    def import_packages(self) -> None:
        assert self.target is not None

        log("Replacing package registry data and restoring package blobs offline")

        source_packages_dir = self.backup_root / "data" / "packages"
        target_packages_dir = self.forgejo_root / "data" / "packages"
        if target_packages_dir.exists():
            shutil.rmtree(target_packages_dir)
        if source_packages_dir.exists():
            shutil.copytree(source_packages_dir, target_packages_dir, symlinks=True)
        else:
            target_packages_dir.mkdir(parents=True, exist_ok=True)

        retained_blob_hashes = {normalize_text(row["hash_sha256"]) for row in self.kept_source_package_blobs}
        for blob in self.source_package_blobs:
            blob_hash = normalize_text(blob["hash_sha256"])
            if blob_hash in retained_blob_hashes:
                continue
            blob_path = target_packages_dir / blob_hash[:2] / blob_hash[2:4] / blob_hash
            if blob_path.exists():
                blob_path.unlink()

        self.target.execute("delete from package_property")
        self.target.execute("delete from package_file")
        self.target.execute("delete from package_version")
        self.target.execute("delete from package_blob")
        self.target.execute("delete from package_cleanup_rule")
        self.target.execute("delete from package")

        user_id_map = self.build_user_id_map()
        repo_id_map = self.build_repo_id_map()

        for package in self.source_packages:
            self.target.execute(
                """
                insert into package (
                    id,
                    owner_id,
                    repo_id,
                    type,
                    name,
                    lower_name,
                    semver_compatible,
                    is_internal
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    package["id"],
                    user_id_map[package["owner_id"]],
                    repo_id_map.get(package["repo_id"], 0) if package["repo_id"] else 0,
                    package["type"],
                    package["name"],
                    package["lower_name"],
                    package["semver_compatible"],
                    package["is_internal"],
                ),
            )

        for blob in self.kept_source_package_blobs:
            self.target.execute(
                """
                insert into package_blob (
                    id,
                    size,
                    hash_md5,
                    hash_sha1,
                    hash_sha256,
                    hash_sha512,
                    hash_blake2b,
                    created_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    blob["id"],
                    blob["size"],
                    blob["hash_md5"],
                    blob["hash_sha1"],
                    blob["hash_sha256"],
                    blob["hash_sha512"],
                    None,
                    blob["created_unix"],
                ),
            )

        for version in self.kept_source_package_versions:
            self.target.execute(
                """
                insert into package_version (
                    id,
                    package_id,
                    creator_id,
                    version,
                    lower_version,
                    created_unix,
                    is_internal,
                    metadata_json,
                    download_count
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version["id"],
                    version["package_id"],
                    user_id_map.get(version["creator_id"], 0),
                    version["version"],
                    version["lower_version"],
                    version["created_unix"],
                    version["is_internal"],
                    version["metadata_json"],
                    version["download_count"],
                ),
            )

        for package_file in self.kept_source_package_files:
            self.target.execute(
                """
                insert into package_file (
                    id,
                    version_id,
                    blob_id,
                    name,
                    lower_name,
                    composite_key,
                    is_lead,
                    created_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    package_file["id"],
                    package_file["version_id"],
                    package_file["blob_id"],
                    package_file["name"],
                    package_file["lower_name"],
                    package_file["composite_key"],
                    package_file["is_lead"],
                    package_file["created_unix"],
                ),
            )

        for package_property in self.kept_source_package_properties:
            self.target.execute(
                """
                insert into package_property (id, ref_type, ref_id, name, value)
                values (?, ?, ?, ?, ?)
                """,
                (
                    package_property["id"],
                    package_property["ref_type"],
                    package_property["ref_id"],
                    package_property["name"],
                    package_property["value"],
                ),
            )

        for cleanup_rule in self.source_package_cleanup_rules:
            self.target.execute(
                """
                insert into package_cleanup_rule (
                    id,
                    enabled,
                    owner_id,
                    type,
                    keep_count,
                    keep_pattern,
                    remove_days,
                    remove_pattern,
                    match_full_name,
                    created_unix,
                    updated_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cleanup_rule["id"],
                    cleanup_rule["enabled"],
                    user_id_map[cleanup_rule["owner_id"]],
                    cleanup_rule["type"],
                    cleanup_rule["keep_count"],
                    cleanup_rule["keep_pattern"],
                    cleanup_rule["remove_days"],
                    cleanup_rule["remove_pattern"],
                    cleanup_rule["match_full_name"],
                    cleanup_rule["created_unix"],
                    cleanup_rule["updated_unix"],
                ),
            )

        self.reset_sqlite_sequences(
            (
                "package",
                "package_blob",
                "package_cleanup_rule",
                "package_file",
                "package_property",
                "package_version",
            )
        )

    # Activity replay helpers.
    def import_activity_actions(self) -> None:
        assert self.target is not None

        log("Replacing import-generated activity feed entries with source activity history")
        self.target.execute("delete from action")

        source_user_map = self.build_user_id_map()
        source_repo_map = self.build_repo_id_map()
        imported_comment_ids = {row["id"] for row in self.target.execute("select id from comment order by id").fetchall()}

        max_action_id = 0
        imported = 0
        skipped = 0

        for action in self.fetch_all("select * from action order by id"):
            op_type = int(action["op_type"] or 0)
            comment_id = int(action["comment_id"] or 0)
            is_deleted = int(action["is_deleted"] or 0)

            if op_type not in SUPPORTED_ACTIVITY_OP_TYPES or is_deleted != 0:
                skipped += 1
                continue

            user_id = int(action["user_id"] or 0)
            act_user_id = int(action["act_user_id"] or 0)
            repo_id = int(action["repo_id"] or 0)

            if user_id and user_id not in source_user_map:
                skipped += 1
                continue
            if act_user_id and act_user_id not in source_user_map:
                skipped += 1
                continue
            if repo_id and repo_id not in source_repo_map:
                skipped += 1
                continue
            if comment_id and comment_id not in imported_comment_ids:
                skipped += 1
                continue

            self.target.execute(
                """
                insert into action (
                    id,
                    user_id,
                    op_type,
                    act_user_id,
                    repo_id,
                    comment_id,
                    ref_name,
                    is_private,
                    content,
                    created_unix
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    action["id"],
                    source_user_map.get(user_id, 0),
                    op_type,
                    source_user_map.get(act_user_id, 0),
                    source_repo_map.get(repo_id, 0),
                    comment_id,
                    nullable_text(action["ref_name"]),
                    int(action["is_private"] or 0),
                    nullable_text(action["content"]),
                    action["created_unix"],
                ),
            )
            imported += 1
            max_action_id = max(max_action_id, int(action["id"]))

        self.target.execute("delete from sqlite_sequence where name = 'action'")
        self.target.execute(
            "insert into sqlite_sequence (name, seq) values ('action', ?)",
            (max_action_id,),
        )

        self.imported_activity_count = imported
        self.skipped_activity_count = skipped

    # Cross-phase state and final reporting.
    def load_state(self) -> None:
        if not self.state_path.exists():
            self.warnings = []
            return
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.warnings = [RepoWarning(**item) for item in payload.get("warnings", [])]

    def write_state(self) -> None:
        payload = {"warnings": [warning.__dict__ for warning in self.warnings]}
        self.state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def write_report(self) -> None:
        assert self.target is not None
        expected_pull_mirrors = len(self.source_mirrors) - len(self.warnings)
        target_counts = {
            "users": self.target.execute("select count(*) from user where type = 0").fetchone()[0],
            "organizations": self.target.execute("select count(*) from user where type = 1").fetchone()[0],
            "teams": self.target.execute("select count(*) from team").fetchone()[0],
            "team_memberships": self.target.execute("select count(*) from team_user").fetchone()[0],
            "repositories": self.target.execute("select count(*) from repository").fetchone()[0],
            "repo_units": self.target.execute("select count(*) from repo_unit").fetchone()[0],
            "issues": self.target.execute("select count(*) from issue where is_pull = 0").fetchone()[0],
            "pull_requests": self.target.execute("select count(*) from pull_request").fetchone()[0],
            "comments": self.target.execute("select count(*) from comment").fetchone()[0],
            "releases": self.target.execute("select count(*) from release").fetchone()[0],
            "attachments": self.target.execute("select count(*) from attachment").fetchone()[0],
            "notifications": self.target.execute("select count(*) from notification").fetchone()[0],
            "stars": self.target.execute("select count(*) from star").fetchone()[0],
            "watches": self.target.execute("select count(*) from watch").fetchone()[0],
            "issue_watches": self.target.execute("select count(*) from issue_watch").fetchone()[0],
            "follows": self.target.execute("select count(*) from follow").fetchone()[0],
            "collaborators": self.target.execute("select count(*) from collaboration").fetchone()[0],
            "pull_mirrors": self.target.execute("select count(*) from mirror").fetchone()[0],
            "push_mirrors": self.target.execute("select count(*) from push_mirror").fetchone()[0],
            "ssh_keys": self.target.execute("select count(*) from public_key").fetchone()[0],
            "activity_entries": self.target.execute("select count(*) from action").fetchone()[0],
            "packages": self.target.execute("select count(*) from package").fetchone()[0],
            "package_versions": self.target.execute("select count(*) from package_version").fetchone()[0],
            "package_files": self.target.execute("select count(*) from package_file").fetchone()[0],
            "package_blobs": self.target.execute("select count(*) from package_blob").fetchone()[0],
        }
        source_activity_entries = self.source.execute("select count(*) from action").fetchone()[0]

        lines = [
            "# Migration Report",
            "",
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
            "",
            "## Source Snapshot",
            "",
            f"- Users: {len(self.source_users)}",
            f"- Organizations: {len(self.source_orgs)}",
            f"- Teams: {len(self.source_teams)}",
            f"- Team memberships: {sum(len(v) for v in self.source_team_users.values())}",
            f"- Repositories: {len(self.source_repositories)}",
            f"- Repo units: {len(self.source_repo_units)}",
            f"- Issues: {sum(1 for row in self.source_issues if not bool_value(row['is_pull']))}",
            f"- Pull requests: {len(self.source_pull_requests)}",
            f"- Comments: {len(self.source_comments)}",
            f"- Releases: {len(self.source_releases)}",
            f"- Attachments: {len(self.source_attachments)}",
            f"- Notifications: {len(self.source_notifications)}",
            f"- Stars: {len(self.source_stars)}",
            f"- Watches: {len(self.source_watches)}",
            f"- Issue watches: {len(self.source_issue_watches)}",
            f"- Follows: {len(self.source_follows)}",
            f"- Collaborators: {len(self.source_collaborations)}",
            f"- Pull mirrors: {len(self.source_mirrors)}",
            f"- Push mirrors: {sum(len(v) for v in self.source_push_mirrors.values())}",
            f"- SSH keys: {sum(len(v) for v in self.source_keys.values())}",
            f"- Activity entries: {source_activity_entries}",
            f"- Packages: {len(self.source_packages)}",
            f"- Package versions: {len(self.source_package_versions)}",
            f"- Package files: {len(self.source_package_files)}",
            f"- Package blobs: {len(self.source_package_blobs)}",
            "",
            "## Target Counts",
            "",
            f"- Users: {target_counts['users']}",
            f"- Organizations: {target_counts['organizations']}",
            f"- Teams: {target_counts['teams']}",
            f"- Team memberships: {target_counts['team_memberships']}",
            f"- Repositories: {target_counts['repositories']}",
            f"- Repo units: {target_counts['repo_units']}",
            f"- Issues: {target_counts['issues']}",
            f"- Pull requests: {target_counts['pull_requests']}",
            f"- Comments: {target_counts['comments']}",
            f"- Releases: {target_counts['releases']}",
            f"- Attachments: {target_counts['attachments']}",
            f"- Notifications: {target_counts['notifications']}",
            f"- Stars: {target_counts['stars']}",
            f"- Watches: {target_counts['watches']}",
            f"- Issue watches: {target_counts['issue_watches']}",
            f"- Follows: {target_counts['follows']}",
            f"- Collaborators: {target_counts['collaborators']}",
            f"- Pull mirrors: {target_counts['pull_mirrors']} (expected after fallbacks: {expected_pull_mirrors})",
            f"- Push mirrors: {target_counts['push_mirrors']}",
            f"- SSH keys: {target_counts['ssh_keys']}",
            f"- Activity entries: {target_counts['activity_entries']}",
            f"- Packages: {target_counts['packages']}",
            f"- Package versions: {target_counts['package_versions']}",
            f"- Package files: {target_counts['package_files']}",
            f"- Package blobs: {target_counts['package_blobs']}",
            "",
            "## Package Compatibility Notes",
            "",
            f"- Forgejo 15 automatically prunes unreferenced `sha256:*` OCI manifests on startup.",
            f"- Expected retained package versions after compatibility cleanup: {len(self.kept_source_package_versions)}",
            f"- Expected retained package files after compatibility cleanup: {len(self.kept_source_package_files)}",
            f"- Expected retained package blobs after compatibility cleanup: {len(self.kept_source_package_blobs)}",
            f"- Pruned package versions during import: {self.pruned_package_version_count}",
            f"- Pruned package files during import: {self.pruned_package_file_count}",
            f"- Pruned package blobs during import: {self.pruned_package_blob_count}",
            "",
            "## Activity Feed Notes",
            "",
            f"- Imported activity rows: {self.imported_activity_count}",
            f"- Skipped activity rows after dependency remapping: {self.skipped_activity_count}",
            "",
            "## Password Notes",
            "",
            (
                "- Password mode: preserved original Gitea password hashes and salts."
                if self.password_mode == "preserve"
                else "- Password mode: randomized temporary passwords for testing."
            ),
            "",
            "## Warnings",
            "",
        ]

        if self.warnings:
            for warning in self.warnings:
                lines.append(f"- `{warning.owner}/{warning.name}`: {warning.reason}")
        else:
            lines.append("- None")
        lines.append("")

        self.report_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import the minimal Gitea backup shape into Forgejo 15.0")
    parser.add_argument("--mode", choices=("api", "finalize"), required=True)
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--forgejo-db", required=True, type=Path)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--forgejo-root", required=True, type=Path)
    parser.add_argument("--admin-username", required=True)
    parser.add_argument("--password-mode", choices=("preserve", "randomize"), default="preserve")
    parser.add_argument("--report-path", required=True, type=Path)
    parser.add_argument("--state-path", required=True, type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--token-file", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = None
    if args.token_file is not None:
        token = args.token_file.read_text(encoding="utf-8").strip()

    try:
        importer = Importer(
            mode=args.mode,
            source_db=args.source_db,
            forgejo_db=args.forgejo_db,
            backup_root=args.backup_root,
            forgejo_root=args.forgejo_root,
            admin_username=args.admin_username,
            password_mode=args.password_mode,
            report_path=args.report_path,
            state_path=args.state_path,
            base_url=args.base_url,
            token=token,
        )
        importer.run()
    except (ImportErrorWithContext, ForgejoAPIError, sqlite3.DatabaseError, OSError) as exc:
        print(f"Import failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
