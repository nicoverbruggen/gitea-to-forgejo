#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
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


def log(message: str) -> None:
    print(f"[import] {message}")


def visibility_from_int(value: Any) -> str:
    mapping = {0: "public", 1: "limited", 2: "private"}
    return mapping.get(int(value or 0), "public")


def bool_value(value: Any) -> bool:
    return bool(int(value or 0))


def nullable_text(value: Any) -> str:
    return "" if value is None else str(value)


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


SUPPORTED_ACTIVITY_OP_TYPES = {1, 2, 5, 8, 9, 16, 17, 18, 19, 20}


@dataclass
class RepoWarning:
    owner: str
    name: str
    reason: str


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
        report_path: Path,
        state_path: Path,
        base_url: str | None = None,
        token: str | None = None,
    ) -> None:
        self.mode = mode
        self.source_db = source_db
        self.forgejo_db = forgejo_db
        self.backup_root = backup_root
        self.forgejo_root = forgejo_root
        self.report_path = report_path
        self.state_path = state_path
        self.api = ForgejoAPI(base_url, token) if base_url and token else None
        self.warnings: list[RepoWarning] = []
        self.imported_activity_count = 0
        self.skipped_activity_count = 0

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
        self.source_mirrors = {
            row["repo_id"]: row for row in self.fetch_all("select * from mirror order by repo_id")
        }
        self.source_push_mirrors = self.fetch_grouped(
            "select * from push_mirror order by repo_id, id",
            "repo_id",
        )

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
                f"/api/v1/admin/users/{path_join('nico')}/orgs",
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
        payload = {
            "service": "git",
            "repo_name": repo["name"],
            "repo_owner": repo["owner_name"],
            "clone_addr": mirror_row["remote_address"],
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

        log("Finalizing user metadata offline")
        for source_user in self.source_users:
            target_user = self.find_target_user(source_user["name"])
            self.sync_user_emails(source_user, target_user["id"])
            self.target.execute(
                """
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
                    theme = ?,
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
                    source_user["theme"],
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
            self.target.execute(
                """
                update repository
                set description = ?,
                    website = ?,
                    original_service_type = ?,
                    original_url = ?,
                    default_branch = ?,
                    wiki_branch = ?,
                    is_private = ?,
                    is_empty = ?,
                    is_archived = ?,
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
                    repo["is_private"],
                    repo["is_empty"],
                    repo["is_archived"],
                    repo["status"],
                    repo["is_fork"],
                    repo["fork_id"],
                    repo["is_template"],
                    repo["template_id"],
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

            mirror_row = self.source_mirrors.get(repo["id"])
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

        self.import_activity_actions()

        self.target.commit()
        self.write_report()

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

    def import_activity_actions(self) -> None:
        assert self.target is not None

        log("Replacing import-generated activity feed entries with source repository activity")
        self.target.execute("delete from action")

        source_user_map = {
            row["id"]: self.find_target_user(row["name"])["id"]
            for row in self.source.execute("select id, name from user order by id")
        }
        source_repo_map = {
            row["id"]: self.find_target_repo(row["owner_name"], row["lower_name"])["id"]
            for row in self.source_repositories
        }

        max_action_id = 0
        imported = 0
        skipped = 0

        for action in self.fetch_all("select * from action order by id"):
            op_type = int(action["op_type"] or 0)
            comment_id = int(action["comment_id"] or 0)
            is_deleted = int(action["is_deleted"] or 0)

            if op_type not in SUPPORTED_ACTIVITY_OP_TYPES or comment_id != 0 or is_deleted != 0:
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
                    0,
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
            "pull_mirrors": self.target.execute("select count(*) from mirror").fetchone()[0],
            "push_mirrors": self.target.execute("select count(*) from push_mirror").fetchone()[0],
            "ssh_keys": self.target.execute("select count(*) from public_key").fetchone()[0],
            "activity_entries": self.target.execute("select count(*) from action").fetchone()[0],
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
            f"- Pull mirrors: {len(self.source_mirrors)}",
            f"- Push mirrors: {sum(len(v) for v in self.source_push_mirrors.values())}",
            f"- SSH keys: {sum(len(v) for v in self.source_keys.values())}",
            f"- Activity entries: {source_activity_entries}",
            "",
            "## Target Counts",
            "",
            f"- Users: {target_counts['users']}",
            f"- Organizations: {target_counts['organizations']}",
            f"- Teams: {target_counts['teams']}",
            f"- Team memberships: {target_counts['team_memberships']}",
            f"- Repositories: {target_counts['repositories']}",
            f"- Pull mirrors: {target_counts['pull_mirrors']} (expected after fallbacks: {expected_pull_mirrors})",
            f"- Push mirrors: {target_counts['push_mirrors']}",
            f"- SSH keys: {target_counts['ssh_keys']}",
            f"- Activity entries: {target_counts['activity_entries']}",
            "",
            "## Activity Feed Notes",
            "",
            f"- Imported repository-safe activity rows: {self.imported_activity_count}",
            f"- Skipped activity rows that depend on unmigrated issues, comments, pull requests, or releases: {self.skipped_activity_count}",
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
