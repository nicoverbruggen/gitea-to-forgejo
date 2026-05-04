from __future__ import annotations

import argparse
import os
from collections import defaultdict
from pathlib import Path
from typing import Any
import sqlite3
import sys
import time

FORGEJO_VERSION = os.environ.get("FORGEJO_VERSION", "15.0.1")

from .features import activity, issues, mirrors, organizations, packages, releases, repositories, repo_units, social, users
from .helpers.api import ForgejoAPI
from .helpers.common import (
    ForgejoAPIError,
    ImportErrorWithContext,
    RepoWarning,
    bool_value,
    log,
    normalize_int,
    normalize_text,
    nullable_text,
    path_join,
    repo_warning_key,
)
from .helpers.files import copy_avatar_if_present, replace_optional_tree, replace_tree
from .helpers.git import git_origin_url
from .helpers.package_retention import compute_retained_package_rows
from .helpers.state import load_warnings, write_warnings


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
        self.source_mirrors = {row["repo_id"]: row for row in self.fetch_all("select * from mirror order by repo_id")}
        self.source_push_mirrors = self.fetch_grouped("select * from push_mirror order by repo_id, id", "repo_id")
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
        ) = compute_retained_package_rows(
            self.source_packages,
            self.source_package_versions,
            self.source_package_files,
            self.source_package_blobs,
            self.source_package_properties,
        )
        self.pruned_package_version_count = len(self.source_package_versions) - len(self.kept_source_package_versions)
        self.pruned_package_file_count = len(self.source_package_files) - len(self.kept_source_package_files)
        self.pruned_package_blob_count = len(self.source_package_blobs) - len(self.kept_source_package_blobs)

    def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return list(self.source.execute(query, params).fetchall())

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
        users.import_api(self)
        organizations.import_api(self)
        repositories.import_api(self)
        self.write_state()

    def run_finalize_phase(self) -> None:
        if self.target is None:
            raise ImportErrorWithContext("Finalize mode requires a target Forgejo database")
        users.finalize(self)
        organizations.finalize(self)
        repositories.finalize(self)
        mirrors.finalize(self)
        repo_units.finalize(self)
        issues.finalize(self)
        releases.finalize(self)
        social.finalize(self)
        packages.finalize(self)
        activity.finalize(self)
        self.target.execute("PRAGMA journal_mode=DELETE")
        self.target.commit()
        self.write_state()
        self.write_report()

    def get_team_id(self, org_name: str, team_name: str) -> int:
        assert self.api is not None
        search = self.api.request("GET", f"/api/v1/orgs/{path_join(org_name)}/teams/search?q={team_name}&limit=50")
        for team in search.get("data", []):
            if team["name"] == team_name:
                return int(team["id"])
        raise ImportErrorWithContext(f"Could not find team {org_name}/{team_name} after API creation")

    @staticmethod
    def placeholder_units_map() -> dict[str, str]:
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

    def find_target_user(self, username: str) -> sqlite3.Row:
        assert self.target is not None
        row = self.target.execute("select * from user where lower_name = ?", (username.lower(),)).fetchone()
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
        source_user_rows = {row["id"]: row["name"] for row in self.fetch_all("select id, name from user order by id")}
        target_user_rows = {row["name"]: row["id"] for row in self.target.execute("select id, name from user order by id").fetchall()}
        return {source_id: target_user_rows[name] for source_id, name in source_user_rows.items() if name in target_user_rows}

    def build_repo_id_map(self) -> dict[int, int]:
        assert self.target is not None
        source_repo_rows = {row["id"]: (row["owner_name"], row["lower_name"]) for row in self.source_repositories}
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
        return {source_id: target_repo_rows[key] for source_id, key in source_repo_rows.items() if key in target_repo_rows}

    def reset_sqlite_sequences(self, table_names: tuple[str, ...]) -> None:
        assert self.target is not None
        for table_name in table_names:
            max_id = self.target.execute(f"select coalesce(max(id), 0) from {table_name}").fetchone()[0]
            self.target.execute("delete from sqlite_sequence where name = ?", (table_name,))
            self.target.execute("insert into sqlite_sequence (name, seq) values (?, ?)", (table_name, max_id))

    def repo_exists(self, owner: str, repo_name: str) -> bool:
        assert self.api is not None
        try:
            self.api.request("GET", f"/api/v1/repos/{path_join(owner, repo_name)}", expected=(200,))
            return True
        except ForgejoAPIError as exc:
            if exc.status == 404:
                return False
            raise

    def source_repo_origin_url(self, repo: sqlite3.Row) -> str:
        source_path = self.backup_root / "repos" / repo["owner_name"] / f"{repo['lower_name']}.git"
        return git_origin_url(source_path) if source_path.exists() else ""

    def discard_warning(self, owner: str, name: str) -> None:
        warning_key = repo_warning_key(owner, name)
        self.warnings = [warning for warning in self.warnings if repo_warning_key(warning.owner, warning.name) != warning_key]

    def sync_user_emails(self, source_user: sqlite3.Row, target_user_id: int) -> None:
        assert self.target is not None
        desired_rows = list(self.source_emails.get(source_user["id"], []))
        if not any(row["email"].lower() == source_user["email"].lower() for row in desired_rows):
            desired_rows.insert(0, {"email": source_user["email"], "is_activated": 1, "is_primary": 1})
        self.target.execute("delete from email_address where uid = ?", (target_user_id,))
        for row in desired_rows:
            self.target.execute(
                "insert into email_address (uid, email, lower_email, is_activated, is_primary) values (?, ?, ?, ?, ?)",
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
        copy_avatar_if_present(self.backup_root / "data" / folder / avatar_name, self.forgejo_root / "data" / folder / avatar_name)

    def copy_repository_data(self, repo: sqlite3.Row) -> None:
        source_path = self.backup_root / "repos" / repo["owner_name"] / f"{repo['lower_name']}.git"
        if not source_path.exists():
            raise ImportErrorWithContext(f"Missing source repository: {source_path}")
        replace_tree(source_path, self.forgejo_root / "data" / "forgejo-repositories" / repo["owner_name"] / f"{repo['lower_name']}.git")

    def copy_attachment_files(self) -> None:
        replace_optional_tree(self.backup_root / "data" / "attachments", self.forgejo_root / "data" / "attachments")

    def copy_package_files(self) -> None:
        replace_optional_tree(self.backup_root / "data" / "packages", self.forgejo_root / "data" / "packages")

    def load_state(self) -> None:
        self.warnings = load_warnings(self.state_path)

    def write_state(self) -> None:
        write_warnings(self.state_path, self.warnings)

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
            "- Forgejo automatically prunes unreferenced `sha256:*` OCI manifests on startup.",
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
            "- Password mode: preserved original Gitea password hashes and salts." if self.password_mode == "preserve" else "- Password mode: randomized temporary passwords for testing.",
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
    parser = argparse.ArgumentParser(description=f"Import the minimal Gitea backup shape into Forgejo {FORGEJO_VERSION}")
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
    token = args.token_file.read_text(encoding="utf-8").strip() if args.token_file is not None else None
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
