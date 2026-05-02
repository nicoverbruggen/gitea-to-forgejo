from __future__ import annotations

from ..helpers.common import ForgejoAPIError, RepoWarning, bool_value, format_duration_from_ns, nullable_text, repo_warning_key


def import_api(importer) -> None:
    for repo in importer.source_repositories:
        mirror_row = importer.source_mirrors.get(repo["id"])
        if mirror_row is not None:
            if not try_create_pull_mirror(importer, repo, mirror_row):
                create_normal_repository(importer, repo)
        else:
            create_normal_repository(importer, repo)


def create_normal_repository(importer, repo) -> None:
    assert importer.api is not None
    payload = {
        "name": repo["name"],
        "private": bool_value(repo["is_private"]),
        "description": nullable_text(repo["description"]),
        "default_branch": nullable_text(repo["default_branch"]) or "main",
        "object_format_name": nullable_text(repo["object_format_name"]) or "sha1",
    }
    if repo["owner_name"] in importer.source_org_names:
        path = f"/api/v1/orgs/{repo['owner_name']}/repos"
    else:
        path = f"/api/v1/admin/users/{repo['owner_name']}/repos"
    importer.api.request("POST", path, payload)


def try_create_pull_mirror(importer, repo, mirror_row) -> bool:
    assert importer.api is not None
    clone_addr = importer.source_repo_origin_url(repo) or mirror_row["remote_address"]
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
        importer.api.request("POST", "/api/v1/repos/migrate", payload)
        return True
    except ForgejoAPIError as exc:
        if importer.repo_exists(repo["owner_name"], repo["lower_name"]):
            importer.warnings.append(
                RepoWarning(
                    owner=repo["owner_name"],
                    name=repo["name"],
                    reason=f"Pull mirror API returned an error after creating the repository shell; keeping the created mirror shell: {exc.body}",
                )
            )
            return True
        importer.warnings.append(
            RepoWarning(
                owner=repo["owner_name"],
                name=repo["name"],
                reason=f"Pull mirror activation failed, imported as a normal repository instead: {exc.body}",
            )
        )
        return False


def finalize(importer) -> None:
    repo_id_map = importer.build_repo_id_map()
    for repo in importer.source_repositories:
        target_repo = importer.find_target_repo(repo["owner_name"], repo["lower_name"])
        importer.copy_repository_data(repo)
        mirror_row = importer.source_mirrors.get(repo["id"])
        fallback_pull_mirror = bool(mirror_row) and any(
            repo_warning_key(warning.owner, warning.name) == repo_warning_key(repo["owner_name"], repo["name"])
            for warning in importer.warnings
        )
        target_fork_id = repo_id_map.get(repo["fork_id"], 0) if repo["fork_id"] else 0
        target_template_id = repo_id_map.get(repo["template_id"], 0) if repo["template_id"] else 0
        importer.target.execute(
            """
            update repository
            set description = ?, website = ?, original_service_type = ?, original_url = ?, default_branch = ?,
                wiki_branch = ?, num_watches = ?, num_stars = ?, num_forks = ?, num_milestones = ?,
                num_closed_milestones = ?, num_projects = ?, num_closed_projects = ?, is_private = ?,
                is_empty = ?, is_archived = ?, is_mirror = ?, status = ?, is_fork = ?, fork_id = ?,
                is_template = ?, template_id = ?, size = ?, git_size = ?, lfs_size = ?, is_fsck_enabled = ?,
                close_issues_via_commit_in_any_branch = ?, topics = ?, object_format_name = ?, trust_model = ?,
                avatar = ?, created_unix = ?, updated_unix = ?, archived_unix = ?
            where id = ?
            """,
            (
                repo["description"], repo["website"], repo["original_service_type"], repo["original_url"],
                repo["default_branch"], repo["default_wiki_branch"], repo["num_watches"], repo["num_stars"],
                repo["num_forks"], repo["num_milestones"], repo["num_closed_milestones"], repo["num_projects"],
                repo["num_closed_projects"], repo["is_private"], repo["is_empty"], repo["is_archived"],
                1 if fallback_pull_mirror else repo["is_mirror"], repo["status"], repo["is_fork"], target_fork_id,
                repo["is_template"], target_template_id, repo["size"], repo["git_size"], repo["lfs_size"],
                repo["is_fsck_enabled"], repo["close_issues_via_commit_in_any_branch"], nullable_text(repo["topics"]),
                nullable_text(repo["object_format_name"]) or "sha1", repo["trust_model"], repo["avatar"],
                repo["created_unix"], repo["updated_unix"], repo["archived_unix"], target_repo["id"],
            ),
        )
        importer.sync_avatar_file("repo-avatars", repo["avatar"], bool(repo["avatar"]))


def validate(validator) -> None:
    validator.validate_repositories()
    validator.validate_git_repositories()
