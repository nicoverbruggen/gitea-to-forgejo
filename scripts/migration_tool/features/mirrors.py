from __future__ import annotations


def finalize(importer) -> None:
    for repo in importer.source_repositories:
        target_repo = importer.find_target_repo(repo["owner_name"], repo["lower_name"])
        mirror_row = importer.source_mirrors.get(repo["id"])
        fallback_pull_mirror = bool(mirror_row) and any(
            warning.owner.lower() == repo["owner_name"].lower() and warning.name.lower() == repo["name"].lower()
            for warning in importer.warnings
        )
        target_mirror = importer.target.execute("select repo_id from mirror where repo_id = ?", (target_repo["id"],)).fetchone()
        if mirror_row is not None and target_mirror is not None:
            importer.target.execute(
                "update mirror set interval = ?, enable_prune = ?, updated_unix = ?, next_update_unix = ?, lfs_enabled = ?, lfs_endpoint = ? where repo_id = ?",
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
            importer.target.execute(
                "insert into mirror (repo_id, interval, enable_prune, updated_unix, next_update_unix, lfs_enabled, lfs_endpoint, encrypted_remote_address) values (?, ?, ?, ?, ?, ?, ?, NULL)",
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
            importer.discard_warning(repo["owner_name"], repo["name"])
        importer.target.execute("delete from push_mirror where repo_id = ?", (target_repo["id"],))
        for push_mirror in importer.source_push_mirrors.get(repo["id"], []):
            importer.target.execute(
                "insert into push_mirror (repo_id, remote_name, remote_address, branch_filter, public_key, private_key, sync_on_commit, interval, created_unix, last_update, last_error) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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


def validate(validator) -> None:
    validator.validate_pull_mirrors()
    validator.validate_push_mirrors()
