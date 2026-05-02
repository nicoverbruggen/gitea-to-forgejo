from __future__ import annotations


def finalize(importer) -> None:
    assert importer.target is not None
    user_id_map = importer.build_user_id_map()
    repo_id_map = importer.build_repo_id_map()
    for table_name in ("attachment", "upload", "release"):
        importer.target.execute(f"delete from {table_name}")
    for release in importer.source_releases:
        importer.target.execute(
            "insert into release (id, repo_id, publisher_id, tag_name, original_author, original_author_id, lower_tag_name, target, title, sha1, hide_archive_links, num_commits, note, is_draft, is_prerelease, is_tag, created_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                release["id"], repo_id_map.get(release["repo_id"], 0), user_id_map.get(release["publisher_id"], 0) if release["publisher_id"] else 0,
                release["tag_name"], release["original_author"], user_id_map.get(release["original_author_id"], 0) if release["original_author_id"] else 0,
                release["lower_tag_name"], release["target"], release["title"], release["sha1"], 0, release["num_commits"], release["note"], release["is_draft"], release["is_prerelease"], release["is_tag"], release["created_unix"],
            ),
        )
    for upload in importer.source_uploads:
        importer.target.execute("insert into upload (id, uuid, name) values (?, ?, ?)", (upload["id"], upload["uuid"], upload["name"]))


def validate(validator) -> None:
    validator.validate_releases()
