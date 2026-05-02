from __future__ import annotations


def finalize(importer) -> None:
    assert importer.target is not None
    user_id_map = importer.build_user_id_map()
    repo_id_map = importer.build_repo_id_map()
    issue_ids = {row["id"] for row in importer.source_issues}
    comment_ids = {row["id"] for row in importer.source_comments}
    release_ids = {row["id"] for row in importer.source_releases}
    for table_name in ("attachment", "notification", "follow", "star", "watch", "collaboration"):
        importer.target.execute(f"delete from {table_name}")
    for attachment in importer.source_attachments:
        importer.target.execute(
            "insert into attachment (id, uuid, uploader_id, repo_id, issue_id, release_id, comment_id, name, download_count, size, created_unix, external_url) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                attachment["id"], attachment["uuid"], user_id_map.get(attachment["uploader_id"], 0) if attachment["uploader_id"] else 0, repo_id_map.get(attachment["repo_id"], 0) if attachment["repo_id"] else 0,
                attachment["issue_id"] if attachment["issue_id"] in issue_ids else 0, attachment["release_id"] if attachment["release_id"] in release_ids else 0, attachment["comment_id"] if attachment["comment_id"] in comment_ids else 0,
                attachment["name"], attachment["download_count"], attachment["size"], attachment["created_unix"], None,
            ),
        )
    for notification in importer.source_notifications:
        if notification["issue_id"] in issue_ids:
            importer.target.execute("insert into notification (id, user_id, repo_id, status, source, issue_id, comment_id, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?)", (notification["id"], user_id_map.get(notification["user_id"], 0), repo_id_map.get(notification["repo_id"], 0), notification["status"], notification["source"], notification["issue_id"], notification["comment_id"] if notification["comment_id"] in comment_ids else 0, notification["created_unix"], notification["updated_unix"]))
    for star in importer.source_stars:
        importer.target.execute("insert into star (id, uid, repo_id, created_unix) values (?, ?, ?, ?)", (star["id"], user_id_map.get(star["uid"], 0), repo_id_map.get(star["repo_id"], 0), star["created_unix"]))
    for follow in importer.source_follows:
        importer.target.execute("insert into follow (id, user_id, follow_id, created_unix) values (?, ?, ?, ?)", (follow["id"], user_id_map.get(follow["user_id"], 0), user_id_map.get(follow["follow_id"], 0), follow["created_unix"]))
    for watch in importer.source_watches:
        importer.target.execute("insert into watch (id, user_id, repo_id, mode, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?)", (watch["id"], user_id_map.get(watch["user_id"], 0), repo_id_map.get(watch["repo_id"], 0), watch["mode"], watch["created_unix"], watch["updated_unix"]))
    for collaboration in importer.source_collaborations:
        importer.target.execute("insert into collaboration (id, repo_id, user_id, mode, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?)", (collaboration["id"], repo_id_map.get(collaboration["repo_id"], 0), user_id_map.get(collaboration["user_id"], 0), collaboration["mode"], collaboration["created_unix"], collaboration["updated_unix"]))
    importer.reset_sqlite_sequences(("attachment", "collaboration", "follow", "notification", "star", "upload", "watch"))


def validate(validator) -> None:
    validator.validate_social()
