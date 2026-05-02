from __future__ import annotations

from ..helpers.common import SUPPORTED_ACTIVITY_OP_TYPES, log, nullable_text


def finalize(importer) -> None:
    assert importer.target is not None
    log("Replacing import-generated activity feed entries with source activity history")
    importer.target.execute("delete from action")
    source_user_map = importer.build_user_id_map()
    source_repo_map = importer.build_repo_id_map()
    imported_comment_ids = {row["id"] for row in importer.target.execute("select id from comment order by id").fetchall()}
    max_action_id = 0
    imported = 0
    skipped = 0
    for action_row in importer.fetch_all("select * from action order by id"):
        op_type = int(action_row["op_type"] or 0)
        comment_id = int(action_row["comment_id"] or 0)
        is_deleted = int(action_row["is_deleted"] or 0)
        if op_type not in SUPPORTED_ACTIVITY_OP_TYPES or is_deleted != 0:
            skipped += 1
            continue
        user_id = int(action_row["user_id"] or 0)
        act_user_id = int(action_row["act_user_id"] or 0)
        repo_id = int(action_row["repo_id"] or 0)
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
        importer.target.execute(
            "insert into action (id, user_id, op_type, act_user_id, repo_id, comment_id, ref_name, is_private, content, created_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                action_row["id"], source_user_map.get(user_id, 0), op_type, source_user_map.get(act_user_id, 0), source_repo_map.get(repo_id, 0), comment_id,
                nullable_text(action_row["ref_name"]), int(action_row["is_private"] or 0), nullable_text(action_row["content"]), action_row["created_unix"],
            ),
        )
        imported += 1
        max_action_id = max(max_action_id, int(action_row["id"]))
    importer.target.execute("delete from sqlite_sequence where name = 'action'")
    importer.target.execute("insert into sqlite_sequence (name, seq) values ('action', ?)", (max_action_id,))
    importer.imported_activity_count = imported
    importer.skipped_activity_count = skipped


def validate(validator) -> None:
    validator.validate_activity_feed()
