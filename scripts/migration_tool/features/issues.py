from __future__ import annotations

from ..helpers.common import log


def finalize(importer) -> None:
    assert importer.target is not None
    log("Replacing issues and issue-linked tables offline")
    importer.copy_attachment_files()
    user_id_map = importer.build_user_id_map()
    repo_id_map = importer.build_repo_id_map()
    label_ids = {row["id"] for row in importer.source_labels}
    milestone_ids = {row["id"] for row in importer.source_milestones}
    issue_ids = {row["id"] for row in importer.source_issues}
    comment_ids = {row["id"] for row in importer.source_comments}
    review_ids = {row["id"] for row in importer.source_reviews}
    pull_request_ids = {row["id"] for row in importer.source_pull_requests}
    for table_name in (
        "issue_content_history", "reaction", "review_state", "review", "pull_request", "comment",
        "issue_assignees", "issue_user", "issue_watch", "issue_label", "issue", "milestone", "label",
    ):
        importer.target.execute(f"delete from {table_name}")
    for label in importer.source_labels:
        importer.target.execute(
            "insert into label (id, repo_id, org_id, name, exclusive, description, color, num_issues, num_closed_issues, created_unix, updated_unix, archived_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                label["id"], repo_id_map.get(label["repo_id"], 0) if label["repo_id"] else 0,
                user_id_map.get(label["org_id"], 0) if label["org_id"] else 0, label["name"], label["exclusive"],
                label["description"], label["color"], label["num_issues"], label["num_closed_issues"],
                label["created_unix"], label["updated_unix"], label["archived_unix"],
            ),
        )
    for milestone in importer.source_milestones:
        importer.target.execute(
            "insert into milestone (id, repo_id, name, content, is_closed, num_issues, num_closed_issues, completeness, created_unix, updated_unix, deadline_unix, closed_date_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                milestone["id"], repo_id_map.get(milestone["repo_id"], 0), milestone["name"], milestone["content"],
                milestone["is_closed"], milestone["num_issues"], milestone["num_closed_issues"], milestone["completeness"],
                milestone["created_unix"], milestone["updated_unix"], milestone["deadline_unix"], milestone["closed_date_unix"],
            ),
        )
    for issue in importer.source_issues:
        importer.target.execute(
            "insert into issue (id, repo_id, \"index\", poster_id, original_author, original_author_id, name, content, content_version, milestone_id, priority, is_closed, is_pull, num_comments, ref, pin_order, deadline_unix, created, created_unix, updated_unix, closed_unix, is_locked) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                issue["id"], repo_id_map.get(issue["repo_id"], 0), issue["index"], user_id_map.get(issue["poster_id"], 0),
                issue["original_author"], user_id_map.get(issue["original_author_id"], 0) if issue["original_author_id"] else 0,
                issue["name"], issue["content"], issue["content_version"], issue["milestone_id"] if issue["milestone_id"] in milestone_ids else 0,
                issue["priority"], issue["is_closed"], issue["is_pull"], issue["num_comments"], issue["ref"], 0,
                issue["deadline_unix"], issue["created_unix"], issue["created_unix"], issue["updated_unix"], issue["closed_unix"], issue["is_locked"],
            ),
        )
    for issue_label in importer.source_issue_labels:
        if issue_label["issue_id"] in issue_ids and issue_label["label_id"] in label_ids:
            importer.target.execute("insert into issue_label (id, issue_id, label_id) values (?, ?, ?)", (issue_label["id"], issue_label["issue_id"], issue_label["label_id"]))
    for assignee in importer.source_issue_assignees:
        if assignee["issue_id"] in issue_ids:
            importer.target.execute("insert into issue_assignees (id, assignee_id, issue_id) values (?, ?, ?)", (assignee["id"], user_id_map.get(assignee["assignee_id"], 0), assignee["issue_id"]))
    for issue_user in importer.source_issue_users:
        if issue_user["issue_id"] in issue_ids:
            importer.target.execute("insert into issue_user (id, uid, issue_id, is_read, is_mentioned) values (?, ?, ?, ?, ?)", (issue_user["id"], user_id_map.get(issue_user["uid"], 0), issue_user["issue_id"], issue_user["is_read"], issue_user["is_mentioned"]))
    for issue_watch in importer.source_issue_watches:
        if issue_watch["issue_id"] in issue_ids:
            importer.target.execute("insert into issue_watch (id, user_id, issue_id, is_watching, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?)", (issue_watch["id"], user_id_map.get(issue_watch["user_id"], 0), issue_watch["issue_id"], issue_watch["is_watching"], issue_watch["created_unix"], issue_watch["updated_unix"]))
    for comment in importer.source_comments:
        if comment["issue_id"] not in issue_ids:
            continue
        importer.target.execute(
            "insert into comment (id, type, poster_id, original_author, original_author_id, issue_id, label_id, old_project_id, project_id, old_milestone_id, milestone_id, time_id, assignee_id, removed_assignee, assignee_team_id, resolve_doer_id, old_title, new_title, old_ref, new_ref, dependent_issue_id, commit_id, line, tree_path, content, content_version, patch, created_unix, updated_unix, commit_sha, review_id, invalidated, ref_repo_id, ref_issue_id, ref_comment_id, ref_action, ref_is_pull) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                comment["id"], comment["type"], user_id_map.get(comment["poster_id"], 0), comment["original_author"], user_id_map.get(comment["original_author_id"], 0) if comment["original_author_id"] else 0,
                comment["issue_id"], comment["label_id"] if comment["label_id"] in label_ids else 0, comment["old_project_id"], comment["project_id"], comment["old_milestone_id"],
                comment["milestone_id"] if comment["milestone_id"] in milestone_ids else 0, comment["time_id"], user_id_map.get(comment["assignee_id"], 0) if comment["assignee_id"] else 0,
                comment["removed_assignee"], comment["assignee_team_id"], user_id_map.get(comment["resolve_doer_id"], 0) if comment["resolve_doer_id"] else 0,
                comment["old_title"], comment["new_title"], comment["old_ref"], comment["new_ref"], comment["dependent_issue_id"] if comment["dependent_issue_id"] in issue_ids else 0,
                comment["commit_id"], comment["line"], comment["tree_path"], comment["content"], comment["content_version"], comment["patch"], comment["created_unix"], comment["updated_unix"],
                comment["commit_sha"], comment["review_id"] if comment["review_id"] in review_ids else 0, comment["invalidated"], repo_id_map.get(comment["ref_repo_id"], 0) if comment["ref_repo_id"] else 0,
                comment["ref_issue_id"] if comment["ref_issue_id"] in issue_ids else 0, comment["ref_comment_id"] if comment["ref_comment_id"] in comment_ids else 0, comment["ref_action"], comment["ref_is_pull"],
            ),
        )
    for pull_request in importer.source_pull_requests:
        if pull_request["issue_id"] in issue_ids:
            importer.target.execute(
                "insert into pull_request (id, type, status, conflicted_files, commits_ahead, commits_behind, changed_protected_files, issue_id, \"index\", head_repo_id, base_repo_id, head_branch, base_branch, merge_base, allow_maintainer_edit, has_merged, merged_commit_id, merger_id, merged_unix, flow) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    pull_request["id"], pull_request["type"], pull_request["status"], pull_request["conflicted_files"], pull_request["commits_ahead"], pull_request["commits_behind"], pull_request["changed_protected_files"],
                    pull_request["issue_id"], pull_request["index"], repo_id_map.get(pull_request["head_repo_id"], 0) if pull_request["head_repo_id"] else 0, repo_id_map.get(pull_request["base_repo_id"], 0) if pull_request["base_repo_id"] else 0,
                    pull_request["head_branch"], pull_request["base_branch"], pull_request["merge_base"], pull_request["allow_maintainer_edit"], pull_request["has_merged"], pull_request["merged_commit_id"],
                    user_id_map.get(pull_request["merger_id"], 0) if pull_request["merger_id"] else 0, pull_request["merged_unix"], pull_request["flow"],
                ),
            )
    for review in importer.source_reviews:
        if review["issue_id"] in issue_ids:
            importer.target.execute("insert into review (id, type, reviewer_id, reviewer_team_id, original_author, original_author_id, issue_id, content, official, commit_id, stale, dismissed, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (review["id"], review["type"], user_id_map.get(review["reviewer_id"], 0) if review["reviewer_id"] else 0, review["reviewer_team_id"], review["original_author"], user_id_map.get(review["original_author_id"], 0) if review["original_author_id"] else 0, review["issue_id"], review["content"], review["official"], review["commit_id"], review["stale"], review["dismissed"], review["created_unix"], review["updated_unix"]))
    for review_state in importer.source_review_states:
        if review_state["pull_id"] in pull_request_ids:
            importer.target.execute("insert into review_state (id, user_id, pull_id, commit_sha, updated_files, updated_unix) values (?, ?, ?, ?, ?, ?)", (review_state["id"], user_id_map.get(review_state["user_id"], 0), review_state["pull_id"], review_state["commit_sha"], review_state["updated_files"], review_state["updated_unix"]))
    for history_row in importer.source_issue_content_history:
        if history_row["issue_id"] in issue_ids:
            importer.target.execute("insert into issue_content_history (id, poster_id, issue_id, comment_id, edited_unix, content_text, is_first_created, is_deleted) values (?, ?, ?, ?, ?, ?, ?, ?)", (history_row["id"], user_id_map.get(history_row["poster_id"], 0) if history_row["poster_id"] else 0, history_row["issue_id"], history_row["comment_id"] if history_row["comment_id"] in comment_ids else 0, history_row["edited_unix"], history_row["content_text"], history_row["is_first_created"], history_row["is_deleted"]))
    for reaction in importer.source_reactions:
        if reaction["issue_id"] in issue_ids:
            importer.target.execute("insert into reaction (id, type, issue_id, comment_id, user_id, original_author_id, original_author, created_unix) values (?, ?, ?, ?, ?, ?, ?, ?)", (reaction["id"], reaction["type"], reaction["issue_id"], reaction["comment_id"] if reaction["comment_id"] in comment_ids else 0, user_id_map.get(reaction["user_id"], 0), user_id_map.get(reaction["original_author_id"], 0) if reaction["original_author_id"] else 0, reaction["original_author"], reaction["created_unix"]))
    importer.reset_sqlite_sequences(("comment", "issue", "issue_assignees", "issue_content_history", "issue_label", "issue_user", "issue_watch", "label", "milestone", "pull_request", "reaction", "review", "review_state"))


def validate(validator) -> None:
    validator.validate_issues()
