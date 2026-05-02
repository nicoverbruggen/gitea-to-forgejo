from __future__ import annotations

from ..helpers.common import bool_value, nullable_text, path_join, visibility_from_int


def import_api(importer) -> None:
    assert importer.api is not None
    for source_user in importer.source_users:
        importer.api.request(
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
        for key_row in importer.source_keys.get(source_user["id"], []):
            importer.api.request(
                "POST",
                f"/api/v1/admin/users/{path_join(source_user['name'])}/keys",
                {"title": key_row["name"], "key": key_row["content"], "read_only": False},
            )


def finalize(importer) -> None:
    for source_user in importer.source_users:
        target_user = importer.find_target_user(source_user["name"])
        importer.sync_user_emails(source_user, target_user["id"])
        if importer.password_mode == "preserve":
            password_sql = """
                    passwd = ?,
                    passwd_hash_algo = ?,
                    must_change_password = ?,
                    rands = ?,
                    salt = ?,
                """
            password_params = (
                source_user["passwd"],
                source_user["passwd_hash_algo"],
                source_user["must_change_password"],
                source_user["rands"],
                source_user["salt"],
            )
        else:
            password_sql = ""
            password_params = ()
        importer.target.execute(
            f"""
            update user
            set email = ?, full_name = ?, location = ?, website = ?, language = ?, description = ?,
                created_unix = ?, updated_unix = ?, last_login_unix = ?, last_repo_visibility = ?,
                max_repo_creation = ?, is_active = ?, is_admin = ?, is_restricted = ?, allow_git_hook = ?,
                allow_import_local = ?, allow_create_organization = ?, prohibit_login = ?, avatar = ?,
                avatar_email = ?, use_custom_avatar = ?, visibility = ?, diff_view_style = ?,
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
        importer.sync_avatar_file("avatars", source_user["avatar"], bool_value(source_user["use_custom_avatar"]))


def validate(validator) -> None:
    check = "users"
    source_rows = {row["name"]: row for row in validator.fetch_all(validator.source, "select * from user where type = 0 order by id")}
    target_rows = {row["name"]: row for row in validator.fetch_all(validator.target, "select * from user where type = 0 order by id")}
    validator.compare_key_sets(check, set(source_rows), set(target_rows))
    fields = [
        "email", "full_name", "location", "website", "language", "description", "visibility", "is_admin",
        "is_active", "is_restricted", "allow_git_hook", "allow_import_local", "allow_create_organization",
        "prohibit_login", "avatar", "avatar_email", "use_custom_avatar", "diff_view_style", "keep_activity_private",
    ]
    for username in sorted(set(source_rows) & set(target_rows)):
        source_row = source_rows[username]
        target_row = target_rows[username]
        for field in fields:
            validator.compare_values(check, f"{username}.{field}", source_row[field], target_row[field])
        if validator.password_mode == "preserve":
            for field in ("passwd", "passwd_hash_algo", "salt", "rands", "must_change_password"):
                validator.compare_values(check, f"{username}.{field}", source_row[field], target_row[field])
    source_emails = validator.user_email_map(validator.source, source_rows)
    target_emails = validator.user_email_map(validator.target, target_rows)
    for username in sorted(set(source_rows) & set(target_rows)):
        if source_emails[username] != target_emails[username]:
            validator.add_failure(check, f"{username}.emails mismatch: expected {source_emails[username]!r}, found {target_emails[username]!r}")
    validator.add_note(f"Validated {len(source_rows)} user records and email sets")
    validator.add_note("User theme preferences are intentionally normalized to the Forgejo default theme")
    if validator.password_mode == "preserve":
        validator.add_note("User passwords are validated against the original Gitea password hashes")
    else:
        validator.add_note("User passwords are intentionally randomized for testing and are not compared to source hashes")
