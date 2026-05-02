from __future__ import annotations

from ..helpers.common import ForgejoAPIError, bool_value, nullable_text, path_join, visibility_from_int


def import_api(importer) -> None:
    assert importer.api is not None
    for source_org in importer.source_orgs:
        importer.api.request(
            "POST",
            f"/api/v1/admin/users/{path_join(importer.admin_username)}/orgs",
            {
                "username": source_org["name"],
                "full_name": nullable_text(source_org["full_name"]),
                "email": nullable_text(source_org["email"]),
                "location": nullable_text(source_org["location"]),
                "description": nullable_text(source_org["description"]),
                "website": nullable_text(source_org["website"]),
                "visibility": visibility_from_int(source_org["visibility"]),
                "repo_admin_change_team_access": bool_value(source_org["repo_admin_change_team_access"]),
            },
        )
        importer.api.request(
            "PATCH",
            f"/api/v1/orgs/{path_join(source_org['name'])}",
            {
                "full_name": nullable_text(source_org["full_name"]),
                "email": nullable_text(source_org["email"]),
                "location": nullable_text(source_org["location"]),
                "description": nullable_text(source_org["description"]),
                "website": nullable_text(source_org["website"]),
                "visibility": visibility_from_int(source_org["visibility"]),
                "repo_admin_change_team_access": bool_value(source_org["repo_admin_change_team_access"]),
            },
        )
        for source_team in importer.source_teams_by_org[source_org["id"]]:
            if source_team["name"] != "Owners":
                importer.api.request(
                    "POST",
                    f"/api/v1/orgs/{path_join(source_org['name'])}/teams",
                    {
                        "name": source_team["name"],
                        "permission": "read",
                        "includes_all_repositories": bool_value(source_team["includes_all_repositories"]),
                        "can_create_org_repo": bool_value(source_team["can_create_org_repo"]),
                        "description": nullable_text(source_team["description"]),
                        "units_map": importer.placeholder_units_map(),
                    },
                )
            team_id = importer.get_team_id(source_org["name"], source_team["name"])
            for membership in importer.source_team_users.get(source_team["id"], []):
                member = importer.source.execute("select name from user where id = ?", (membership["uid"],)).fetchone()
                if member is None:
                    continue
                try:
                    importer.api.request(
                        "PUT",
                        f"/api/v1/teams/{team_id}/members/{path_join(member['name'])}",
                        expected=(204,),
                    )
                except ForgejoAPIError as exc:
                    if exc.status != 422:
                        raise


def finalize(importer) -> None:
    for source_org in importer.source_orgs:
        target_org = importer.find_target_user(source_org["name"])
        importer.target.execute(
            """
            update user
            set full_name = ?, email = ?, location = ?, website = ?, description = ?, created_unix = ?,
                updated_unix = ?, avatar = ?, avatar_email = ?, use_custom_avatar = ?, visibility = ?,
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
        importer.sync_avatar_file("avatars", source_org["avatar"], bool_value(source_org["use_custom_avatar"]))
        target_teams = {
            row["name"]: row
            for row in importer.target.execute("select * from team where org_id = ? order by id", (target_org["id"],)).fetchall()
        }
        for source_team in importer.source_teams_by_org[source_org["id"]]:
            target_team = target_teams[source_team["name"]]
            importer.target.execute(
                """
                update team
                set lower_name = ?, name = ?, description = ?, authorize = ?, num_repos = ?,
                    num_members = ?, includes_all_repositories = ?, can_create_org_repo = ?
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
            importer.target.execute("delete from team_unit where team_id = ?", (target_team["id"],))
            for team_unit in importer.source_team_units.get(source_team["id"], []):
                importer.target.execute(
                    "insert into team_unit (org_id, team_id, type, access_mode) values (?, ?, ?, ?)",
                    (target_org["id"], target_team["id"], team_unit["type"], team_unit["access_mode"]),
                )
        for org_member in importer.source_org_users.get(source_org["id"], []):
            source_member = importer.source.execute("select name from user where id = ?", (org_member["uid"],)).fetchone()
            if source_member is None:
                continue
            target_member = importer.find_target_user(source_member["name"])
            importer.target.execute(
                "update org_user set is_public = ? where uid = ? and org_id = ?",
                (org_member["is_public"], target_member["id"], target_org["id"]),
            )


def validate(validator) -> None:
    validator.validate_organizations()
    validator.validate_org_memberships()
    validator.validate_teams()
