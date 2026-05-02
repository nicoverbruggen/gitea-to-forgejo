from __future__ import annotations


def finalize(importer) -> None:
    repo_id_map = importer.build_repo_id_map()
    importer.target.execute("delete from repo_unit")
    for repo_unit in importer.source_repo_units:
        target_repo_id = repo_id_map.get(repo_unit["repo_id"])
        if target_repo_id is None:
            continue
        importer.target.execute(
            "insert into repo_unit (id, repo_id, type, config, created_unix, default_permissions) values (?, ?, ?, ?, ?, ?)",
            (
                repo_unit["id"],
                target_repo_id,
                repo_unit["type"],
                repo_unit["config"],
                repo_unit["created_unix"],
                repo_unit["everyone_access_mode"],
            ),
        )
    importer.reset_sqlite_sequences(("repo_unit",))


def validate(validator) -> None:
    validator.validate_repo_units()
