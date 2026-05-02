from __future__ import annotations

from ..helpers.common import log, normalize_text


def finalize(importer) -> None:
    assert importer.target is not None
    log("Replacing package registry data and restoring package blobs offline")
    importer.copy_package_files()
    target_packages_dir = importer.forgejo_root / "data" / "packages"
    retained_blob_hashes = {normalize_text(row["hash_sha256"]) for row in importer.kept_source_package_blobs}
    for blob in importer.source_package_blobs:
        blob_hash = normalize_text(blob["hash_sha256"])
        if blob_hash in retained_blob_hashes:
            continue
        blob_path = target_packages_dir / blob_hash[:2] / blob_hash[2:4] / blob_hash
        if blob_path.exists():
            blob_path.unlink()
    for table_name in ("package_property", "package_file", "package_version", "package_blob", "package_cleanup_rule", "package"):
        importer.target.execute(f"delete from {table_name}")
    user_id_map = importer.build_user_id_map()
    repo_id_map = importer.build_repo_id_map()
    for package in importer.source_packages:
        importer.target.execute("insert into package (id, owner_id, repo_id, type, name, lower_name, semver_compatible, is_internal) values (?, ?, ?, ?, ?, ?, ?, ?)", (package["id"], user_id_map[package["owner_id"]], repo_id_map.get(package["repo_id"], 0) if package["repo_id"] else 0, package["type"], package["name"], package["lower_name"], package["semver_compatible"], package["is_internal"]))
    for blob in importer.kept_source_package_blobs:
        importer.target.execute("insert into package_blob (id, size, hash_md5, hash_sha1, hash_sha256, hash_sha512, hash_blake2b, created_unix) values (?, ?, ?, ?, ?, ?, ?, ?)", (blob["id"], blob["size"], blob["hash_md5"], blob["hash_sha1"], blob["hash_sha256"], blob["hash_sha512"], None, blob["created_unix"]))
    for version in importer.kept_source_package_versions:
        importer.target.execute("insert into package_version (id, package_id, creator_id, version, lower_version, created_unix, is_internal, metadata_json, download_count) values (?, ?, ?, ?, ?, ?, ?, ?, ?)", (version["id"], version["package_id"], user_id_map.get(version["creator_id"], 0), version["version"], version["lower_version"], version["created_unix"], version["is_internal"], version["metadata_json"], version["download_count"]))
    for package_file in importer.kept_source_package_files:
        importer.target.execute("insert into package_file (id, version_id, blob_id, name, lower_name, composite_key, is_lead, created_unix) values (?, ?, ?, ?, ?, ?, ?, ?)", (package_file["id"], package_file["version_id"], package_file["blob_id"], package_file["name"], package_file["lower_name"], package_file["composite_key"], package_file["is_lead"], package_file["created_unix"]))
    for package_property in importer.kept_source_package_properties:
        importer.target.execute("insert into package_property (id, ref_type, ref_id, name, value) values (?, ?, ?, ?, ?)", (package_property["id"], package_property["ref_type"], package_property["ref_id"], package_property["name"], package_property["value"]))
    for cleanup_rule in importer.source_package_cleanup_rules:
        importer.target.execute("insert into package_cleanup_rule (id, enabled, owner_id, type, keep_count, keep_pattern, remove_days, remove_pattern, match_full_name, created_unix, updated_unix) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (cleanup_rule["id"], cleanup_rule["enabled"], user_id_map[cleanup_rule["owner_id"]], cleanup_rule["type"], cleanup_rule["keep_count"], cleanup_rule["keep_pattern"], cleanup_rule["remove_days"], cleanup_rule["remove_pattern"], cleanup_rule["match_full_name"], cleanup_rule["created_unix"], cleanup_rule["updated_unix"]))
    importer.reset_sqlite_sequences(("package", "package_blob", "package_cleanup_rule", "package_file", "package_property", "package_version"))


def validate(validator) -> None:
    validator.validate_packages()
