from __future__ import annotations

from .common import normalize_int, normalize_text


def compute_retained_package_rows(packages, versions, files, blobs, properties):
    package_ids = {row["id"] for row in packages}
    referenced_digests = {
        normalize_text(row["value"])
        for row in properties
        if normalize_int(row["ref_type"]) == 0 and normalize_text(row["name"]) == "container.manifest.reference"
    }
    kept_versions = [
        row
        for row in versions
        if not normalize_text(row["version"]).startswith("sha256:")
        or normalize_text(row["version"]) in referenced_digests
    ]
    kept_version_ids = {row["id"] for row in kept_versions}
    kept_files = [row for row in files if row["version_id"] in kept_version_ids]
    kept_file_ids = {row["id"] for row in kept_files}
    kept_blob_ids = {row["blob_id"] for row in kept_files}
    kept_blobs = [row for row in blobs if row["id"] in kept_blob_ids]
    kept_properties = [
        row
        for row in properties
        if (normalize_int(row["ref_type"]) == 0 and normalize_int(row["ref_id"]) in kept_version_ids)
        or (normalize_int(row["ref_type"]) == 1 and normalize_int(row["ref_id"]) in kept_file_ids)
        or (normalize_int(row["ref_type"]) == 2 and normalize_int(row["ref_id"]) in package_ids)
    ]
    return kept_versions, kept_files, kept_blobs, kept_properties
