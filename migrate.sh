#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$ROOT_DIR/backup/gitea"
BACKUP_DIR="$ROOT_DIR/backup/forgejo"
SOURCE_DB="$SOURCE_DIR/data/gitea.db"
SOURCE_APP_INI="$SOURCE_DIR/app.ini"

FORGEJO_DIR="$ROOT_DIR/forgejo"
FORGEJO_DATA_DIR="$FORGEJO_DIR/data"
FORGEJO_CUSTOM_DIR="$FORGEJO_DIR/custom"
FORGEJO_APP_INI="$FORGEJO_CUSTOM_DIR/conf/app.ini"
FORGEJO_DB="$FORGEJO_DATA_DIR/forgejo.db"
REPORT_DIR="$ROOT_DIR/report"

GITEA_VERSION="1.26"
FORGEJO_VERSION="15.0.1"
export GITEA_VERSION FORGEJO_VERSION
FORGEJO_IMAGE="${FORGEJO_IMAGE:-codeberg.org/forgejo/forgejo:${FORGEJO_VERSION}-rootless}"
FORGEJO_CONTAINER_NAME="${FORGEJO_CONTAINER_NAME:-forgejo-migration-local}"
FORGEJO_HTTP_PORT="${FORGEJO_HTTP_PORT:-3000}"
FORGEJO_SSH_PORT="${FORGEJO_SSH_PORT:-2222}"
FORGEJO_BASE_URL="http://localhost:${FORGEJO_HTTP_PORT}"
PASSWORD_MODE="preserve"
ADMIN_USERNAME=""
DETECTED_ADMIN_USERNAME=""

BOOTSTRAP_PASSWORD_FILE="$REPORT_DIR/bootstrap-passwords.txt"
PASSWORD_FILE="$REPORT_DIR/temporary-passwords.txt"
TOKEN_FILE="$REPORT_DIR/admin-token.txt"
REPORT_FILE="$REPORT_DIR/migration-report.md"
VALIDATION_REPORT="$REPORT_DIR/validation-report.md"
STATE_FILE="$REPORT_DIR/import-state.json"
IMPORTER="$ROOT_DIR/scripts/import_gitea_minimal.py"
VALIDATOR="$ROOT_DIR/scripts/validate_migration.py"

log() {
    printf '[migrate] %s\n' "$*"
}

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf 'Missing required command: %s\n' "$1" >&2
        exit 1
    fi
}

bootstrap_mount() {
    # `:U` remaps ownership for the rootless image inside the Podman VM.
    # We only need that during initial bootstrap when the local Forgejo
    # workspace is still empty.
    printf '%s:/var/lib/gitea:U' "$FORGEJO_DIR"
}

runtime_mount() {
    printf '%s:/var/lib/gitea' "$FORGEJO_DIR"
}

backup_mount() {
    # The rootless image also needs ownership remapping to write the exported
    # dump archive back into the host backup directory.
    printf '%s:/backup:U' "$BACKUP_DIR"
}

forgejo_bootstrap_run() {
    podman run --rm \
        -v "$(bootstrap_mount)" \
        "$FORGEJO_IMAGE" \
        forgejo --config /var/lib/gitea/custom/conf/app.ini "$@"
}

forgejo_run() {
    podman run --rm \
        -v "$(runtime_mount)" \
        "$FORGEJO_IMAGE" \
        forgejo --config /var/lib/gitea/custom/conf/app.ini "$@"
}

forgejo_dump_run() {
    podman run --rm \
        -v "$(runtime_mount)" \
        -v "$(backup_mount)" \
        "$FORGEJO_IMAGE" \
        forgejo --config /var/lib/gitea/custom/conf/app.ini dump "$@"
}

remove_import_token() {
    sqlite3 "$FORGEJO_DB" \
        "delete from access_token where uid = (select id from user where lower_name = lower('$DETECTED_ADMIN_USERNAME')) and name = 'minimal-import'"
}

cleanup_container() {
    podman rm -f "$FORGEJO_CONTAINER_NAME" >/dev/null 2>&1 || true
}

wait_for_forgejo() {
    local attempts=60
    local index

    for ((index = 1; index <= attempts; index += 1)); do
        if curl -fsS "${FORGEJO_BASE_URL}/api/v1/version" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done

    podman logs "$FORGEJO_CONTAINER_NAME" >&2 || true
    return 1
}

append_doctor_report() {
    local doctor_output
    local doctor_status=0

    doctor_output="$(forgejo_run doctor check --all 2>&1)" || doctor_status=$?

    {
        printf '\n## Forgejo Doctor\n\n'
        if [ "$doctor_status" -eq 0 ]; then
            printf 'Status: **PASS**\n\n'
        else
            printf 'Status: **ADVISORY FAILURE**\n\n'
            printf '> [!WARNING]\n'
            printf '> `forgejo doctor check --all` reported problems, but the migration continued because doctor failures are advisory in this workflow.\n\n'
        fi
        printf 'Exit status: `%s`\n\n' "$doctor_status"
        printf '```text\n%s\n```\n' "$doctor_output"
    } >>"$REPORT_FILE"

    return "$doctor_status"
}

generate_passwords() {
    : >"$BOOTSTRAP_PASSWORD_FILE"

    while IFS='|' read -r username; do
        password="$(
            python3 - <<'PY'
import secrets
import string

alphabet = string.ascii_letters + string.digits
print("".join(secrets.choice(alphabet) for _ in range(24)))
PY
        )"

        printf '%s|%s\n' "$username" "$password" >>"$BOOTSTRAP_PASSWORD_FILE"
    done < <(
        sqlite3 -separator '|' "$SOURCE_DB" \
            "select name from user where type = 0 order by id"
    )

    if [ "$PASSWORD_MODE" = "randomize" ]; then
        cp "$BOOTSTRAP_PASSWORD_FILE" "$PASSWORD_FILE"
    else
        rm -f "$PASSWORD_FILE"
    fi
}

password_for_user() {
    local username="$1"
    awk -F'|' -v wanted="$username" '$1 == wanted { print $2 }' "$BOOTSTRAP_PASSWORD_FILE"
}

detect_admin_user() {
    DETECTED_ADMIN_USERNAME="$(sqlite3 "$SOURCE_DB" "select name from user where type = 0 and coalesce(is_admin, 0) = 1 order by id limit 1")"
    if [ -z "$DETECTED_ADMIN_USERNAME" ]; then
        printf 'Missing source admin user in %s\n' "$SOURCE_DB" >&2
        exit 1
    fi
    ADMIN_USERNAME="$DETECTED_ADMIN_USERNAME"
}

create_local_config() {
    python3 - "$SOURCE_APP_INI" "$FORGEJO_APP_INI" "$FORGEJO_HTTP_PORT" "$FORGEJO_SSH_PORT" <<'PY'
import configparser
import io
import os
import secrets
import sys
from pathlib import Path

source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
http_port = sys.argv[3]
ssh_port = sys.argv[4]
forgejo_version = os.environ["FORGEJO_VERSION"]

parser = configparser.RawConfigParser(strict=False)
parser.optionxform = str
source_text = source_path.read_text(encoding="utf-8")
source_lines = source_text.splitlines()

root_options: dict[str, str] = {}
section_lines: list[str] = []
in_section = False

for line in source_lines:
    stripped = line.strip()
    if stripped.startswith("["):
        in_section = True
    if in_section:
        section_lines.append(line)
    elif "=" in line:
        key, value = line.split("=", 1)
        root_options[key.strip()] = value.strip()

if section_lines:
    parser.read_string("\n".join(section_lines))

for section in ("database", "repository", "server", "security", "oauth2", "service", "mailer"):
    if not parser.has_section(section):
        parser.add_section(section)

for section in ("mirror", "cron.update_mirrors"):
    if not parser.has_section(section):
        parser.add_section(section)

parser.set("database", "DB_TYPE", "sqlite3")
parser.set("database", "PATH", "/var/lib/gitea/data/forgejo.db")
parser.set("database", "SQLITE_JOURNAL_MODE", "DELETE")

parser.set("repository", "ROOT", "/var/lib/gitea/data/forgejo-repositories")

parser.set("server", "PROTOCOL", "http")
parser.set("server", "HTTP_ADDR", "0.0.0.0")
parser.set("server", "HTTP_PORT", http_port)
parser.set("server", "DOMAIN", "localhost")
parser.set("server", "SSH_DOMAIN", "localhost")
parser.set("server", "ROOT_URL", f"http://localhost:{http_port}/")
parser.set("server", "START_SSH_SERVER", "true")
parser.set("server", "DISABLE_SSH", "false")
parser.set("server", "SSH_PORT", ssh_port)
parser.set("server", "SSH_LISTEN_PORT", ssh_port)
parser.set("server", "LFS_START_SERVER", "true")

parser.set("security", "INSTALL_LOCK", "true")
parser.set("security", "INTERNAL_TOKEN", secrets.token_urlsafe(32))
parser.set("security", "SECRET_KEY", secrets.token_urlsafe(48))
if not parser.has_option("security", "PASSWORD_HASH_ALGO"):
    parser.set("security", "PASSWORD_HASH_ALGO", "pbkdf2")

parser.set("oauth2", "JWT_SECRET", secrets.token_urlsafe(32))

parser.set("service", "DISABLE_REGISTRATION", "true")
if not parser.has_option("service", "REQUIRE_SIGNIN_VIEW"):
    parser.set("service", "REQUIRE_SIGNIN_VIEW", "false")

# This local verification instance should preserve mirror metadata, but it
# should never queue background pull or push sync jobs against remotes.
parser.set("mirror", "ENABLED", "true")
parser.set("mirror", "DISABLE_NEW_PULL", "false")
parser.set("mirror", "DISABLE_NEW_PUSH", "true")
parser.set("cron.update_mirrors", "PULL_LIMIT", "0")
parser.set("cron.update_mirrors", "PUSH_LIMIT", "0")

if not parser.has_section("mailer"):
    parser.add_section("mailer")
parser.set("mailer", "ENABLED", "false")

buffer = io.StringIO()
parser.write(buffer)
body = buffer.getvalue().strip()

lines = [
    f"; Generated by migrate.sh for a local Forgejo {forgejo_version} verification instance.",
    "; Compatibility paths still use /var/lib/gitea because the official rootless image expects them.",
]

preferred_root = {
    "APP_NAME": root_options.get("APP_NAME", "Forgejo"),
    "RUN_USER": root_options.get("RUN_USER", "git"),
    "RUN_MODE": root_options.get("RUN_MODE", "prod"),
    "WORK_PATH": root_options.get("WORK_PATH", "/var/lib/gitea"),
}

for key, value in preferred_root.items():
    lines.append(f"{key} = {value}")

lines.extend(
    [
        "",
        body,
    ]
)

target_path.parent.mkdir(parents=True, exist_ok=True)
target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

create_cli_users() {
    while IFS='|' read -r username email is_admin; do
        local password
        password="$(password_for_user "$username")"
        log "Creating local Forgejo user $username"

        if [ "$is_admin" = "1" ]; then
            forgejo_bootstrap_run admin user create \
                --username "$username" \
                --password "$password" \
                --email "$email" \
                --admin \
                --must-change-password=false
        else
            forgejo_bootstrap_run admin user create \
                --username "$username" \
                --password "$password" \
                --email "$email" \
                --must-change-password=false
        fi
    done < <(
        sqlite3 -separator '|' "$SOURCE_DB" \
            "select name,email,coalesce(is_admin, 0) from user where type = 0 order by id"
    )
}

main() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --randomize-passwords)
                PASSWORD_MODE="randomize"
                shift
                ;;
            *)
                printf 'Unknown argument: %s\n' "$1" >&2
                exit 1
                ;;
        esac
    done

    require_command podman
    require_command sqlite3
    require_command curl
    require_command python3
    require_command git

    if [ ! -f "$SOURCE_DB" ]; then
        printf 'Missing source database: %s\n' "$SOURCE_DB" >&2
        exit 1
    fi

    if [ ! -f "$IMPORTER" ]; then
        printf 'Missing importer: %s\n' "$IMPORTER" >&2
        exit 1
    fi

    if [ ! -f "$VALIDATOR" ]; then
        printf 'Missing validator: %s\n' "$VALIDATOR" >&2
        exit 1
    fi

    detect_admin_user

    log "Resetting local Forgejo workspace"
    cleanup_container
    rm -rf "$FORGEJO_DIR" "$REPORT_DIR" "$BACKUP_DIR"
    mkdir -p "$FORGEJO_CUSTOM_DIR/conf" "$FORGEJO_CUSTOM_DIR/templates" "$FORGEJO_DATA_DIR/home" "$FORGEJO_DIR/git"
    mkdir -p "$REPORT_DIR" "$BACKUP_DIR"

    if [ -d "$SOURCE_DIR/custom/templates" ]; then
        mkdir -p "$FORGEJO_CUSTOM_DIR/templates"
        cp -R "$SOURCE_DIR/custom/templates/." "$FORGEJO_CUSTOM_DIR/templates/"
    fi

    if [ -d "$SOURCE_DIR/custom/public" ]; then
        mkdir -p "$FORGEJO_CUSTOM_DIR/public"
        cp -R "$SOURCE_DIR/custom/public/." "$FORGEJO_CUSTOM_DIR/public/"
    fi

    if [ -f "$SOURCE_DIR/data/home/.gitconfig" ]; then
        cp "$SOURCE_DIR/data/home/.gitconfig" "$FORGEJO_DATA_DIR/home/.gitconfig"
    fi

    create_local_config
    generate_passwords

    log "Bootstrapping the Forgejo database"
    forgejo_bootstrap_run migrate

    log "Creating bootstrap users"
    create_cli_users

    log "Generating admin token for importer"
    forgejo_bootstrap_run admin user generate-access-token \
        --username "$ADMIN_USERNAME" \
        --token-name minimal-import \
        --scopes all \
        --raw >"$TOKEN_FILE"

    log "Starting local Forgejo container"
    podman run -d \
        --name "$FORGEJO_CONTAINER_NAME" \
        -p "${FORGEJO_HTTP_PORT}:${FORGEJO_HTTP_PORT}" \
        -p "${FORGEJO_SSH_PORT}:${FORGEJO_SSH_PORT}" \
        -v "$(runtime_mount)" \
        "$FORGEJO_IMAGE" >/dev/null

    log "Waiting for Forgejo to accept HTTP requests"
    wait_for_forgejo

    log "Importing users, organizations, teams, repositories, and mirrors"
    python3 "$IMPORTER" \
        --mode api \
        --source-db "$SOURCE_DB" \
        --forgejo-db "$FORGEJO_DB" \
        --backup-root "$SOURCE_DIR" \
        --forgejo-root "$FORGEJO_DIR" \
        --base-url "$FORGEJO_BASE_URL" \
        --token-file "$TOKEN_FILE" \
        --admin-username "$ADMIN_USERNAME" \
        --report-path "$REPORT_FILE" \
        --state-path "$STATE_FILE"

    log "Stopping Forgejo so hooks, keys, and doctor checks run against settled data"
    podman stop "$FORGEJO_CONTAINER_NAME" >/dev/null

    log "Removing temporary importer access token"
    remove_import_token

    log "Finalizing offline database and repository metadata"
    python3 "$IMPORTER" \
        --mode finalize \
        --source-db "$SOURCE_DB" \
        --forgejo-db "$FORGEJO_DB" \
        --backup-root "$SOURCE_DIR" \
        --forgejo-root "$FORGEJO_DIR" \
        --admin-username "$DETECTED_ADMIN_USERNAME" \
        --password-mode "$PASSWORD_MODE" \
        --report-path "$REPORT_FILE" \
        --state-path "$STATE_FILE"

    log "Normalizing writable permissions for the disposable local Forgejo data tree"
    mkdir -p "$FORGEJO_DIR/log"
    chmod -R u+rwX,go+rwX "$FORGEJO_DIR/data/forgejo-repositories"
    chmod u+rwX,go+rwX "$FORGEJO_DIR/log"

    log "Clearing copied legacy hook files so Forgejo can regenerate its own hook layout"
    find "$FORGEJO_DIR/data/forgejo-repositories" -type f -path '*/hooks/*' -delete

    log "Regenerating Forgejo hooks"
    forgejo_run admin regenerate hooks

    log "Regenerating Forgejo authorized_keys"
    forgejo_run admin regenerate keys

    log "Running Forgejo doctor"
    append_doctor_report || true

    log "Validating migrated data"
    python3 "$VALIDATOR" \
        --source-db "$SOURCE_DB" \
        --forgejo-db "$FORGEJO_DB" \
        --backup-root "$SOURCE_DIR" \
        --forgejo-root "$FORGEJO_DIR" \
        --password-mode "$PASSWORD_MODE" \
        --state-path "$STATE_FILE" \
        --report-path "$VALIDATION_REPORT"

    rm -f "$BOOTSTRAP_PASSWORD_FILE"

    log "Exporting Forgejo backup"
    forgejo_dump_run \
        --file /backup/forgejo-dump.zip \
        --type zip \
        --tempdir /tmp

    log "Restarting Forgejo for local verification"
    podman start "$FORGEJO_CONTAINER_NAME" >/dev/null
    wait_for_forgejo

    log "Migration complete"
    printf '\n'
    printf 'Forgejo URL: %s\n' "$FORGEJO_BASE_URL"
    printf 'SSH URL base: ssh://git@localhost:%s/\n' "$FORGEJO_SSH_PORT"
    if [ "$PASSWORD_MODE" = "randomize" ]; then
        printf 'Password mode: randomized for testing\n'
        printf 'Temporary passwords: %s\n' "$PASSWORD_FILE"
    else
        printf 'Password mode: preserved from source Gitea backup\n'
    fi
    printf 'Migration report: %s\n' "$REPORT_FILE"
    printf 'Validation report: %s\n' "$VALIDATION_REPORT"
    printf 'Forgejo backup: %s/forgejo-dump.zip\n' "$BACKUP_DIR"
}

main "$@"
