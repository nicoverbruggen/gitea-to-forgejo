# Forgejo 15.0 Minimal Import Plan

## Summary
Build a fresh-import workflow for a too-new Gitea backup instead of attempting an in-place Forgejo upgrade.

The deliverable is a local, repeatable migration setup centered on:
- `migrate.sh` as the only command the user runs
- `scripts/import_gitea_minimal.py` as the import helper
- `./forgejo/` as the disposable local Forgejo data/config directory

This first pass will target the confirmed source set:
- 2 real user accounts: `nico`, `packagebot`
- 7 organizations
- 9 teams and 10 team memberships
- 74 repositories
- 23 pull mirrors
- 10 push mirrors
- 7 SSH public keys

## Implementation Changes
### 1. Local Forgejo setup
- `migrate.sh` will stop and remove any existing local Forgejo container, then rebuild `./forgejo/` from scratch.
- The script will start Forgejo `15.0-rootless` with Podman on `http://localhost:3000`, plus local SSH on a non-conflicting host port.
- The generated local config will start from `backup/gitea/app.ini`, preserve the original basic instance behavior where safe, and rewrite only local-runtime values:
  - `ROOT_URL`, `DOMAIN`, `SSH_DOMAIN`, and local ports changed for localhost use
  - outbound mail settings copied into a commented block so they can be re-enabled later
  - an active local `[mailer]` section left disabled
  - existing `custom/` assets copied over so branding, templates, and `robots.txt` match the old instance
- The script will initialize Forgejo non-interactively, create the first admin user, and mint an admin token for the import helper.

### 2. User migration
- Import only source rows where `user.type = 0`, which is `nico` and `packagebot`.
- Create users via Forgejo CLI so the target instance owns canonical user rows and password handling.
- Assign new temporary passwords, store them in `./forgejo/temporary-passwords.txt`, and print their location at the end.
- After user creation, patch supplemental user data using a source-to-target username map:
  - full name, admin flag, visibility/privacy flags, descriptive profile fields
  - primary and secondary email addresses from `email_address`
  - SSH keys from `public_key`
  - custom avatar references plus avatar blob files
- Password hashes will not be copied.

### 3. Organization and team migration
- Create all 7 organizations through the Forgejo API using source org metadata.
- Preserve org profile fields and org avatars.
- Rebuild team structure from source `team` and `team_user` data:
  - reuse the default `Owners` team created by Forgejo for each org
  - create the two source `Bot` teams where they exist
  - apply memberships from `team_user`
  - preserve `can_create_org_repo` and `includes_all_repositories`
- Ignore direct `team_repo` replay because every source team already includes all repositories.

### 4. Repository and mirror migration
- For every non-pull-mirror repository:
  - create the repo via API with owner, name, description, visibility, default branch, archived state, and repo avatar metadata
  - stop or pause writes, replace the empty target bare repo with the backup bare repo from `backup/gitea/repos`
  - restore repo avatar blobs and patch the avatar reference if needed
- For pull mirrors:
  - create the repo as a Forgejo pull mirror from the source `mirror.remote_address`
  - overwrite the created bare repo with the exact backup snapshot so the initial local verification reflects the backup
  - restore mirror settings from the source `mirror` row, including interval and prune behavior
- For push mirrors:
  - create the repo from backup data first
  - then recreate push mirror rows from `push_mirror`, preserving remote URL, interval, and `sync_on_commit`
- If a pull-mirror remote is unreachable during creation, the fallback is deterministic:
  - import the backup repo as a normal repo
  - record that mirror reactivation failed in the migration report
  - continue the rest of the migration instead of aborting

## Public Interfaces
- `migrate.sh`
  - full reset and rebuild of the local verification instance
  - no partial/incremental mode in v1
- `scripts/import_gitea_minimal.py`
  - reads the Gitea SQLite backup
  - talks to the running Forgejo instance and its SQLite DB
- Outputs in `./forgejo/`
  - `temporary-passwords.txt`
  - `migration-report.md`

## Test Plan
- `migrate.sh` completes on a clean run and leaves Forgejo reachable at `http://localhost:3000`.
- Login works for `nico` with the generated temporary password.
- Target counts match source expectations for this first pass:
  - 2 users
  - 7 organizations
  - 9 teams
  - 10 team memberships
  - 74 repositories
  - 23 pull mirrors
  - 10 push mirrors
  - 7 SSH public keys
- Spot-check representative repos:
  - normal repo
  - archived repo
  - private repo
  - pull mirror
  - repo with push mirror
- Spot-check representative metadata:
  - user avatar
  - org avatar
  - repo avatar
  - default branch
  - archived flag
  - visibility
- Run `forgejo doctor check --all` after import and include the result in `migration-report.md`.

## Assumptions And Defaults
- This workflow is for local verification, not direct production cutover.
- Local URL is `localhost`, not the production domain.
- Outbound email stays disabled locally, but the original SMTP block is preserved as comments for later reuse.
- The first pass intentionally excludes issues, releases, attachments, packages, and Actions history.
- Temporary passwords are acceptable for the imported user accounts.
- Live mirror recreation is in scope, but mirror-network failures degrade to a normal imported repo plus a clear warning in the report instead of failing the whole run.
