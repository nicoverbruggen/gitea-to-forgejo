# Gitea to Forgejo migration

A few years ago, it was a breeze to migrate from Gitea to Forgejo. Unfortunately, things aren't as simple today.

This script aims to transfer your existing data from **Gitea 1.26 to Forgejo 15.0**.

## What you need

- An (unzipped) backup of your Gitea installation in `./backup/gitea`.
- The following dependencies installed locally: `podman`, `sqlite3`, `curl`, `python3`, and `git`.
- At least one source admin user in the backup. The migration picks the first admin user by ID and uses that account to generate its temporary importer token.

## What is not migrated

- Gitea Actions runtime state, logs, schedules, artifacts, runner registrations, and related tokens.
- OCI package blobs and manifest rows that Forgejo 15 prunes as dangling data during import.
- Source branding and static overrides from `custom/public/*`, such as logos, favicons, and `robots.txt`.
- Two-factor authentication state, WebAuthn credentials, and per-user theme selections.

## What you will get

- A migration report in `./report`.
- A local instance of Forgejo running at `localhost:3000` with its data in `./forgejo`.
- A Forgejo `.zip` backup in `./backup/forgejo` that you can restore.

## What you still need to do

- Validate login credentials approach (see more below)
- Copy and test any remaining customizations that are not migrated automatically into `./forgejo/custom`.
- Enable push and pull in configuration (see more below)

## How to use

Run:

```bash
./migrate.sh
```

This will:

- rebuild `./forgejo`
- rebuild `./report`
- rebuild `./backup/forgejo`
- start a local Forgejo 15.0 instance with Podman
- import users, organizations, teams, repositories, pull mirrors, push mirror rows, issues, comments, releases, stars, watches, collaborators, and attachments from `./backup/gitea`
- import the container package registry data that Forgejo 15 can retain
- replay the source activity feed rows that Forgejo 15 still understands
- validate the migrated data against the backup and fail if mismatches are found
- export a Forgejo backup archive to `./backup/forgejo/forgejo-dump.zip`
- leave Forgejo running on `http://localhost:3000` so you can validate the installation

## Password behavior

By default, the migration preserves the original Gitea password hashes, salts, and password algorithm metadata, so imported users keep their existing passwords.

Two-factor authentication and WebAuth tokens are **not** transferred, so this approach is *a potential security risk*, which you should keep in mind.

If you'd like to generate random passwords which you can send to users:

```bash
./migrate.sh --randomize-passwords
```

Optional environment overrides:

```bash
FORGEJO_IMAGE=codeberg.org/forgejo/forgejo:15.0-rootless \
FORGEJO_CONTAINER_NAME=forgejo-migration-local \
FORGEJO_HTTP_PORT=3000 \
FORGEJO_SSH_PORT=2222 \
./migrate.sh
```

### Mirror behavior

- Pull mirrors are imported as real pull mirrors when Forgejo can create them.
- Push mirror rows are imported too.
- Push mirror remote configuration is preserved because the source bare repositories are copied as-is, including any `remote "remote_mirror_*"` entries in their Git config.
- URL-based push mirror credentials that are stored in those copied Git remotes therefore carry over too.
- Scheduled pull and push mirror updates are both disabled locally, so this verification instance does not sync outward or refresh from remotes in the background.

Mirror credential caveats:

- Pull mirror credentials are only preserved when Forgejo can create the pull mirror from the source remote address successfully.
- Push mirror authentication only carries over when the required remote URL or credentials are present in the backup itself.
- Secrets that lived outside the Gitea backup, such as host-level SSH keys, agent state, or external credential helpers, are not migrated.

### Updating the configuration file

Once the data has been migrated, you need to manually validate the configuration file, and to re-enable pull and push mirroring, change:

```ini
[mirror]
DISABLE_NEW_PULL = false
DISABLE_NEW_PUSH = false

[cron.update_mirrors]
PULL_LIMIT = <non-zero value>
PUSH_LIMIT = <non-zero value>
```

These local safety settings are written into `./forgejo/custom/conf/app.ini` so the verification instance never syncs outward or refreshes mirrors in the background while you inspect the migrated data.

If you don't want to customize the defaults, removing these safeguards for the local installation when deploying to your production instance is probably sufficient.

## Report

Generated outputs:

- `./report/migration-report.md`
- `./report/validation-report.md`
- `./backup/forgejo/forgejo-dump.zip`

Generated only when using `--randomize-passwords`:

- `./report/temporary-passwords.txt`

## Migrated data

The current migration imports:

- users, emails, SSH keys, avatars, organizations, teams, team memberships, and org memberships
- repositories, bare Git history, repo avatars, repo metadata, per-repository enabled/disabled repo units, pull mirrors, and push mirror rows
- issues, issue comments, issue assignees, issue-user state, issue watches, issue content history, labels, milestones, reactions, notifications, follows, and pull requests/reviews if present
- releases and release attachments
- stars, watches, and repository collaborators
- package registry data that Forgejo 15 retains after OCI compatibility cleanup
- activity feed rows from the `action` table

## Normalized data

Some fields are carried across with Forgejo-side normalization because the schemas are not perfectly identical:

- `issue.created` is derived from Gitea `issue.created_unix`
- `issue.pin_order` is set to `0`
- `release.hide_archive_links` is set to `0`
- `attachment.external_url` is left empty
- Forgejo 15 prunes dangling OCI `sha256:*` package manifests, files, and blobs during import

## Omitted data

The current transition intentionally omits:

- Gitea Actions runtime data and logs: `action_run*`, `action_task*`, `action_artifact`, schedules, artifacts, variables, and log files
- runner registration/runtime state: `action_runner*` and related tokens
- package-manifest rows and blobs that Forgejo 15 itself considers dangling OCI data and removes during startup/import cleanup
- source Gitea branding/style overrides from `custom/public/*`, including logos, favicons, and `robots.txt`

The current transition also does not preserve a few Gitea-only fields that do not have a meaningful Forgejo 15 target in this workflow:

- `issue.time_estimate`
- `comment.comment_meta_data`
- `label.exclusive_order`
- user theme selections from the source instance
