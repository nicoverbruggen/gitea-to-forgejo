# Gitea to Forgejo migration

A few years ago, it was a breeze to migrate from Gitea to Forgejo. Unfortunately, things aren't as simple today.

Your goal is to produce a script that I can easily migrate all of the data from my Gitea backup to Forgejo.

You can find the backup in `./backup/gitea`. This is a complete backup. Some of these files are rather big, so keep that in mind.

You can also learn more about how this is set up via: `./server-setup`

To accomplish this, I would like to migrate as much as possible to Forgejo, as seamlessly as possible, without needing to do this manually. 

Ideally, for a minimal use case, the following should be migrated:

- Organizations
- Users
- All repositories (with avatar, description)

Not to be migrated:

- Actions history (incompatible)
- Runner configuration (I will need to set this up again)

The goal of the script is to make it easy to test if things have been properly migrated, so use the latest Docker image to spin up an image locally. 

You can find it here: https://codeberg.org/forgejo/-/packages/container/forgejo

(Note: On this Mac, I have access to Podman, so use that.)

The goal: I can run a script called: `migrate.sh`, which will:

- Spin up a new Forgejo instance with Podman (and remove the running one, if one exists)
- Set up/migrate data via the ./forgejo directory, which is used for the Podman Forgejo instance.
- Make this instance available so I can browse locally to it and see if everything looks OK

(How the runners are set up will be migrated later.)

--> You can find the actual plan to execute at: `PLAN.md`

## Current workflow

Run:

```bash
./migrate.sh
```

This will:

- rebuild `./forgejo`
- start a local Forgejo 15.0 instance with Podman
- import users, organizations, teams, repositories, pull mirrors, push mirror rows, issues, comments, releases, stars, watches, collaborators, and attachments from `./backup/gitea`
- import the container package registry data that Forgejo 15 can retain
- replay the source activity feed rows that Forgejo 15 still understands
- validate the migrated data against the backup and fail if mismatches are found
- leave Forgejo running on `http://localhost:3000`

Mirror behavior on the local test instance:

- Pull mirrors are imported as real pull mirrors when Forgejo can create them.
- Push mirror rows are imported too.
- Scheduled pull and push mirror updates are both disabled locally, so this verification instance does not sync outward or refresh from remotes in the background.

Branding and theme behavior:

- Source Gitea `custom/` styling and asset overrides are not copied into the local Forgejo instance.
- The only preserved source customization is `custom/templates/home.tmpl`.
- User theme preferences are not replayed, so the local verification instance uses the default Forgejo theme.

Generated outputs:

- `./forgejo/temporary-passwords.txt`
- `./forgejo/migration-report.md`
- `./forgejo/validation-report.md`

## Migrated data

The current migration imports:

- users, emails, SSH keys, avatars, organizations, teams, team memberships, and org memberships
- repositories, bare Git history, repo avatars, repo metadata, pull mirrors, and push mirror rows
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

## Gitea 1.26 to Forgejo 15.0

This repository currently targets migration from:

- Gitea `1.26.x` backup data
- into Forgejo `15.0.x`

## Nice to have

- Restructure migration so that things are documented clearly so in the future, this migration can be updated (to support newer versions of Gitea / Forgejo in the future)

## Theming

- How has Codeberg modified their styling for Forgejo?
- Can we create something similar?
- Potentially, apply some CSS fixes to the custom theme to improve the look & feel of the Forgejo client; should be inspired by Codeberg's modifications (can we find that online somewhere?)
