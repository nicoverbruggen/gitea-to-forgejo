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
- import the minimal supported data set from `./backup/gitea`
- import the container package registry data that Forgejo 15 can retain
- validate the migrated data against the backup and fail if mismatches are found
- leave Forgejo running on `http://localhost:3000`

Generated outputs:

- `./forgejo/temporary-passwords.txt`
- `./forgejo/migration-report.md`
- `./forgejo/validation-report.md`

## Todo

- Based on the migration script, tell me what differences exist in how the data is stored between Gitea -> Forgejo. Is any meaningful data omitted?
- Since it should be possible, migrate other data (issues, issue comments, pull requests, pull request comments, stars, releases, release attachments)
- Determine if actions data can be migrated

## Nice to have

- Restructure migration so that things are documented clearly so in the future, this migration can be updated (to support newer versions of Gitea / Forgejo)
- Document we are migrating from Gitea 1.26 to Forgejo 15.0
- Use Forgejo customizations instead (it's one of the repos: `backup/gitea/repos/nico/forgejo-customize.git`)
- Potentially, apply some CSS fixes to the custom theme to improve the look & feel of the Forgejo client; should be inspired by Codeberg's modifications (can we find that online somewhere?)
