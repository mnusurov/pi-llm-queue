# Merge policy: branch recipes

The repo settings are the source of truth for the policy itself — read them, don't restate them:

    gh api repos/mnusurov/pi-llm-queue \
      --jq '{allow_squash_merge,allow_merge_commit,allow_rebase_merge,delete_branch_on_merge}'

The head branch is auto-deleted on merge, so `gh pr merge --delete-branch` is redundant.

## `main` is protected

`main` takes changes only through a pull request: a PR is required, the CI checks must be green,
and the rules hold for admins too, so a direct push is refused. The rules are settings — read
them, don't restate them:

    gh api repos/mnusurov/pi-llm-queue/branches/main/protection \
      --jq '{checks: .required_status_checks.contexts, pr: (.required_pull_request_reviews != null), approvals: .required_pull_request_reviews.required_approving_review_count, admins: .enforce_admins.enabled}'

The protection requires the checks and an up-to-date branch, not a review: the repo has one
maintainer.

## Work committed on local `main` moves to a branch

Local commits on `main` are fine: the protection is on the remote branch, not in your clone. Only
the push is refused, so the commit moves to a branch first:

    git switch -c <branch> && git push -u origin <branch>
    git branch -f main origin/main

## Deleting the merged branch

    git checkout main && git pull --prune
    git branch -D <branch>

Squash rewrites history, so the merged branch is never an ancestor of `main`: `git branch -d`
refuses, `-D` is the delete that works. Done when `git branch --list <branch>` and
`git branch --list -r <branch>` are both empty.
