# Release: the recipe

The npm publish happens in CI. A tag `v*` pushed to `main` starts `.github/workflows/release.yml`,
which runs the gate, checks the tag, and publishes. No npm token lives in the repository: npm
trusts the workflow through OIDC.

## One-time setup on npmjs.com

npmjs.com → package `pi-llm-queue` → Settings → Trusted Publisher → GitHub Actions:

- Organization or user: `mnusurov`
- Repository: `pi-llm-queue`
- Workflow filename: `release.yml`
- Environment name: empty
- Allowed actions: `npm publish`

The configuration binds to the repository on its first successful publish, and it expires if that
publish does not happen within two days of creating it. So make the setup and the first release
one sitting.

After the first publish succeeds, tighten the package: Settings → Publishing access → "Require
two-factor authentication and disallow tokens". The trusted publisher keeps working — that setting
only closes the door to long-lived tokens.

## The release

1. On a branch, bump the version: `npm version patch` (or `minor`, `major`). It moves the version
   in `package.json` and in the lockfile.
2. Open a PR, let CI go green, squash-merge it into `main` (`docs/agents/merge-policy.md`).
3. Tag the merged commit and push the tag:

       git tag v0.1.2
       git push origin v0.1.2

4. The Release workflow publishes. Watch it: `gh run watch`.
5. Check the registry: `npm view pi-llm-queue version`.

The tag is the only trigger, so it must be pushed from a `main` that already carries the version
the tag names. The workflow refuses anything else, and it refuses before it uploads anything.

## What the workflow refuses

- A tag that is not `vMAJOR.MINOR.PATCH` — a prerelease suffix is not publishable here.
- A tag whose version is not the version `package.json` declares.
- A tree that fails `npm run check` or `npm test` — nothing is published from a red tree.

The tag rule runs as `npm run check:release`, and it takes the tag as its argument, so it runs
locally too: `npm run check:release -- v0.1.2`.

## When the publish fails

- `ENEEDAUTH` or `E404` — the trusted publisher on npmjs.com does not match this workflow: the
  owner, the repository and the workflow filename must match exactly. npm must be 11.5.1 or newer,
  which the node version the workflow pins guarantees.
- The version is already published — npm takes a version once. Bump the version and release again.
- The gate failed — read the failing step. Nothing reached npm.
