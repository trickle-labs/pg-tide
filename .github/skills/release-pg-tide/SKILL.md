---
name: release-pg-tide
description: "Release workflow for pg-tide. Use when: cutting a release, bumping the version, preparing a new version, tagging a release, publishing pg-tide, incrementing the version number."
argument-hint: "new version number, e.g. 0.5.0"
---

# pg-tide Release Workflow

## When to Use
- Cutting a new release of pg-tide
- Bumping the version (patch, minor, or major)
- Preparing a release commit and tag

## Files That Must Be Updated

Every release requires updating **all** of these files. Missing any will leave
the extension or relay binary at the wrong version.

| File | Field | Notes |
|------|-------|-------|
| `Cargo.toml` | `version` under `[workspace.package]` | Both crates inherit via `version.workspace = true` |
| `pg_tide.control` (root) | `default_version` | Used by `CREATE EXTENSION` |
| `pg-tide-ext/pg_tide.control` | `default_version` | Duplicate; must match root |
| `sql/pg_tide--<old>--<new>.sql` | New file | SQL migration from the previous version to the new one |
| `CHANGELOG.md` | New section at the top | Follow the existing heading format |

The `pg-tide-ext/Cargo.toml` and `pg-tide-relay/Cargo.toml` both use
`version.workspace = true` — they do **not** need manual edits.

## Release Procedure

### 1. Determine the new version

Follow semantic versioning:
- **Patch** (`0.4.x`): bug fixes, no schema changes
- **Minor** (`0.x.0`): new SQL functions, backward-compatible schema additions
- **Major** (`x.0.0`): breaking schema or API changes

### 2. Create the SQL migration file

```
sql/pg_tide--<OLD>--<NEW>.sql
```

The file must contain all DDL changes (new functions, tables, columns, indexes,
`ALTER`s) needed to migrate an existing installation from `<OLD>` to `<NEW>`.
See existing migration files for style and conventions.

If there are no schema changes, the file can be minimal but must still exist:

```sql
-- pg_tide <OLD> → <NEW>
-- No schema changes in this release.
```

### 3. Update `Cargo.toml`

In the **root** `Cargo.toml`, change:
```toml
[workspace.package]
version = "<NEW>"
```

### 4. Update both control files

In **`pg_tide.control`** (root) and **`pg-tide-ext/pg_tide.control`**:
```
default_version = '<NEW>'
```

### 5. Update `CHANGELOG.md`

Add a new section at the top (after the TOC entry) following this format:

```markdown
<!-- TOC start -->
- [<NEW> — <Date> — <Title>](#<anchor>)
...existing entries...
<!-- TOC end -->

---

## [<NEW>] — <YYYY-MM-DD> — <Title>

<Summary paragraph>

### <Category>
- **Feature**: description
```

### 6. Verify consistency

Run this sanity check to confirm all versions agree:
```bash
grep -h "^version\|^default_version" Cargo.toml pg_tide.control pg-tide-ext/pg_tide.control
ls sql/
```

All should show `<NEW>`. The `sql/` listing must include
`pg_tide--<OLD>--<NEW>.sql`.

### 7. Format and lint

```bash
just fmt
just lint
```

Both must pass with zero warnings before committing.

### 8. Commit, tag, and push

```bash
git add Cargo.toml pg_tide.control pg-tide-ext/pg_tide.control \
        sql/pg_tide--<OLD>--<NEW>.sql CHANGELOG.md
git commit -m "Release <NEW>"
git tag v<NEW>
git push && git push --tags
```

## Common Mistakes

- Updating `Cargo.toml` but forgetting one or both `pg_tide.control` files
  (or vice-versa) — this was the motivation for this skill.
- Forgetting to create the SQL migration file — PostgreSQL will refuse to run
  `ALTER EXTENSION pg_tide UPDATE` without it.
- Tagging before lint passes.
