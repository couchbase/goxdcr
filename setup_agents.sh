#!/bin/bash

# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

git submodule update --init
ln -sf .agent-context/AGENTS.md AGENTS.md
ln -sf .agent-context/AGENTS.md CLAUDE.md

# Copilot instructions for legacy JetBrains IDEs
mkdir -p .github
ln -sf ../.agent-context/AGENTS.md .github/copilot-instructions.md

# ---------------------------------------------------------------------------
# Factory AI workspace skills
#
# Factory AI discovers skills at .factory/skills/<skill-name>/SKILL.md.
#
# This block is fully generic and idempotent:
# - It walks every .md file under .agent-context/skills/ (min depth 2,
#   skipping the top-level SKILLS.md index).
# - The Factory AI skill name is derived from the relative path by:
#     1. Stripping the leading "skills/" prefix
#     2. Dropping the ".md" extension
#     3. Lowercasing everything
#     4. Replacing path separators, underscores, and spaces with hyphens
#   Example: skills/architecture/checkpoint/brokenMap.md
#         -> architecture-checkpoint-brokenmap
# - mkdir -p and ln -sf make re-runs safe and forward-compatible: new skills
#   added to .agent-context/skills/ are picked up automatically on next run.
# ---------------------------------------------------------------------------

SKILLS_ROOT=".agent-context/skills"
FACTORY_SKILLS=".factory/skills"

# Create the skills index symlink (README.md points to canonical SKILLS.md)
mkdir -p "$FACTORY_SKILLS"
ln -sf ../../.agent-context/skills/SKILLS.md "$FACTORY_SKILLS/README.md"

while IFS= read -r -d '' md_file; do
    # path relative to SKILLS_ROOT, e.g. architecture/checkpoint/brokenMap.md
    rel="${md_file#"$SKILLS_ROOT/"}"

    # derive skill name: lowercase, strip .md, slashes/underscores/spaces -> hyphens
    skill_name="$(echo "${rel%.md}" | tr '[:upper:]' '[:lower:]' | tr '/_ ' '-')"

    skill_dir="$FACTORY_SKILLS/$skill_name"
    mkdir -p "$skill_dir"

    # skill_dir is always 2 levels deep (.factory/skills/<name>), so target is ../../../
    ln -sf "../../../$md_file" "$skill_dir/SKILL.md"
done < <(find "$SKILLS_ROOT" -mindepth 2 -name "*.md" ! -name "SKILLS.md" -print0 | sort -z)
