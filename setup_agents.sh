#!/bin/bash

# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

git submodule update --init
ln -sf .agent-config/AGENTS.md AGENTS.md

# Copilot instructions for legacy JetBrains IDEs
mkdir -p .github
ln -sf ../.agent-config/AGENTS.md .github/copilot-instructions.md
