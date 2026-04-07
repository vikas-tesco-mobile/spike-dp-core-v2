#!/bin/bash

commit_msg_file="$1"
commit_msg=$(cat "$commit_msg_file")

# Conventional Commits pattern
pattern='^CDM-[0-9]+ \| .+$'

if ! echo "$commit_msg" | grep -Eq "$pattern"; then
  echo "❌ Commit message does not follow Conventional Commits format."
  echo "👉 Example: CDM-100 | Add basic repo set up"
  echo ""
  echo "Your message was:"
  echo "$commit_msg"
  exit 1
fi

echo "✅ Commit message format passed."