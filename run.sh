#!/usr/bin/env bash

set -euo pipefail

db_uri=$(pg_tmp)
sql=$(mktemp)

trap "rm ${sql}" exit

# Convert the Markdown file into a runnable SQL script, see
# https://github.com/monacoremo/postgrest-sessions-example/blob/master/deploy/md2sql.sed
sed -e '/^```/,/^```/ !s/^/-- /' -e '/^```/ s/^/-- /' < trees.sql.md > "${sql}"

psql ${db_uri} -P pager=off -f "${sql}"
