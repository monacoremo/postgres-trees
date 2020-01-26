#!/usr/bin/env bash

db_uri=$(pg_tmp)

psql ${db_uri} -P pager=off -f trees.sql
