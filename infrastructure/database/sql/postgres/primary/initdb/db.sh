#!/bin/bash
# Copyright 2025 Rodericus Ifo Krista
# SPDX-License-Identifier: MIT

set -e

psql -v ON_ERROR_STOP=1 -U ${POSTGRES_USER} <<-EOSQL
    CREATE DATABASE hr_assist;
EOSQL