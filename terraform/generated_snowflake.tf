# ============================================================
# HDMF EXISTING SNOWFLAKE INFRASTRUCTURE
# File: generated_snowflake.tf
# ============================================================


# ------------------------------------------------------------
# SNOWFLAKE DATABASE
# ------------------------------------------------------------

resource "snowflake_database" "hdmf" {
  name = "HDMF_MIGRATION_DB"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# SNOWFLAKE WAREHOUSE
# ------------------------------------------------------------

resource "snowflake_warehouse" "hdmf" {
  name = "HDMF_MIGRATION_WH"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# SNOWFLAKE ACCOUNT ROLE
# ------------------------------------------------------------

resource "snowflake_account_role" "hdmf" {
  name = "HDMF_MIGRATION_ROLE"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# STAGING SCHEMA
# ------------------------------------------------------------

resource "snowflake_schema" "staging" {
  database = "HDMF_MIGRATION_DB"
  name     = "STAGING"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# CURATED SCHEMA
# ------------------------------------------------------------

resource "snowflake_schema" "curated" {
  database = "HDMF_MIGRATION_DB"
  name     = "CURATED"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# AUDIT SCHEMA
# ------------------------------------------------------------

resource "snowflake_schema" "audit" {
  database = "HDMF_MIGRATION_DB"
  name     = "AUDIT"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}


# ------------------------------------------------------------
# CONTROL SCHEMA
# ------------------------------------------------------------

resource "snowflake_schema" "control" {
  database = "HDMF_MIGRATION_DB"
  name     = "CONTROL"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = all
  }
}
