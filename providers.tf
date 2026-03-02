terraform {
  required_version = ">= 1.0.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

provider "snowflake" {
  organization_name        = var.snowflake_organization
  account_name             = var.snowflake_account
  user                     = var.snowflake_user
  password                 = var.snowflake_password
  passcode                 = "000000"
  role                     = var.snowflake_role
  preview_features_enabled = ["snowflake_table_resource"]
}
