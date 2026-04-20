terraform {
  required_version = ">= 1.7.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.95"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstateiotlakehouse"
    container_name       = "tfstate"
    key                  = "iot-lakehouse.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

# ── Resource Group ─────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "${var.prefix}-rg-${var.environment}"
  location = var.location

  tags = local.common_tags
}

# ── Storage — ADLS Gen2 ────────────────────────────────────────────────────────

resource "azurerm_storage_account" "adls" {
  name                     = "${var.prefix}adls${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true      # Hierarchical namespace — required for ADLS Gen2

  blob_properties {
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 30
    }
  }

  tags = local.common_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "iot-lakehouse"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_path" "layers" {
  for_each = toset(["bronze", "silver", "gold", "landing", "reference", "_checkpoints"])

  path               = each.value
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

# ── Key Vault ─────────────────────────────────────────────────────────────────

resource "azurerm_key_vault" "main" {
  name                      = "${var.prefix}-kv-${var.environment}"
  location                  = azurerm_resource_group.main.location
  resource_group_name       = azurerm_resource_group.main.name
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  tags = local.common_tags
}

# ── Azure Data Factory ────────────────────────────────────────────────────────

resource "azurerm_data_factory" "main" {
  name                = "${var.prefix}-adf-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  github_configuration {
    account_name    = var.github_account
    branch_name     = var.environment == "prod" ? "main" : var.environment
    git_url         = "https://github.com"
    repository_name = var.github_repo
    root_folder     = "/adf"
  }

  tags = local.common_tags
}

# Grant ADF managed identity access to ADLS
resource "azurerm_role_assignment" "adf_adls" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ── Azure Databricks ──────────────────────────────────────────────────────────

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.prefix}-dbw-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.environment == "prod" ? "premium" : "standard"   # Premium for Unity Catalog

  tags = local.common_tags
}

# ── Azure Synapse Analytics ───────────────────────────────────────────────────

resource "azurerm_synapse_workspace" "main" {
  name                                 = "${var.prefix}-syn-${var.environment}"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.lakehouse.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = data.azurerm_key_vault_secret.synapse_admin_pwd.value

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

resource "azurerm_synapse_sql_pool" "gold" {
  name                 = "goldpool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = var.environment == "prod" ? "DW200c" : "DW100c"
  create_mode          = "Default"

  tags = local.common_tags
}

# ── Azure Event Hubs ──────────────────────────────────────────────────────────

resource "azurerm_eventhub_namespace" "iot" {
  name                = "${var.prefix}-evhns-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 2

  tags = local.common_tags
}

resource "azurerm_eventhub" "sensor_events" {
  name                = "sensor-events"
  namespace_name      = azurerm_eventhub_namespace.iot.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 8     # Scaled for expected IoT throughput
  message_retention   = 3     # Days of event retention for replay
}

# ── Monitor + Alerting ────────────────────────────────────────────────────────

resource "azurerm_monitor_action_group" "pipeline_alerts" {
  name                = "${var.prefix}-ag-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "pipelinealrt"

  email_receiver {
    name                    = "DataEngineering"
    email_address           = var.alert_email
    use_common_alert_schema = true
  }
}

resource "azurerm_monitor_metric_alert" "adf_pipeline_failed" {
  name                = "adf-pipeline-failure-alert"
  resource_group_name = azurerm_resource_group.main.name
  scopes              = [azurerm_data_factory.main.id]
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }
}

# ── Locals ────────────────────────────────────────────────────────────────────

locals {
  common_tags = {
    Project     = "iot-lakehouse-pipeline"
    Environment = var.environment
    Owner       = "data-engineering"
    ManagedBy   = "terraform"
    Repository  = "github.com/Neeraj600/iot-lakehouse-pipeline"
  }
}

data "azurerm_client_config" "current" {}
