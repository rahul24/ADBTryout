// Databricks notebook source

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "91141215-e8bf-4c3e-b508-d5f04be24489",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "AADScope", key = "ADDSecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/153fe5d0-5dce-481e-b52b-320e42f55ca8/oauth2/token")


dbutils.fs.mount(
  source = "abfss://output@azuredatastorelake.dfs.core.windows.net/",
  mountPoint = "/mnt/output",
  extraConfigs = configs)


// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/output

// COMMAND ----------

