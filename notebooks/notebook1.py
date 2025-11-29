# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env")

print(f"Hello from env = {env}")
