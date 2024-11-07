# Databricks notebook source
xlsx_file_path = "/Workspace/Shared/GLOBAL PERFORMANCE DASHBOARD (MW SNACKING)/Naming Convention QC/Source of truth (Naming-Convenison) 05-07-2024.xlsx"
campaign_codes = pd.read_excel(xlsx_file_path, sheet_name='CampaignAcceptedValues', dtype=str)