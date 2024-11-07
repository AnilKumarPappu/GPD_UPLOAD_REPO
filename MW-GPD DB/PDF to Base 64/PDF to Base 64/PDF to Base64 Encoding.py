# Databricks notebook source
import base64
pdf_path = '/Workspace/Shared/MW - GPD/PDF to Base 64/GPD- TL User Guide_Oct 2024.pptx'


# COMMAND ----------


def pdf_to_base64(pdf_path):
  """
  Converts a PDF file to base64 encoding.

  Args:
    pdf_path: Path to the PDF file.

  Returns:
    Base64 encoded string of the PDF file.
  """
  with open(pdf_path, "rb") as pdf_file:
    pdf_data = pdf_file.read()
    encoded_string = base64.b64encode(pdf_data).decode('utf-8')
  return encoded_string

# COMMAND ----------

pdf_to_base64(pdf_path)