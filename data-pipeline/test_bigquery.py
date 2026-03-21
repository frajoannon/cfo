"""
Script de prueba — Conexión con BigQuery
Verifica credenciales, crea el dataset si no existe, y carga 3 órdenes de Shopify.
"""

import os
from dotenv import load_dotenv
load_dotenv()

# Apuntar las credenciales al archivo JSON
credentials_path = os.path.join(os.path.dirname(__file__), os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

from loaders.bigquery import BigQueryLoader
from extractors.shopify import create_from_env as shopify_extractor
from datetime import datetime, timedelta

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BQ_DATASET_ID"]

print(f"Conectando a BigQuery: {project_id}.{dataset_id}")
loader = BigQueryLoader(project_id=project_id, dataset_id=dataset_id)
print("✓ Conexión a BigQuery exitosa")

print("\nObteniendo 3 órdenes de Shopify para prueba...")
shopify = shopify_extractor()
since = datetime.now() - timedelta(days=3)
orders = shopify.get_orders(since=since)[:3]
print(f"  → {len(orders)} órdenes obtenidas")

print("\nCargando en BigQuery (tabla: shopify_orders)...")
count = loader.upsert("shopify_orders", orders, id_field="id")
print(f"✓ {count} registros cargados en BigQuery")
print(f"\nVerifica en: https://console.cloud.google.com/bigquery?project={project_id}")
