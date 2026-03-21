"""
Carga histórica inicial — Solo se corre UNA vez.
Baja todas las órdenes y productos históricos de Shopify a BigQuery.
"""

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-credentials.json"

from dotenv import load_dotenv
load_dotenv()

from extractors.shopify import create_from_env as shopify_extractor
from loaders.bigquery import BigQueryLoader
import logging, sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])

project_id = os.environ["GCP_PROJECT_ID"]
dataset_id = os.environ["BQ_DATASET_ID"]

loader = BigQueryLoader(project_id=project_id, dataset_id=dataset_id)
shopify = shopify_extractor()

# Órdenes — sin filtro de fecha = todo el historial
print("Descargando TODAS las órdenes históricas (puede tardar varios minutos)...")
orders = shopify.get_orders(since=None)
print(f"Total órdenes: {len(orders)}")

# Truncar tabla antes de cargar para evitar duplicados
from google.cloud import bigquery
client = bigquery.Client(project=project_id)
table_ref = f"{project_id}.{dataset_id}.shopify_orders"
try:
    client.query(f"TRUNCATE TABLE `{table_ref}`").result()
    print("Tabla shopify_orders limpiada.")
except Exception:
    pass  # Si no existe aún, no hay problema

count = loader.upsert("shopify_orders", orders, id_field="id")
print(f"✓ {count} órdenes cargadas en BigQuery")

# Productos
print("\nDescargando productos...")
products = shopify.get_products()
table_ref_p = f"{project_id}.{dataset_id}.shopify_products"
try:
    client.query(f"TRUNCATE TABLE `{table_ref_p}`").result()
except Exception:
    pass
count_p = loader.upsert("shopify_products", products, id_field="id")
print(f"✓ {count_p} productos cargados en BigQuery")

print(f"\nCarga histórica completa.")
print(f"Ver datos: https://console.cloud.google.com/bigquery?project={project_id}")
