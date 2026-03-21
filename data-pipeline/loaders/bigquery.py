"""
Loader de BigQuery
------------------
Inserta registros en tablas de BigQuery de forma incremental.
Crea las tablas si no existen. Evita duplicados usando MERGE.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


# Schemas de las tablas (tipos de BigQuery)
# Cada tabla tiene la columna _loaded_at para saber cuándo fue insertada.

SCHEMAS = {
    "shopify_orders": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("financial_status", "STRING"),
        bigquery.SchemaField("fulfillment_status", "STRING"),
        bigquery.SchemaField("total_price", "FLOAT"),
        bigquery.SchemaField("subtotal_price", "FLOAT"),
        bigquery.SchemaField("total_tax", "FLOAT"),
        bigquery.SchemaField("total_discounts", "FLOAT"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("customer_email", "STRING"),
        bigquery.SchemaField("raw_json", "JSON"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "shopify_products": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("raw_json", "JSON"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "ml_orders": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_created", "TIMESTAMP"),
        bigquery.SchemaField("date_closed", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("currency_id", "STRING"),
        bigquery.SchemaField("buyer_id", "STRING"),
        bigquery.SchemaField("raw_json", "JSON"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "ml_items": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category_id", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("available_quantity", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("raw_json", "JSON"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "chipax_movements": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("fecha", "DATE"),
        bigquery.SchemaField("descripcion", "STRING"),
        bigquery.SchemaField("monto", "FLOAT"),
        bigquery.SchemaField("tipo", "STRING"),
        bigquery.SchemaField("cuenta", "STRING"),
        bigquery.SchemaField("categoria", "STRING"),
        bigquery.SchemaField("raw_json", "JSON"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
}


class BigQueryLoader:
    """Carga datos en tablas de BigQuery."""

    def __init__(self, project_id: str, dataset_id: str):
        """
        Args:
            project_id: ID del proyecto de Google Cloud
            dataset_id: ID del dataset en BigQuery, ej: 'raw_cliente1'
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self._ensure_dataset()

    def _ensure_dataset(self):
        """Crea el dataset si no existe."""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            logger.info(f"Creando dataset {dataset_ref}...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            logger.info(f"  → Dataset creado.")

    def _ensure_table(self, table_name: str):
        """Crea la tabla si no existe usando el schema definido."""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            self.client.get_table(table_ref)
        except NotFound:
            schema = SCHEMAS.get(table_name)
            if not schema:
                raise ValueError(f"No hay schema definido para la tabla '{table_name}'.")
            logger.info(f"Creando tabla {table_ref}...")
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"  → Tabla creada.")

    def _serialize_record(self, record: dict, id_field: str) -> dict:
        """
        Prepara un registro para insertar en BigQuery:
        - Agrega _loaded_at
        - Convierte el registro completo a JSON para la columna raw_json
        - Extrae los campos estructurados clave
        """
        import json

        loaded_at = datetime.now(timezone.utc).isoformat()

        return {
            "id": str(record.get(id_field, "")),
            **self._extract_fields(record),
            "raw_json": json.dumps(record, default=str),
            "_loaded_at": loaded_at,
        }

    def _extract_fields(self, record: dict) -> dict:
        """Extrae campos comunes de un registro."""
        fields = {}
        for key in [
            "created_at", "updated_at", "date_created", "date_closed",
            "financial_status", "fulfillment_status", "total_price",
            "subtotal_price", "total_tax", "total_discounts", "currency",
            "status", "total_amount", "currency_id", "title", "product_type",
            "category_id", "price", "available_quantity",
            "fecha", "descripcion", "monto", "tipo", "cuenta", "categoria",
        ]:
            if key in record:
                fields[key] = record[key]

        # Campos anidados frecuentes
        if "customer" in record and record["customer"]:
            fields["customer_email"] = record["customer"].get("email", "")
        if "buyer" in record and record["buyer"]:
            fields["buyer_id"] = str(record["buyer"].get("id", ""))

        return fields

    def upsert(self, table_name: str, records: List[Dict], id_field: str = "id") -> int:
        """
        Inserta o actualiza registros en BigQuery.
        Usa una tabla temporal + MERGE para evitar duplicados.

        Args:
            table_name: Nombre de la tabla destino (ej: 'shopify_orders')
            records: Lista de registros a insertar
            id_field: Campo que identifica únicamente cada registro

        Returns:
            Número de registros procesados.
        """
        if not records:
            logger.info(f"  → No hay registros nuevos para {table_name}.")
            return 0

        self._ensure_table(table_name)

        serialized = [self._serialize_record(r, id_field) for r in records]
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"

        # Usamos load job (compatible con plan gratuito de BigQuery)
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMAS[table_name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        import json as _json
        ndjson = "\n".join(_json.dumps(r, default=str) for r in serialized)
        import io
        job = self.client.load_table_from_file(
            io.BytesIO(ndjson.encode("utf-8")),
            table_ref,
            job_config=job_config,
        )
        job.result()  # Espera a que termine

        if job.errors:
            logger.error(f"Errores al cargar en {table_name}: {job.errors[:3]}")
            raise RuntimeError(f"Error de carga en BigQuery: {job.errors[:1]}")

        total_inserted = len(serialized)
        logger.info(f"  → {total_inserted} registros cargados en {table_name}.")
        return total_inserted

    def get_last_loaded_at(self, table_name: str) -> Optional[datetime]:
        """
        Obtiene la fecha del último registro cargado en una tabla.
        Se usa para la carga incremental (traer solo datos nuevos).

        Returns:
            datetime del último registro, o None si la tabla está vacía.
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            self.client.get_table(table_ref)
        except NotFound:
            return None

        query = f"SELECT MAX(_loaded_at) as last_loaded FROM `{table_ref}`"
        result = list(self.client.query(query).result())
        if result and result[0].last_loaded:
            return result[0].last_loaded
        return None
