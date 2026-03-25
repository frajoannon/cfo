"""
Pipeline ETL — CFO as a Service
---------------------------------
Punto de entrada principal. Orquesta la extracción desde todas las fuentes
y la carga en BigQuery.

Uso local:
    python main.py

En producción (GitHub Actions):
    Se ejecuta este mismo archivo disparado diariamente por el scheduler de GitHub.
"""

import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

from extractors.shopify import create_from_env as shopify_extractor
from extractors.mercadolibre import create_from_env as ml_extractor
from extractors.chipax import create_from_env as chipax_extractor
from loaders.bigquery import BigQueryLoader

# Cargar variables de entorno desde .env (solo en local; en producción vienen del entorno)
load_dotenv()

# Si GOOGLE_APPLICATION_CREDENTIALS es una ruta relativa, convertirla a absoluta
_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
if _creds and not os.path.isabs(_creds):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), _creds)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_pipeline(
    project_id: str,
    dataset_id: str,
    skip_shopify: bool = False,
    skip_mercadolibre: bool = False,
    skip_chipax: bool = False,
):
    """
    Ejecuta el pipeline completo: extrae de todas las fuentes y carga en BigQuery.

    Args:
        project_id: ID del proyecto de Google Cloud
        dataset_id: ID del dataset en BigQuery (ej: 'raw_cliente1')
        skip_*: Permite saltar una fuente específica (útil para debugging)
    """
    loader = BigQueryLoader(project_id=project_id, dataset_id=dataset_id)
    results = {}

    # ── Shopify ──────────────────────────────────────────────────────────────
    if not skip_shopify:
        logger.info("=" * 50)
        logger.info("SHOPIFY")
        logger.info("=" * 50)
        try:
            shopify = shopify_extractor()

            # Carga incremental: desde el último registro en BigQuery
            last_order_date = loader.get_last_loaded_at("shopify_orders")

            orders = shopify.get_orders(since=last_order_date)
            results["shopify_orders"] = loader.upsert("shopify_orders", orders, id_field="id")

            # Productos: carga completa (catálogo pequeño)
            products = shopify.get_products()
            results["shopify_products"] = loader.upsert("shopify_products", products, id_field="id")

        except Exception as e:
            logger.error(f"Error en Shopify: {e}", exc_info=True)
            results["shopify_error"] = str(e)

    # ── Mercado Libre ─────────────────────────────────────────────────────────
    if not skip_mercadolibre:
        logger.info("=" * 50)
        logger.info("MERCADO LIBRE")
        logger.info("=" * 50)
        try:
            ml = ml_extractor()

            last_ml_date = loader.get_last_loaded_at("ml_orders")

            orders = ml.get_orders(since=last_ml_date)
            results["ml_orders"] = loader.upsert("ml_orders", orders, id_field="id")

            items = ml.get_active_items()
            results["ml_items"] = loader.upsert("ml_items", items, id_field="id")

        except Exception as e:
            logger.error(f"Error en Mercado Libre: {e}", exc_info=True)
            results["ml_error"] = str(e)

    # ── Chipax ────────────────────────────────────────────────────────────────
    if not skip_chipax:
        logger.info("=" * 50)
        logger.info("CHIPAX")
        logger.info("=" * 50)
        try:
            chipax = chipax_extractor()

            # Tablas con fecha (carga incremental)
            chipax_incremental = [
                ("chipax_movimientos",      chipax.get_movimientos),
                ("chipax_cartolas",         chipax.get_cartolas),
                ("chipax_compras",          chipax.get_compras),
                ("chipax_dtes",             chipax.get_dtes),
                ("chipax_gastos",           chipax.get_gastos),
                ("chipax_remuneraciones",   chipax.get_remuneraciones),
                ("chipax_honorarios",       chipax.get_honorarios),
            ]
            for table_name, fn in chipax_incremental:
                try:
                    since = loader.get_last_loaded_at(table_name)
                    rows = fn(since=since)
                    results[table_name] = loader.upsert(table_name, rows, id_field="id")
                except Exception as e:
                    logger.error(f"Error en {table_name}: {e}", exc_info=True)
                    results[f"{table_name}_error"] = str(e)

            # Tablas sin fecha (carga completa)
            chipax_full = [
                ("chipax_cuentas",              chipax.get_cuentas),
                ("chipax_cuentas_corrientes",   chipax.get_cuentas_corrientes),
            ]
            for table_name, fn in chipax_full:
                try:
                    rows = fn()
                    results[table_name] = loader.upsert(table_name, rows, id_field="id")
                except Exception as e:
                    logger.error(f"Error en {table_name}: {e}", exc_info=True)
                    results[f"{table_name}_error"] = str(e)

        except Exception as e:
            logger.error(f"Error en Chipax: {e}", exc_info=True)
            results["chipax_error"] = str(e)

    # ── Resumen ───────────────────────────────────────────────────────────────
    logger.info("=" * 50)
    logger.info("RESUMEN DEL PIPELINE")
    logger.info("=" * 50)
    errors = {k: v for k, v in results.items() if "error" in k}
    successes = {k: v for k, v in results.items() if "error" not in k}

    for table, count in successes.items():
        logger.info(f"  ✓ {table}: {count} registros cargados")

    if errors:
        for source, msg in errors.items():
            logger.error(f"  ✗ {source}: {msg}")
        # Si hay errores, salimos con código de error para que Cloud Run lo detecte
        sys.exit(1)

    logger.info("Pipeline completado exitosamente.")


if __name__ == "__main__":
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "raw_cliente1")

    if not project_id:
        logger.error("Variable de entorno GCP_PROJECT_ID no definida.")
        sys.exit(1)

    # Saltar fuentes sin credenciales configuradas
    skip_ml = not os.environ.get("ML_CLIENT_ID") or os.environ.get("ML_CLIENT_ID") == "1234567890"
    skip_chipax = not os.environ.get("CHIPAX_APP_ID") or os.environ.get("CHIPAX_APP_ID").startswith("xxx")

    run_pipeline(
        project_id=project_id,
        dataset_id=dataset_id,
        skip_mercadolibre=skip_ml,
        skip_chipax=skip_chipax,
    )
