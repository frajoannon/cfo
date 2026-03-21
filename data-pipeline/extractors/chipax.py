"""
Extractor de Chipax
-------------------
Extrae movimientos bancarios y documentos desde la API de Chipax.

NOTA: Chipax tiene una API REST privada. Si no tienes acceso a la API,
contacta a soporte de Chipax (soporte@chipax.com) para solicitar credenciales.
Como alternativa, este módulo también soporta carga desde CSV exportado manualmente.
"""

import os
import csv
import logging
from datetime import datetime
from io import StringIO
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CHIPAX_API_BASE = "https://api.chipax.com/v1"


class ChipaxExtractor:
    """Extrae movimientos bancarios desde Chipax via API o CSV."""

    def __init__(self, api_key: str):
        """
        Args:
            api_key: API key de Chipax (solicitada al equipo de soporte)
        """
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def _get(self, endpoint: str, params: dict = None):
        """Hace un GET autenticado a la API de Chipax."""
        response = requests.get(
            f"{CHIPAX_API_BASE}{endpoint}",
            headers=self.headers,
            params=params or {},
        )
        response.raise_for_status()
        return response.json()

    def get_bank_movements(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        Extrae movimientos bancarios.

        Args:
            since: Fecha inicio del período (inclusive)
            until: Fecha fin del período (inclusive). Por defecto: hoy.

        Returns:
            Lista de movimientos como dicts.
        """
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")

        logger.info(f"Extrayendo movimientos de Chipax desde {since or 'el inicio'}...")
        try:
            data = self._get("/movimientos", params=params)
            movements = data if isinstance(data, list) else data.get("data", [])
            logger.info(f"  → {len(movements)} movimientos extraídos.")
            return movements
        except requests.HTTPError as e:
            logger.error(f"Error al consultar Chipax API: {e}")
            raise

    def get_accounts(self) -> List[Dict]:
        """Extrae las cuentas bancarias registradas en Chipax."""
        logger.info("Extrayendo cuentas bancarias de Chipax...")
        data = self._get("/cuentas")
        accounts = data if isinstance(data, list) else data.get("data", [])
        logger.info(f"  → {len(accounts)} cuentas extraídas.")
        return accounts


class ChipaxCSVExtractor:
    """
    Alternativa: carga movimientos desde un archivo CSV exportado manualmente desde Chipax.
    Úsalo si no tienes acceso a la API de Chipax.
    """

    def load_from_file(self, filepath: str) -> List[Dict]:
        """
        Carga movimientos desde un archivo CSV exportado de Chipax.

        Args:
            filepath: Ruta al archivo CSV

        Returns:
            Lista de movimientos como dicts.
        """
        logger.info(f"Cargando movimientos desde CSV: {filepath}")
        rows = []
        with open(filepath, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(self._normalize_row(row))
        logger.info(f"  → {len(rows)} movimientos cargados desde CSV.")
        return rows

    def _normalize_row(self, row: dict) -> dict:
        """Normaliza los nombres de columnas del CSV de Chipax al formato estándar."""
        return {
            "id": row.get("ID", ""),
            "fecha": row.get("Fecha", ""),
            "descripcion": row.get("Descripción", row.get("Descripcion", "")),
            "monto": self._parse_amount(row.get("Monto", "0")),
            "tipo": row.get("Tipo", ""),
            "cuenta": row.get("Cuenta", ""),
            "categoria": row.get("Categoría", row.get("Categoria", "")),
        }

    def _parse_amount(self, value: str) -> float:
        """Convierte string de monto (ej: '$ 1.234,56') a float."""
        cleaned = value.replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0


def create_from_env() -> ChipaxExtractor:
    """Crea un extractor leyendo credenciales desde variables de entorno."""
    return ChipaxExtractor(api_key=os.environ["CHIPAX_API_KEY"])
