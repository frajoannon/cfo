"""
Extractor de Chipax
-------------------
Extrae datos financieros desde la API v2 de Chipax.

Autenticación: POST /login con app_id + secret_key → JWT token
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CHIPAX_API_BASE = "https://api.chipax.com/v2"


class ChipaxExtractor:
    """Extrae datos financieros desde Chipax via API v2."""

    def __init__(self, app_id: str, secret_key: str):
        self.app_id = app_id
        self.secret_key = secret_key
        self._token = None
        self._authenticate()

    def _authenticate(self):
        """Obtiene el JWT token haciendo POST a /login."""
        response = requests.post(
            f"{CHIPAX_API_BASE}/login",
            json={"app_id": self.app_id, "secret_key": self.secret_key},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        self._token = response.json()["token"]
        logger.info("✓ Autenticación Chipax exitosa.")

    def _get(self, endpoint: str, params: dict = None):
        """Hace un GET autenticado a la API de Chipax."""
        response = requests.get(
            f"{CHIPAX_API_BASE}{endpoint}",
            headers={
                "Authorization": f"JWT {self._token}",
                "Content-Type": "application/json",
            },
            params=params or {},
        )
        response.raise_for_status()
        return response.json()

    def _extract(self, endpoint: str, label: str, params: dict = None) -> List[Dict]:
        """Helper genérico: extrae una lista desde un endpoint."""
        logger.info(f"Extrayendo {label} desde Chipax...")
        try:
            data = self._get(endpoint, params=params)
            rows = data if isinstance(data, list) else data.get("data", data.get("items", []))
            logger.info(f"  → {len(rows)} {label} extraídos.")
            return rows
        except requests.HTTPError as e:
            logger.error(f"Error al consultar {endpoint}: {e}")
            raise

    # ── Endpoints ─────────────────────────────────────────────────────────────

    def get_movimientos(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Asientos contables / movimientos."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/movimientos", "movimientos", params)

    def get_cartolas(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Movimientos bancarios (cartolas)."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/flujo-caja/cartolas", "cartolas", params)

    def get_compras(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Facturas de proveedores / compras."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/compras", "compras", params)

    def get_dtes(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Documentos tributarios electrónicos (facturas emitidas, boletas, etc.)."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/dtes", "dtes", params)

    def get_gastos(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Gastos registrados."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/gastos", "gastos", params)

    def get_remuneraciones(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Remuneraciones / nómina."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/remuneraciones", "remuneraciones", params)

    def get_cuentas_corrientes(self) -> List[Dict]:
        """Saldos de cuentas bancarias."""
        return self._extract("/cuentas-corrientes", "cuentas corrientes")

    def get_cuentas(self) -> List[Dict]:
        """Plan de cuentas contables."""
        return self._extract("/cuentas", "cuentas")

    def get_honorarios(self, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict]:
        """Honorarios."""
        params = {}
        if since:
            params["fecha_inicio"] = since.strftime("%Y-%m-%d")
        if until:
            params["fecha_fin"] = until.strftime("%Y-%m-%d")
        return self._extract("/honorarios", "honorarios", params)


def create_from_env() -> ChipaxExtractor:
    """Crea un extractor leyendo credenciales desde variables de entorno."""
    return ChipaxExtractor(
        app_id=os.environ["CHIPAX_APP_ID"],
        secret_key=os.environ["CHIPAX_SECRET_KEY"],
    )
