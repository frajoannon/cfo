"""
Extractor de Mercado Libre
--------------------------
Extrae órdenes y publicaciones desde la API oficial de Mercado Libre.
Maneja automáticamente la renovación del access_token usando el refresh_token.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Generator, List, Optional

import requests

logger = logging.getLogger(__name__)

ML_API_BASE = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://api.mercadolibre.com/oauth/token"


class MercadoLibreExtractor:
    """Extrae datos de una cuenta de Mercado Libre."""

    PAGE_SIZE = 50  # ML permite hasta 50 por request en órdenes

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        """
        Args:
            client_id: App ID de la aplicación en ML Developers
            client_secret: Secret key de la aplicación en ML Developers
            refresh_token: Refresh token obtenido en el flujo OAuth inicial
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._access_token: Optional[str] = None
        self._seller_id: Optional[str] = None

    def _refresh_access_token(self) -> str:
        """Obtiene un nuevo access_token usando el refresh_token."""
        logger.info("Renovando access_token de Mercado Libre...")
        response = requests.post(
            ML_AUTH_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
        )
        response.raise_for_status()
        token_data = response.json()
        self._access_token = token_data["access_token"]
        # ML puede rotar el refresh_token; lo actualizamos
        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]
            logger.info("  → Refresh token actualizado. Guarda el nuevo valor en Secret Manager.")
        return self._access_token

    @property
    def access_token(self) -> str:
        if not self._access_token:
            self._refresh_access_token()
        return self._access_token

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Hace un GET autenticado. Reintenta una vez si el token expiró."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(f"{ML_API_BASE}{endpoint}", headers=headers, params=params or {})

        if response.status_code == 401:
            # Token expirado → renovar y reintentar
            self._access_token = None
            headers["Authorization"] = f"Bearer {self.access_token}"
            response = requests.get(f"{ML_API_BASE}{endpoint}", headers=headers, params=params or {})

        response.raise_for_status()
        return response.json()

    def get_seller_id(self) -> str:
        """Obtiene el ID del vendedor autenticado."""
        if not self._seller_id:
            data = self._get("/users/me")
            self._seller_id = str(data["id"])
            logger.info(f"  → Seller ID: {self._seller_id} ({data.get('nickname', '')})")
        return self._seller_id

    def _get_orders_paginated(self, params: dict) -> Generator[dict, None, None]:
        """Itera sobre todas las páginas de búsqueda de órdenes."""
        seller_id = self.get_seller_id()
        params["offset"] = 0
        params["limit"] = self.PAGE_SIZE

        while True:
            data = self._get(f"/orders/search", params={**params, "seller": seller_id})
            results = data.get("results", [])

            for order in results:
                yield order

            total = data.get("paging", {}).get("total", 0)
            current_offset = params["offset"] + len(results)
            if current_offset >= total or not results:
                break

            params["offset"] = current_offset

    def get_orders(self, since: Optional[datetime] = None) -> List[Dict]:
        """
        Extrae todas las órdenes del vendedor.

        Args:
            since: Solo trae órdenes creadas después de esta fecha.

        Returns:
            Lista de órdenes como dicts.
        """
        params = {"sort": "date_asc"}
        if since:
            # ML filtra por rango de fechas en el campo date_created
            params["q"] = f"date_created:[{since.strftime('%Y-%m-%dT%H:%M:%S.000-00:00')} TO *]"

        logger.info(f"Extrayendo órdenes de Mercado Libre desde {since or 'el inicio'}...")
        orders = list(self._get_orders_paginated(params))
        logger.info(f"  → {len(orders)} órdenes extraídas.")
        return orders

    def get_active_items(self) -> List[Dict]:
        """
        Extrae las publicaciones activas del vendedor.

        Returns:
            Lista de items con sus detalles.
        """
        seller_id = self.get_seller_id()
        logger.info("Extrayendo publicaciones activas de Mercado Libre...")

        # Primero obtenemos los IDs
        offset = 0
        all_ids = []
        while True:
            data = self._get(
                f"/users/{seller_id}/items/search",
                params={"status": "active", "offset": offset, "limit": 100},
            )
            ids = data.get("results", [])
            all_ids.extend(ids)
            if len(ids) < 100:
                break
            offset += len(ids)

        if not all_ids:
            logger.info("  → No hay publicaciones activas.")
            return []

        # Luego buscamos detalles en lotes de 20 (límite de ML multiget)
        items = []
        for i in range(0, len(all_ids), 20):
            batch = all_ids[i : i + 20]
            ids_param = ",".join(batch)
            data = self._get(f"/items", params={"ids": ids_param})
            for entry in data:
                if entry.get("code") == 200:
                    items.append(entry["body"])

        logger.info(f"  → {len(items)} publicaciones extraídas.")
        return items


def create_from_env() -> MercadoLibreExtractor:
    """Crea un extractor leyendo credenciales desde variables de entorno."""
    return MercadoLibreExtractor(
        client_id=os.environ["ML_CLIENT_ID"],
        client_secret=os.environ["ML_CLIENT_SECRET"],
        refresh_token=os.environ["ML_REFRESH_TOKEN"],
    )
