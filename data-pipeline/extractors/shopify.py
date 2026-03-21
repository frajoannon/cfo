"""
Extractor de Shopify
--------------------
Extrae órdenes y productos desde la Admin REST API de Shopify.
Carga incremental: solo trae datos desde la última fecha procesada.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional

import requests

logger = logging.getLogger(__name__)


class ShopifyExtractor:
    """Extrae datos de una tienda Shopify via Admin REST API."""

    API_VERSION = "2024-01"
    PAGE_LIMIT = 250  # máximo permitido por Shopify

    def __init__(self, shop_domain: str, access_token: str):
        """
        Args:
            shop_domain: Dominio de la tienda, ej: 'mi-tienda.myshopify.com'
            access_token: Token de acceso del custom app de Shopify
        """
        self.base_url = f"https://{shop_domain}/admin/api/{self.API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

    def _get_paginated(self, endpoint: str, params: dict) -> Generator[dict, None, None]:
        """Itera sobre todas las páginas de un endpoint usando cursor-based pagination."""
        url = f"{self.base_url}/{endpoint}"
        params["limit"] = self.PAGE_LIMIT

        while url:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Determinar qué clave tiene los datos (orders, products, etc.)
            resource_key = endpoint.split(".json")[0].split("/")[-1]
            records = data.get(resource_key, [])

            for record in records:
                yield record

            # Cursor-based pagination: buscar el link de siguiente página
            link_header = response.headers.get("Link", "")
            url = self._parse_next_link(link_header)
            params = {}  # Los params ya van en la URL del cursor

    def _parse_next_link(self, link_header: str) -> Optional[str]:
        """Extrae la URL de la siguiente página del header Link de Shopify."""
        if not link_header:
            return None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                return part.strip().split(";")[0].strip().strip("<>")
        return None

    def get_orders(self, since: Optional[datetime] = None) -> List[Dict]:
        """
        Extrae todas las órdenes.

        Args:
            since: Solo trae órdenes creadas después de esta fecha.
                   Si es None, trae todas las órdenes históricas.

        Returns:
            Lista de órdenes como dicts.
        """
        params = {"status": "any"}
        if since:
            params["created_at_min"] = since.isoformat()

        logger.info(f"Extrayendo órdenes de Shopify desde {since or 'el inicio'}...")
        orders = list(self._get_paginated("orders.json", params))
        logger.info(f"  → {len(orders)} órdenes extraídas.")
        return orders

    def get_products(self) -> List[Dict]:
        """
        Extrae el catálogo completo de productos.
        Se hace carga completa (no incremental) ya que el catálogo es pequeño.

        Returns:
            Lista de productos como dicts.
        """
        logger.info("Extrayendo productos de Shopify...")
        products = list(self._get_paginated("products.json", {}))
        logger.info(f"  → {len(products)} productos extraídos.")
        return products


def create_from_env() -> ShopifyExtractor:
    """Crea un extractor leyendo credenciales desde variables de entorno."""
    shop_domain = os.environ["SHOPIFY_SHOP_DOMAIN"]
    access_token = os.environ["SHOPIFY_ACCESS_TOKEN"]
    return ShopifyExtractor(shop_domain=shop_domain, access_token=access_token)
