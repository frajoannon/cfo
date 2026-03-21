"""
Script de prueba — Conexión con Shopify
Verifica que las credenciales son correctas y trae las últimas 5 órdenes.
"""

from dotenv import load_dotenv
load_dotenv()

from extractors.shopify import create_from_env

print("Conectando con Shopify...")
shopify = create_from_env()

print("Obteniendo las últimas 5 órdenes...")
from datetime import datetime, timedelta
since = datetime.now() - timedelta(days=30)
orders = shopify.get_orders(since=since)
orders_sample = orders[:5]

if not orders:
    print("⚠️  No se encontraron órdenes (la tienda puede estar vacía).")
else:
    print(f"\n✓ Conexión exitosa. Total de órdenes: {len(orders)}")
    print("\nÚltimas 5 órdenes:")
    for o in orders_sample:
        print(f"  #{o.get('order_number')} | {o.get('created_at', '')[:10]} | "
              f"{o.get('total_price')} {o.get('currency')} | {o.get('financial_status')}")
