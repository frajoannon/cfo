# Estado del Proyecto — Finatics ETL

*Última actualización: Marzo 2026*

---

## Infraestructura

| Componente | Herramienta | Detalle |
|---|---|---|
| Código fuente | GitHub | github.com/frajoannon/cfo-website |
| Ejecución del pipeline | GitHub Actions | Corre automático todos los días a las 6 AM Chile |
| Data warehouse | Google BigQuery | Proyecto: cfo-as-a-service-490915 |
| Visualización | Power BI / Looker Studio | Conectado a BigQuery vía importación |

---

## Clientes activos

### Circular Pet
- **Dataset en BigQuery:** `raw_circularpet`
- **Fuentes conectadas:** Shopify ✅ | Chipax ✅ | Mercado Libre ❌ (pendiente)

---

## Tablas en BigQuery (raw_circularpet)

| Tabla | Fuente | Registros aprox. | Notas |
|---|---|---|---|
| shopify_orders | Shopify | 15.571 | Historial completo cargado |
| shopify_products | Shopify | 144 | |
| shopify_orders_clean | Vista SQL | — | Vista limpia sobre shopify_orders |
| chipax_compras | Chipax | 1.472 | Facturas de compra |
| chipax_dtes | Chipax | 426 | Documentos tributarios |
| chipax_gastos | Chipax | 1.067 | Gastos registrados |
| chipax_remuneraciones | Chipax | 143 | Liquidaciones de sueldo |
| chipax_honorarios | Chipax | 108 | Boletas de honorarios |
| chipax_cuentas | Chipax | 58 | Cuentas contables |
| chipax_cuentas_corrientes | Chipax | 9 | Cuentas bancarias |
| chipax_movimientos | Chipax | 9 | Movimientos bancarios |

---

## Estructura del repositorio

```
cfo-website/
├── CONTEXTO_NEGOCIO.md
├── ESTADO_PROYECTO.md
├── index.html
└── data-pipeline/
    ├── main.py                  # Orquestador principal
    ├── requirements.txt         # Dependencias Python
    ├── .env                     # Credenciales locales (no en GitHub)
    ├── gcp-credentials.json     # Llave GCP local (no en GitHub)
    ├── extractors/
    │   ├── shopify.py           # Extractor Shopify
    │   ├── chipax.py            # Extractor Chipax
    │   └── mercadolibre.py      # Extractor ML (pendiente credenciales)
    ├── loaders/
    │   └── bigquery.py          # Carga a BigQuery
    └── docs/
        └── guia-conexiones.md   # Guía para onboardear nuevos clientes
```

---

## GitHub Secrets configurados

| Secret | Descripción |
|---|---|
| GCP_PROJECT_ID | cfo-as-a-service-490915 |
| GCP_CREDENTIALS_JSON | Contenido del archivo gcp-credentials.json |
| BQ_DATASET_ID | raw_circularpet |
| SHOPIFY_SHOP_DOMAIN | Dominio de Circular Pet |
| SHOPIFY_ACCESS_TOKEN | Token de acceso Shopify |
| CHIPAX_APP_ID | ID de aplicación Chipax |
| CHIPAX_SECRET_KEY | Clave secreta Chipax |

---

## Pendiente

- [ ] Conectar Mercado Libre (falta conseguir credenciales)
- [ ] Conectar Chipax (falta acceso administrador — ya resuelto)
- [ ] Crear vistas SQL adicionales para limpieza de datos
- [ ] Armar dashboard en Looker Studio / Power BI
- [ ] Documentar proceso para dar acceso a equipo en BigQuery (roles IAM)

---

## Decisiones técnicas relevantes

- **Python 3.8** en local — genera warnings de Google SDK pero funciona. Pendiente actualizar a 3.10+
- **Chipax usa JWT** — el token se renueva en cada ejecución del pipeline
- **Paginación Chipax** — usa offset + deduplicación por id para evitar registros repetidos en última página
- **Shopify usa cursor-based pagination** — sigue el header Link hasta agotar resultados
- **BigQuery WRITE_TRUNCATE** en carga histórica, **WRITE_APPEND** en carga incremental
- **Dataset nombrado por cliente** (raw_circularpet) para escalar a múltiples clientes fácilmente
