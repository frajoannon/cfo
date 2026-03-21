# Setup del Pipeline de Datos

Guía paso a paso para poner en marcha el pipeline por primera vez.

---

## 1. Prerequisitos

- Cuenta de Google Cloud (gratis para empezar: cloud.google.com)
- Python 3.12 instalado localmente
- Docker instalado (para el deploy)

---

## 2. Obtener credenciales

### Shopify
1. Ve a tu Shopify Admin → Settings → Apps and sales channels → Develop apps
2. Crea una nueva app → "Allow custom app development"
3. En "API credentials" → "Configure Admin API scopes"
4. Activa: `read_orders`, `read_products`, `read_customers`
5. Instala la app → copia el **Admin API access token** (empieza con `shpat_`)

### Mercado Libre
1. Ve a https://developers.mercadolibre.com → Tus apps → Crear app
2. En "Redirect URI" pon: `https://localhost`
3. Copia `client_id` y `client_secret`
4. Para obtener el **refresh_token**, sigue el flujo OAuth:
   - Abre en el navegador:
     `https://auth.mercadolibre.cl/authorization?response_type=code&client_id=TU_CLIENT_ID&redirect_uri=https://localhost`
   - Autoriza la app → te redirige a `https://localhost?code=XXXXX`
   - Copia ese `code` y haz este request (reemplaza los valores):
     ```
     POST https://api.mercadolibre.com/oauth/token
     grant_type=authorization_code
     client_id=TU_CLIENT_ID
     client_secret=TU_CLIENT_SECRET
     code=EL_CODE_QUE_COPIASTE
     redirect_uri=https://localhost
     ```
   - La respuesta incluye `access_token` y `refresh_token` → guarda el **refresh_token**

### Chipax
- Contacta a soporte@chipax.com solicitando acceso a la API
- Menciona que necesitas la API key para integraciones
- Si no está disponible, usa la alternativa de exportar CSV manualmente

---

## 3. Setup local (para pruebas)

```bash
# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Crear archivo .env con tus credenciales
cp .env.example .env
# Edita .env y completa todos los valores

# Autenticar con Google Cloud localmente
gcloud auth application-default login

# Probar el pipeline
python main.py
```

---

## 4. Verificar en BigQuery

Una vez corrido el pipeline, verifica los datos en BigQuery:

1. Ve a https://console.cloud.google.com/bigquery
2. Busca el dataset `raw_cliente1`
3. Ejecuta:
   ```sql
   SELECT COUNT(*) FROM `tu-proyecto.raw_cliente1.shopify_orders`
   SELECT COUNT(*) FROM `tu-proyecto.raw_cliente1.ml_orders`
   SELECT COUNT(*) FROM `tu-proyecto.raw_cliente1.chipax_movements`
   ```

---

## 5. Deploy en Google Cloud

### Construir y subir la imagen Docker

```bash
# Configurar variables
PROJECT_ID="tu-proyecto-gcp"
REGION="us-central1"
IMAGE="gcr.io/$PROJECT_ID/cfo-pipeline"

# Build y push
docker build -t $IMAGE .
docker push $IMAGE
```

### Crear el Cloud Run Job

```bash
gcloud run jobs create cfo-pipeline \
  --image $IMAGE \
  --region $REGION \
  --set-env-vars GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET_ID=raw_cliente1 \
  --set-secrets SHOPIFY_ACCESS_TOKEN=shopify-token:latest \
  --set-secrets ML_CLIENT_ID=ml-client-id:latest \
  --set-secrets ML_CLIENT_SECRET=ml-client-secret:latest \
  --set-secrets ML_REFRESH_TOKEN=ml-refresh-token:latest \
  --set-secrets CHIPAX_API_KEY=chipax-api-key:latest \
  --set-secrets SHOPIFY_SHOP_DOMAIN=shopify-domain:latest
```

### Configurar el scheduler diario (06:00 AM Chile)

```bash
gcloud scheduler jobs create http cfo-pipeline-daily \
  --schedule="0 9 * * *" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/cfo-pipeline:run" \
  --http-method=POST \
  --oauth-service-account-email=tu-service-account@$PROJECT_ID.iam.gserviceaccount.com \
  --time-zone="America/Santiago"
```

---

## 6. Guardar credenciales en Secret Manager

```bash
# Ejemplo para Shopify token
echo -n "shpat_xxxxx" | gcloud secrets create shopify-token --data-file=-

# Hacer lo mismo para cada credencial:
# ml-client-id, ml-client-secret, ml-refresh-token, chipax-api-key, shopify-domain
```
