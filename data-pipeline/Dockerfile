# Imagen base liviana de Python
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias primero (para aprovechar cache de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Cloud Run Jobs ejecuta el contenedor y espera que termine (exit 0 = éxito)
CMD ["python", "main.py"]
