# code/Dockerfile
FROM python:3.10-alpine

# RUN apk add --no-cache sqlite3 jq

# Instalar dependencias del sistema incluyendo ffmpeg
RUN apk update && apk add --no-cache ffmpeg

WORKDIR /app

# Copiar requirements e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del proyecto
COPY . .

EXPOSE 8000

# Ejecutar migraciones y luego iniciar el servidor
CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8000"]
