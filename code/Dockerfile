FROM python:3.10-alpine

# Instalar dependencias del sistema (Alpine usa 'apk')
RUN apk update && apk add --no-cache \
    ffmpeg \
    gcc \
    musl-dev \
    libffi-dev \
    make \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Solo copiar requirements primero para cache
COPY requirements.txt .

RUN pip install -r requirements.txt

# Ahora sí copiar el resto (pero ya reducido por el .dockerignore)
COPY . .

EXPOSE 8000

CMD ["sh","-c","python manage.py makemigrations && python manage.py migrate && python manage.py runserver 0.0.0.0:8000"]
