# ui/Dockerfile
FROM node:24-alpine

WORKDIR /app

# Copiar package.json primero para cache
COPY package*.json ./
RUN npm install

EXPOSE 5173  # Puerto por defecto de Vite

# Para desarrollo con Vite y hot-reload
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]