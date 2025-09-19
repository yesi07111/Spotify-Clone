# 🎵 Spotify Clone - Frontend

## 📋 Descripción del Proyecto

Este proyecto es un clon de Spotify desarrollado con Vue.js 3, que simula las funcionalidades principales de un reproductor de música como reproducción de canciones, búsqueda, control de volumen y gestión de listas de reproducción. Por ahora es centralizado.

## ✨ Funcionalidades (a implementar en su mayoria)
- 🎵 Reproducción de audio con controles básicos (play/pause)
- 🔄 Funcionalidad de shuffle y repeat
- 🔊 Control de volumen
- 🔍 Búsqueda de canciones
- 🎯 Navegación entre canciones
- 📱 Interfaz responsive con diseño moderno
- 🖼️ Sistema "inteligente" (es aleatorio) de gestión de imágenes, para bonito.


## 🛠️ Requisitos del Sistema

### Prerrequisitos
- **Node.js** versión 16.0.0 o superior
- **npm** versión 8.0.0 o superior
- **Vue CLI** (opcional)

### Verificar instalaciones
```bash
node --version
npm --version
```

## ⚙️ Instalación y Configuración

### 1. Clonar el repositorio
```bash
git clone https://github.com/yesi07111/Spotify-Clone.git
cd Spotify_Clone/ui
```

### 2. Instalar dependencias
```bash
npm install
```

## 🚀 Ejecución del Proyecto

### Modo desarrollo
```bash
npm run dev
```
La aplicación estará disponible en `http://localhost:8080`

### Compilación para producción
```bash
npm run build
```

### Preview de producción
```bash
npm run preview
```

### Linter (verificación de código)
```bash
npm run lint
```

## 🎨 Estructura del Proyecto

```
src/
├── assets/
│   └── styles/          # Estilos CSS organizados
├── components/
│   ├── layout/          # Componentes de layout
│   ├── player/          # Componentes del reproductor
│   └── tracks/          # Componentes de listas de canciones
├── pages/               # Vistas/páginas
├── routing/             # Configuración de rutas 
├── services/            # Servicios (API, audio)
├── store/               # Gestión de estado (Vuex)
├── utils/               # Utilidades y helpers
├── App.vue
└── main.js
```
## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.
