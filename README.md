# 🎵 Spotify Clone

## 📋 Descripción del Proyecto

**Spotify Clone** es una aplicación web inspirada en Spotify, desarrollada con **Vue.js 3** para el frontend y **Django** como backend con **SQLite3** como base de datos.
El proyecto simula las funcionalidades principales de un reproductor de música moderno, incluyendo **reproducción de canciones, búsqueda, control de volumen, gestión de listas de reproducción** y un sistema estético de asignación aleatoria de imágenes para cada pista o álbum.

> 💡 **Nota:** El sistema actual es **centralizado** (no distribuido), ideal para entornos de desarrollo o pruebas locales.

---

## ✨ Funcionalidades Principales

* 🎵 **Reproducción de audio** con controles completos:
  * **Play / Pause** - Reproducir y pausar la canción actual
  * **Next / Previous** - Avanzar o retroceder entre canciones
  * **Seek** - Buscar posición específica dentro de la canción mediante la barra de progreso
* 🔄 Modos de reproducción: *Shuffle*, *Repeat All* y *Repeat One*
* 🔊 Control dinámico de volumen
* 🔍 Búsqueda de canciones por nombre
* 🖇️ Filtros avanzados por **artista(s)** y **álbum**
* 🎯 Navegación entre canciones
* 🖼️ Sistema "inteligente" de asignación de imágenes (aleatorio pero estético)

> **Nota técnica:** Las canciones se transmiten por **chunks** (fragmentos) para un futuro uso distribuido.

---

## 🛠️ Requisitos del Sistema

### 🔧 Prerrequisitos Frontend

* **Node.js** ≥ 16.0.0
* **npm** ≥ 8.0.0
* (Opcional) **Vue CLI**

Para verificar tus versiones:

```bash
node --version
npm --version
```

### 🐍 Prerrequisitos Backend

* **Python** ≥ 3.10
* **pip** (gestor de paquetes de Python)
* **ffmpeg** (requerido para el manejo de audio en Django)

---

## ⚙️ Instalación y Configuración

### 1️⃣ Clonar el repositorio

```bash
git clone https://github.com/yesi07111/Spotify-Clone.git
```

---

## 🎨 Configuración del Frontend (Vue.js)

```bash
cd Spotify-Clone/ui
npm install
```

### 🚀 Modo desarrollo

```bash
npm run dev
```

La aplicación estará disponible en:
👉 `http://localhost:8080`

### 🏗️ Compilación para producción

```bash
npm run build
```

### 👀 Preview de producción

```bash
npm run preview
```

### 🧹 Linter (verificación de código)

```bash
npm run lint
```

---

## ⚙️ Configuración del Backend (Django)

### 1️⃣ Crear entorno virtual (recomendado)

Dentro de la carpeta raíz del proyecto (no dentro de `/code`):

```bash
python -m venv .venv
```

Activar el entorno virtual:

* **Windows:**

  ```bash
  .venv\Scripts\activate
  ```
* **Linux / macOS:**

  ```bash
  source .venv/bin/activate
  ```

---

### 2️⃣ Instalar dependencias de Python

Las dependencias necesarias están definidas en `requirements.txt`.
Ejecuta:

```bash
pip install -r requirements.txt
```

> 🧩 **Nota:** No es necesario instalar manualmente nada adicional para la base de datos — **SQLite3** ya viene integrada con Python.

---

### 3️⃣ Estructura del Backend

```
code/
├── app/                 # Lógica principal del proyecto (tu aplicación Django)
│   ├── models.py        # Definición de modelos (entidades)
│   ├── serializers.py   # Serializadores para la API REST
│   ├── urls.py          # Rutas del backend
│   ├── views.py         # Vistas y controladores de la API
│   ├── admin.py         # Administración Django (por defecto)
│   ├── apps.py          # Configuración de la app
│   ├── tests.py         # Pruebas unitarias
│   └── (otros archivos generados automáticamente)
│
├── backend/             # Archivos generados automáticamente por Django (settings, urls, wsgi, etc.)
│
├── manage.py            # Script de gestión del proyecto
└── db.sqlite3           # Base de datos (no se sube al repositorio)
```

---

## 🗄️ Migración de la Base de Datos

Las migraciones de la base de datos ya están creadas y listas para aplicar. Solo necesitas ejecutar:

```bash
python code/manage.py migrate
```

Este comando aplicará todos los cambios del modelo a la base de datos SQLite3.

### 4️⃣ Ejecución del Servidor Django

#### ❗ Importante:

Debido a que la base de datos (`db.sqlite3`) y las rutas absolutas configuradas en `settings.py` están **fuera** de la carpeta `code`, **no se debe ejecutar** el servidor desde dentro de `/code`.

Usa:

```bash
python code/manage.py runserver
```

> ⚠️ Si intentas ejecutar:
>
> ```bash
> python manage.py runserver
> ```
>
> desde dentro de `code/`, obtendrás un error indicando que la base de datos no existe o que las rutas no se encuentran.

El servidor Django se ejecutará en:
👉 `http://127.0.0.1:8000`

---

## 🎧 Instalación de FFmpeg

`ffmpeg` es **necesario** para el tratamiento y conversión de archivos de audio en el backend.

### 🪟 En Windows

1. Descarga el binario desde:
   👉 [https://ffmpeg.org/download.html](https://ffmpeg.org/download.html)
2. Descomprime el archivo descargado (por ejemplo en `C:\ffmpeg`)
3. Agrega la ruta `C:\ffmpeg\bin` al **PATH** del sistema:

   * Abre *Configuración del sistema → Variables de entorno → PATH → Editar → Nuevo*
   * Añade `C:\ffmpeg\bin`
4. Verifica la instalación:

   ```bash
   ffmpeg -version
   ```

### 🐧 En Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install ffmpeg
ffmpeg -version
```

---

## 📁 Estructura General del Proyecto

```
Spotify-Clone/
├── code/                # Backend (Django)
│   ├── app/
│   ├── backend/
│   ├── manage.py
│   └── db.sqlite3
│
├── ui/                  # Frontend (Vue.js)
│   ├── src/
│   │   ├── assets/
│   │   │   └── styles/
│   │   ├── components/
│   │   │   ├── layout/
│   │   │   ├── player/
│   │   │   └── tracks/
│   │   ├── pages/
│   │   ├── routing/
│   │   ├── services/
│   │   ├── store/
│   │   ├── utils/
│   │   ├── App.vue
│   │   └── main.js
│   └── package.json
│
├── requirements.txt
└── README.md
```

---

## ⚠️ Notas Importantes

* Asegurarse de ejecutar el backend **desde fuera de `code/`**, por las rutas absolutas configuradas.
* Si trabajas en Windows, verifica que `ffmpeg` esté correctamente agregado al `PATH`.
* El proyecto está configurado para uso local y desarrollo.

---

## 📄 Licencia

Este proyecto está bajo la **Licencia MIT**.
Consulta el archivo [`LICENSE`](LICENSE) para más detalles.
