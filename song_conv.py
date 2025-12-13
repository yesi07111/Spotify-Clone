import base64
import sys
import os
import json
import mutagen

def extract_audio_metadata(file_path):
    """
    Extrae metadatos de un archivo de audio usando mutagen.
    Retorna duración en segundos y bitrate.
    """
    try:
        audio = mutagen.File(file_path)
        if audio is None:
            print("⚠️  No se pudieron extraer metadatos del archivo. Usando valores por defecto.")
            return 0, 0
        
        # Obtener duración en segundos
        duration = audio.info.length if hasattr(audio.info, 'length') else 0
        
        # Obtener bitrate (en bps)
        bitrate = audio.info.bitrate if hasattr(audio.info, 'bitrate') else 0
        
        # Convertir bitrate a kbps si es necesario
        if bitrate > 1000:
            bitrate = bitrate // 1000
        
        return duration, bitrate
        
    except Exception as e:
        print(f"⚠️  Error extrayendo metadatos: {e}. Usando valores por defecto.")
        return 0, 0

def create_song_json(song_path, output_file=None):
    """
    Crea un archivo JSON con la estructura solicitada y el archivo codificado en base64.
    """
    try:
        # Verificar que el archivo existe
        if not os.path.exists(song_path):
            print(f"❌ Error: No se pudo encontrar el archivo en {song_path}")
            return False
        
        # Obtener metadatos del audio
        duration_seconds, bitrate = extract_audio_metadata(song_path)
        
        # Leer y codificar el archivo en base64
        with open(song_path, 'rb') as song_file:
            binary_data = song_file.read()
            base64_encoded = base64.b64encode(binary_data)
            base64_string = base64_encoded.decode('utf-8')
        
        # Obtener información del archivo
        file_name = os.path.basename(song_path)
        file_name_without_ext, extension = os.path.splitext(file_name)
        extension = extension.lower()
        
        # Crear la estructura del JSON
        song_data = {
            "id": "ABC1",
            "title": "string",  # Podrías cambiar esto por file_name_without_ext si quieres
            "album": None,
            "artist": [
                "string"
            ],
            "file_base64": base64_string,
            "duration_seconds": int(duration_seconds) if duration_seconds.is_integer() else float(duration_seconds),
            "bitrate": int(bitrate),
            "extension": extension.replace('.', '')  # Quitar el punto inicial
        }
        
        # Generar nombre del archivo de salida si no se proporciona
        if output_file is None:
            base_name = os.path.basename(song_path)
            file_name, _ = os.path.splitext(base_name)
            output_file = f"{file_name}_song_data.txt"
        
        # Guardar el JSON en un archivo .txt
        with open(output_file, 'w', encoding='utf-8') as txt_file:
            json.dump(song_data, txt_file, indent=2, ensure_ascii=False)
        
        # Mostrar información de éxito
        file_size = os.path.getsize(song_path)
        print(f"✅ JSON creado exitosamente!")
        print(f"📂 Archivo de entrada: {song_path}")
        print(f"📊 Tamaño original: {file_size:,} bytes")
        print(f"📄 Archivo de salida: {output_file}")
        print(f"🎵 Duración: {duration_seconds:.2f} segundos")
        print(f"🎚️  Bitrate: {bitrate} kbps")
        print(f"📝 Extensión: {extension}")
        
        # Mostrar preview del JSON
        print(f"\n🔍 Preview del JSON (primeros 200 caracteres del base64):")
        preview_data = song_data.copy()
        base64_preview = preview_data["file_base64"]
        if len(base64_preview) > 200:
            preview_data["file_base64"] = base64_preview[:200] + "..."
        print(json.dumps(preview_data, indent=2, ensure_ascii=False))
        
        return True
        
    except FileNotFoundError:
        print(f"❌ Error: No se pudo encontrar el archivo en {song_path}")
        return False
    except PermissionError:
        print(f"❌ Error: No tienes permisos para leer/escribir en {song_path}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def main():
    """
    Función principal que maneja los argumentos de línea de comandos.
    """
    # Verificar que se proporcionó al menos un argumento
    if len(sys.argv) < 2:
        print("Uso: python script.py <ruta_de_la_cancion> [archivo_salida]")
        print("Ejemplos:")
        print("  python script.py cancion.mp3")
        print("  python script.py cancion.mp3 song_data.json")
        print("\nNota: Necesitas tener instalado mutagen: pip install mutagen")
        sys.exit(1)
    
    # Obtener argumentos
    song_path = sys.argv[1]
    
    # Si se proporciona un segundo argumento, usarlo como archivo de salida
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Ejecutar la creación del JSON
    success = create_song_json(song_path, output_file)
    
    # Salir con código apropiado
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()