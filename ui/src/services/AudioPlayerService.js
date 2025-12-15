// services/AudioPlayerService.js
import { Howl } from 'howler'
import ApiService from '@/services/ApiService'

// Cache global para audio (solo canción actual)
const audioCache = {
  trackId: null,
  blob: null,
  chunks: null,
  metadata: null
}

export default class AudioPlayerService {
  constructor() {
    this.howl = null
    this.currentTrackId = null
    this.isPlaying = false
    this.volume = 0.5
    this.duration = 0
    this.isLoading = false
    this.isDestroyed = false
    
    // 🔥 NUEVAS PROPIEDADES PARA CONTROL
    this.loadingPromise = null
    this.lastStartTime = 0
    this.isStarting = false

    // Callbacks
    this.onPlaybackProgressUpdate = null
    this.onPlaybackEnd = null
    this.onLoadProgress = null
    this.onLoadComplete = null
    
    this.progressInterval = null
  }

  async start(trackId, volume = null) {
    console.log('🎵 [start] === INICIANDO REPRODUCCIÓN ===', { 
      trackId, 
      volume, 
      currentTrackId: this.currentTrackId, 
      isLoading: this.isLoading, 
      isStarting: this.isStarting,
      hasHowl: !!this.howl
    });

    // 🔥 CONTROL CRÍTICO: Prevenir múltiples inicios de la MISMA canción
    if (this.isStarting && this.currentTrackId === trackId) {
      console.log('🚫 [start] BLOQUEADO: Ya se está iniciando esta misma canción, ignorando...', { trackId });
      return false;
    }

    // 🔥 CONTROL CRÍTICO: Debounce para clics rápidos (500ms)
    const now = Date.now();
    if (now - this.lastStartTime < 500 && this.currentTrackId === trackId) {
      console.log('⚡ [start] BLOQUEADO: Clic demasiado rápido en la misma canción', { 
        trackId, 
        timeSinceLastClick: now - this.lastStartTime 
      });
      return false;
    }
    this.lastStartTime = now;

    // Si ya estamos cargando esta canción, esperar
    if (this.isLoading && this.currentTrackId === trackId && this.loadingPromise) {
      console.log('⏳ [start] Esperando: Misma canción ya se está cargando...', { trackId });
      await this.loadingPromise;
      return true;
    }

    // 🔥 CASO PRINCIPAL: Misma canción ya cargada y lista (INCLUYE MODO REPEAT)
    if (this.currentTrackId === trackId && this.howl && !this.isDestroyed) {
      console.log('🔄 [start] Misma canción - Reiniciando desde inicio', { 
        trackId,
        isPlaying: this.isPlaying,
        isRepeatCase: true
      });
      
      // 🔥 DETENER PRIMERO para evitar superposición
      if (this.isPlaying) {
        console.log('⏸️ [start] Pausando reproducción actual antes de reiniciar');
        this.howl.pause();
      }
      
      this.howl.seek(0);
      
      // Pequeño delay para asegurar que el seek se procese
      setTimeout(() => {
        if (this.howl && !this.isDestroyed) {
          console.log('▶️ [start] Reiniciando reproducción después de seek');
          this.howl.play();
        }
      }, 50);
      
      return true;
    }

    try {
      console.log('🚀 [start] INICIANDO NUEVA CARGA/REPRODUCCIÓN', { 
        trackId, 
        previousTrackId: this.currentTrackId,
        isDifferentTrack: this.currentTrackId !== trackId 
      });

      // 🔥 MARCAR COMO INICIANDO INMEDIATAMENTE
      this.isStarting = true;
      this.isLoading = true;

      // 🔥 LIMPIAR SOLO SI ES CANCIÓN DIFERENTE
      if (this.currentTrackId !== trackId) {
        console.log('🔄 [start] Cambiando de canción - Limpiando anterior...', {
          from: this.currentTrackId,
          to: trackId
        });
        this._cleanupPreviousTrack();
        
        // 🔥 LIMPIAR CACHE SI CAMBIA DE CANCIÓN
        if (audioCache.trackId && audioCache.trackId !== trackId) {
          console.log('🗑️ [start] Limpiando caché de canción anterior:', {
            previousCachedTrack: audioCache.trackId,
            newTrack: trackId
          });
          audioCache.trackId = null;
          audioCache.blob = null;
          audioCache.chunks = null;
          audioCache.metadata = null;
        }
      } else {
        console.log('📝 [start] Misma canción - Limpieza preservada');
      }

      this.currentTrackId = trackId;
      this.isDestroyed = false;

      if (volume !== null) {
        console.log('🔊 [start] Configurando volumen:', volume);
        this.volume = volume / 100;
      }

      // Cargar y crear player
      console.log('📥 [start] Iniciando carga del audio...');
      this.loadingPromise = this._loadAndCreatePlayer(trackId);
      await this.loadingPromise;
      
      console.log('✅ [start] Carga completada exitosamente');
      this.loadingPromise = null;
      this.isLoading = false;
      this.isStarting = false;

      if (this.onLoadComplete) {
        console.log('📢 [start] Ejecutando callback onLoadComplete');
        this.onLoadComplete();
      }

      return true;

    } catch (error) {
      console.error('❌ [start] ERROR durante inicio de reproducción:', error);
      this.isLoading = false;
      this.isStarting = false;
      this._cleanupPreviousTrack();
      return false;
    }
  }

  _cleanupPreviousTrack() {
    console.log('🧹 [_cleanupPreviousTrack] === LIMPIANDO CANCIÓN ANTERIOR ===', {
      currentTrackId: this.currentTrackId,
      hasHowl: !!this.howl,
      isPlaying: this.isPlaying,
      isDestroyed: this.isDestroyed
    });

    this.stopProgressTracking();
    
    if (this.howl) {
      console.log('🔇 [_cleanupPreviousTrack] Deteniendo y descargando Howl...');
      try {
        this.howl.stop();
        this.howl.unload();
        console.log('✅ [_cleanupPreviousTrack] Howl anterior descargado');
      } catch (e) {
        console.warn('⚠️ [_cleanupPreviousTrack] Error limpiando Howl:', e);
      }
      this.howl = null;
    } else {
      console.log('ℹ️ [_cleanupPreviousTrack] No hay instancia Howl que limpiar');
    }
    
    this.isPlaying = false;
    console.log('🧹 [_cleanupPreviousTrack] Limpieza completada');
  }

  async _loadAndCreatePlayer(trackId) {
    console.log('📦 [_loadAndCreatePlayer] === CARGANDO Y CREANDO REPRODUCTOR ===', { 
      trackId,
      cachedTrackId: audioCache.trackId,
      hasCachedBlob: !!audioCache.blob
    });

    // Verificar cache primero - SOLO SI ES LA MISMA CANCIÓN
    if (audioCache.trackId === trackId && audioCache.blob && audioCache.metadata) {
      console.log('✅ [_loadAndCreatePlayer] USANDO CACHÉ para track:', trackId, {
        cachedTrackId: audioCache.trackId,
        hasBlob: !!audioCache.blob,
        blobSize: audioCache.blob.size,
        hasMetadata: !!audioCache.metadata
      });
      this.duration = audioCache.metadata.duration_seconds || 0;
      
      // 🔥 NOTIFICAR INMEDIATAMENTE QUE SE USA CACHÉ
      if (this.onLoadComplete) {
        console.log('📢 [_loadAndCreatePlayer] Notificando carga completada (caché)');
        this.onLoadComplete();
      }
      
      await this._createHowlFromBlob(audioCache.blob);
      return;
    }

    console.log('🔄 [_loadAndCreatePlayer] Caché no disponible, cargando desde servidor...');

    // Obtener metadata usando ApiService
    console.log('📡 [_loadAndCreatePlayer] Obteniendo metadatos con ApiService...');
    const metadata = await this.getTrackMetadata(trackId);
    this.duration = metadata.duration_seconds || metadata.duration || 0;
    const totalChunks = metadata.total_chunks;

    console.log('📊 [_loadAndCreatePlayer] Metadatos obtenidos:', {
      duration: this.duration,
      totalChunks: totalChunks
    });

    // Cargar chunks usando ApiService
    console.log('📥 [_loadAndCreatePlayer] Iniciando carga de chunks con ApiService...');
    const chunks = await this.loadAllChunks(trackId, totalChunks);

    if (this.isDestroyed) {
      console.log('❌ [_loadAndCreatePlayer] CARGA CANCELADA - Servicio destruido durante carga');
      throw new Error('Loading cancelled - service destroyed');
    }

    // Crear blob
    console.log('🔨 [_loadAndCreatePlayer] Creando blob desde chunks...');
    const blob = this.createBlobFromChunks(chunks);

    // Guardar en cache - SOLO PARA LA CANCIÓN ACTUAL
    console.log('💾 [_loadAndCreatePlayer] Guardando en caché...', { trackId });
    audioCache.trackId = trackId;
    audioCache.blob = blob;
    audioCache.chunks = chunks;
    audioCache.metadata = metadata;

    console.log('✅ [_loadAndCreatePlayer] Audio guardado en caché para uso futuro');

    // Crear Howl
    console.log('🎵 [_loadAndCreatePlayer] Creando instancia Howl...');
    await this._createHowlFromBlob(blob);
    console.log('🎉 [_loadAndCreatePlayer] Proceso completado exitosamente');
  }

  async getTrackMetadata(trackId) {
    try {
      console.log('[getTrackMetadata] Obteniendo metadata para track:', trackId);
      
      const response = await ApiService.streamAudio({
        audio_id: trackId,
        chunk_index: 0,
        chunk_count: 1,
        include_metadata: true
      });
      
      const data = response.data;
      
      if (!data.metadata) {
        throw new Error('Metadata no encontrada en la respuesta');
      }
      
      console.log('[getTrackMetadata] Metadata obtenida:', data.metadata);
      return data.metadata;
      
    } catch (error) {
      console.error('[getTrackMetadata] Error obteniendo metadata:', error);
      throw error;
    }
  }

  async loadAllChunks(trackId, totalChunks) {
    console.log('📥 [loadAllChunks] Cargando todos los chunks:', { trackId, totalChunks });
    const loadedChunks = [];
    const BATCH_SIZE = 10;
    const batches = Math.ceil(totalChunks / BATCH_SIZE);

    for (let batchIndex = 0; batchIndex < batches; batchIndex++) {
      if (this.isDestroyed) {
        console.log('❌ [loadAllChunks] Carga cancelada - servicio destruido');
        break;
      }

      const startChunk = batchIndex * BATCH_SIZE;
      const endChunk = Math.min(startChunk + BATCH_SIZE, totalChunks);
      const chunksInBatch = endChunk - startChunk;

      console.log(`📦 [loadAllChunks] Cargando batch ${batchIndex + 1}/${batches} (chunks ${startChunk}-${endChunk - 1})`);

      try {
        const response = await ApiService.streamAudio({
          audio_id: trackId,
          chunk_index: startChunk,
          chunk_count: chunksInBatch,
          include_metadata: false
        });

        const data = response.data;
        
        // Procesar chunks
        for (let i = 0; i < data.chunks.length; i++) {
          const chunkData = data.chunks[i];
          const binaryString = atob(chunkData);
          const bytes = new Uint8Array(binaryString.length);
          
          for (let j = 0; j < binaryString.length; j++) {
            bytes[j] = binaryString.charCodeAt(j);
          }
          
          loadedChunks.push(bytes);

          // Actualizar progreso
          if (this.onLoadProgress) {
            const progress = (loadedChunks.length / totalChunks) * 100;
            this.onLoadProgress(loadedChunks.length, totalChunks, progress);
          }
        }

        console.log(`✅ [loadAllChunks] Batch ${batchIndex + 1} cargado (${loadedChunks.length}/${totalChunks} chunks)`);

      } catch (error) {
        console.error(`❌ [loadAllChunks] Error cargando batch ${batchIndex + 1}:`, error);
        throw error;
      }
    }

    console.log('✅ [loadAllChunks] Todos los chunks cargados exitosamente');
    return loadedChunks;
  }

  createBlobFromChunks(chunks) {
    let totalLength = 0;
    chunks.forEach(chunk => {
      totalLength += chunk.length;
    });

    const combined = new Uint8Array(totalLength);
    let offset = 0;
    chunks.forEach(chunk => {
      combined.set(chunk, offset);
      offset += chunk.length;
    });

    const blob = new Blob([combined], { type: 'audio/mpeg' });
    console.log(`🎵 [createBlobFromChunks] Audio blob creado: ${(blob.size / 1024 / 1024).toFixed(2)} MB`);
    
    return blob;
  }

  async _createHowlFromBlob(blob) {
    console.log('🎵 [_createHowlFromBlob] === CREANDO INSTANCIA HOWL ===', {
      blobSize: blob.size,
      blobType: blob.type,
      currentVolume: this.volume
    });

    const audioUrl = URL.createObjectURL(blob);
    console.log('🔗 [_createHowlFromBlob] URL de objeto creada:', audioUrl);

    // 🔥 GARANTIZAR SOLO UNA INSTANCIA DE HOWL
    if (this.howl) {
      console.log('🔄 [_createHowlFromBlob] Ya existe instancia Howl - Reemplazando...');
      try {
        this.howl.unload();
        console.log('✅ [_createHowlFromBlob] Instancia Howl anterior descargada');
      } catch (e) {
        console.warn('⚠️ [_createHowlFromBlob] Error descargando Howl anterior:', e);
      }
      this.howl = null;
    }

    console.log('👶 [_createHowlFromBlob] Creando NUEVA instancia Howl...');
    this.howl = new Howl({
      src: [audioUrl],
      format: ['mp3'],
      html5: true,
      volume: this.volume,
      onload: () => {
        console.log('✅ [_createHowlFromBlob] CALLBACK: Howl cargado exitosamente', {
          duration: this.howl.duration(),
          state: 'ready'
        });
        if (this.howl) {
          this.duration = this.howl.duration();
          console.log(`⏱️ [_createHowlFromBlob] Duración: ${this.formatTime(this.duration)}`);
          
          // 🔥 REPRODUCIR AUTOMÁTICAMENTE AL CARGAR
          console.log('▶️ [_createHowlFromBlob] Reproduciendo automáticamente...');
          this.howl.play();
        }
      },
      onplay: () => {
        console.log('▶️ [_createHowlFromBlob] CALLBACK: Howl reproduciendo', {
          currentTime: this.howl.seek(),
          isPlaying: true
        });
        this.isPlaying = true;
        this.startProgressTracking();
      },
      onpause: () => {
        console.log('⏸️ [_createHowlFromBlob] CALLBACK: Howl pausado');
        this.isPlaying = false;
        this.stopProgressTracking();
      },
      onend: () => {
        console.log('⏹️ [_createHowlFromBlob] CALLBACK: Howl terminado');
        this.isPlaying = false;
        this.stopProgressTracking();
        if (this.onPlaybackEnd) {
          console.log('📢 [_createHowlFromBlob] Ejecutando callback onPlaybackEnd');
          this.onPlaybackEnd();
        }
      },
      onstop: () => {
        console.log('🛑 [_createHowlFromBlob] CALLBACK: Howl detenido');
        this.isPlaying = false;
        this.stopProgressTracking();
      },
      onloaderror: (id, error) => {
        console.error('❌ [_createHowlFromBlob] CALLBACK: Error de carga Howl:', error);
        this.isLoading = false;
        this.isStarting = false;
      },
      onplayerror: (id, error) => {
        console.error('❌ [_createHowlFromBlob] CALLBACK: Error de reproducción Howl:', error);
        // Reintentar después de un breve delay
        setTimeout(() => {
          if (this.howl && !this.isDestroyed && !this.isPlaying) {
            console.log('🔄 [_createHowlFromBlob] Reintentando reproducción después de error...');
            this.howl.play();
          }
        }, 100);
      }
    });

    console.log('⏳ [_createHowlFromBlob] Esperando a que Howl se cargue...');
    return new Promise((resolve, reject) => {
      this.howl.once('load', () => {
        console.log('✅ [_createHowlFromBlob] Howl cargado - Promise resuelta');
        resolve();
      });
      this.howl.once('loaderror', (id, error) => {
        console.error('❌ [_createHowlFromBlob] Howl error al cargar - Promise rechazada:', error);
        reject(error);
      });
    });
  }

  startProgressTracking() {
    this.stopProgressTracking();
    
    this.progressInterval = setInterval(() => {
      if (!this.howl || this.isDestroyed) {
        this.stopProgressTracking();
        return;
      }

      const currentTime = this.howl.seek();
      const duration = this.howl.duration();

      if (this.onPlaybackProgressUpdate) {
        this.onPlaybackProgressUpdate({
          currentTime: currentTime,
          duration: duration,
          formattedTime: this.formatTime(currentTime),
          formattedDuration: this.formatTime(duration),
          progress: duration > 0 ? (currentTime / duration) * 100 : 0
        });
      }
    }, 100);
  }

  stopProgressTracking() {
    if (this.progressInterval) {
      clearInterval(this.progressInterval);
      this.progressInterval = null;
    }
  }

  playAndPause() {
    if (!this.howl || this.isDestroyed || this.isLoading) {
      return;
    }

    if (this.isPlaying) {
      this.howl.pause();
    } else {
      this.howl.play();
    }
  }

  play() {
    if (this.howl && !this.isDestroyed && !this.isLoading) {
      this.howl.play();
    }
  }

  pause() {
    if (this.howl && !this.isDestroyed) {
      this.howl.pause();
    }
  }

  stop() {
    if (this.howl && !this.isDestroyed) {
      this.howl.stop();
    }
  }

  seekToPosition(positionPercent) {
    if (!this.howl || this.isDestroyed || this.isLoading) {
      return;
    }

    const duration = this.howl.duration();
    if (!duration || duration <= 0) {
      return;
    }

    const targetTime = (positionPercent / 100) * duration;
    this.howl.seek(targetTime);

    if (this.onPlaybackProgressUpdate) {
      this.onPlaybackProgressUpdate({
        currentTime: targetTime,
        duration: duration,
        formattedTime: this.formatTime(targetTime),
        formattedDuration: this.formatTime(duration),
        progress: positionPercent
      });
    }
  }

  setVolume(volume) {
    const normalizedVolume = Math.max(0, Math.min(1, volume / 100));
    this.volume = normalizedVolume;
    
    if (this.howl && !this.isDestroyed) {
      this.howl.volume(normalizedVolume);
    }
  }

  formatTime(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '0:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  }

  destroy() {
    console.log('🧹 [destroy] === DESTRUYENDO AUDIO PLAYER SERVICE ===', {
      currentTrackId: this.currentTrackId,
      hasHowl: !!this.howl,
      isPlaying: this.isPlaying,
      isLoading: this.isLoading
    });

    this.isDestroyed = true;
    this.isPlaying = false;
    this.isLoading = false;
    this.isStarting = false;

    this.stopProgressTracking();
    this._cleanupPreviousTrack();
    
    this.currentTrackId = null;
    console.log('✅ [destroy] AudioPlayerService completamente destruido');
  }
}

// Función para limpiar cache manualmente si es necesario
export function clearAudioCache() {
  audioCache.trackId = null;
  audioCache.blob = null;
  audioCache.chunks = null;
  audioCache.metadata = null;
  console.log('🗑️ Audio cache cleared');
}