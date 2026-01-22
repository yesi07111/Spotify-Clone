// store/modules/player.js
import AudioPlayerService from '@/services/AudioPlayerService'
import PlaylistManager from '@/services/PlaylistManager'

const REPEAT_MODES = {
  OFF: 'off',
  ALL: 'all',
  ONE: 'one'
}

const state = {
  audioService: null,
  playlistManager: null,
  currentSong: {},
  isPlaying: false,
  currentTime: 0,
  duration: 0,
  formattedElapsedTime: '0:00',
  formattedTotalDuration: '0:00',
  playbackProgress: 0,
  volume: 50,
  previousVolume: 50, // Para guardar el volumen antes de mutear
  isMuted: false,
  isShuffleActive: false,
  repeatMode: REPEAT_MODES.OFF,
  loadingTrack: false,
  chunksLoaded: 0,
  totalChunks: 0,
  loadProgress: 0,
  originalTrackOrder: []
}

const mutations = {
  SET_AUDIO_SERVICE(state, service) {
    state.audioService = service
  },

  SET_PLAYLIST_MANAGER(state, manager) {
    state.playlistManager = manager
  },

  SET_CURRENT_SONG(state, song) {
    state.currentSong = song || {}
  },

  SET_PLAYING_STATE(state, isPlaying) {
    state.isPlaying = isPlaying
  },

  SET_PLAYBACK_TIME(state, { currentTime, duration }) {
    state.currentTime = currentTime
    state.duration = duration
  },

  SET_PLAYBACK_PROGRESS(state, { currentTime, duration, progress }) {
    state.currentTime = currentTime
    state.duration = duration
    state.playbackProgress = progress
  },

  SET_FORMATTED_TIMES(state, { elapsed, total }) {
    state.formattedElapsedTime = elapsed
    state.formattedTotalDuration = total
  },

  SET_VOLUME(state, volume) {
    state.volume = volume
  },

  SET_PREVIOUS_VOLUME(state, volume) {
    state.previousVolume = volume
  },

  SET_MUTED(state, isMuted) {
    state.isMuted = isMuted
  },

  SET_SHUFFLE_ACTIVE(state, isActive) {
    state.isShuffleActive = isActive
  },

  SET_REPEAT_MODE(state, mode) {
    state.repeatMode = mode
  },

  SET_LOADING_TRACK(state, isLoading) {
    state.loadingTrack = isLoading
  },

  SET_CHUNKS_PROGRESS(state, { loaded, total, progress }) {
    state.chunksLoaded = loaded
    state.totalChunks = total
    state.loadProgress = progress || (total > 0 ? (loaded / total) * 100 : 0)
  },

  SET_ORIGINAL_TRACK_ORDER(state, tracks) {
    state.originalTrackOrder = [...tracks]
  }
}

const actions = {
  initializeAudioService({ commit, state, dispatch }) {
    if (state.audioService) {
      return state.audioService
    }

    const audioService = new AudioPlayerService()

    audioService.onPlaybackProgressUpdate = (data) => {
      commit('SET_PLAYBACK_TIME', {
        currentTime: data.currentTime,
        duration: data.duration
      })

      commit('SET_FORMATTED_TIMES', {
        elapsed: data.formattedTime,
        total: data.formattedDuration
      })

      commit('SET_PLAYBACK_PROGRESS', {
        currentTime: data.currentTime,
        duration: data.duration,
        progress: data.progress
      })
    }

    audioService.onPlaybackEnd = () => {
  console.log('⏹️ [onPlaybackEnd] CANCIÓN TERMINADA', {
    currentTrackId: state.currentSong?.id,
    repeatMode: state.repeatMode,
    isPlaying: state.isPlaying
  });
  
  commit('SET_PLAYING_STATE', false);
  
  if (state.repeatMode === REPEAT_MODES.ONE) {
    console.log('🔁 [onPlaybackEnd] MODO REPEAT ONE - Reiniciando misma canción');
    
    // 🔥 REINICIAR DIRECTAMENTE SIN PASAR POR selectTrack
    setTimeout(() => {
      if (state.audioService && state.audioService.howl && state.currentSong.id) {
        console.log('🔄 [onPlaybackEnd] Reiniciando canción en modo repeat one');
        
        // Reiniciar posición y reproducir
        state.audioService.howl.seek(0);
        state.audioService.howl.play();
        commit('SET_PLAYING_STATE', true);
        
        console.log('✅ [onPlaybackEnd] Canción reiniciada en modo repeat one');
      } else {
        console.warn('⚠️ [onPlaybackEnd] No se pudo reiniciar - audioService no disponible');
      }
    }, 100);
    
  } else if (state.repeatMode === REPEAT_MODES.ALL) {
    console.log('🔁 [onPlaybackEnd] MODO REPEAT ALL - Siguiente canción');
    setTimeout(() => {
      dispatch('playNextTrack');
    }, 500);
  } else if (state.repeatMode === REPEAT_MODES.OFF) {
    console.log('⏹️ [onPlaybackEnd] MODO REPEAT OFF - Verificando siguiente canción');
    if (state.playlistManager) {
      const nextIndex = state.playlistManager.currentIndex + 1;
      if (nextIndex < state.playlistManager.tracks.length) {
        setTimeout(() => {
          dispatch('playNextTrack');
        }, 500);
      } else {
        console.log('🏁 [onPlaybackEnd] Última canción - reproducción terminada');
      }
    }
  }
}

    audioService.onLoadProgress = (loaded, total, progress) => {
      commit('SET_CHUNKS_PROGRESS', { loaded, total, progress })
    }

    audioService.onLoadComplete = () => {
      commit('SET_LOADING_TRACK', false)
    }

    commit('SET_AUDIO_SERVICE', audioService)
    return audioService
  },

  initializePlaylistManager({ commit, state, rootState }) {
    if (state.playlistManager) {
      return state.playlistManager
    }

    const playlistManager = new PlaylistManager()
    
    if (rootState.tracks && rootState.tracks.tracks) {
      playlistManager.setSongs(rootState.tracks.tracks)
      commit('SET_ORIGINAL_TRACK_ORDER', rootState.tracks.tracks)
    }

    commit('SET_PLAYLIST_MANAGER', playlistManager)
    return playlistManager
  },

async selectTrack({ commit, state, dispatch }, track) {
  console.log('[selectTrack] === SELECCIONANDO CANCIÓN ===', {
    trackId: track?.id,
    trackTitle: track?.title,
    currentTrackId: state.currentSong?.id,
    isCurrentlyPlaying: state.isPlaying,
    isLoading: state.loadingTrack,
    repeatMode: state.repeatMode
  })

  // Validación básica de la canción
  if (!track || !track.id) {
    console.error('[selectTrack] CANCIÓN NO VÁLIDA proporcionada:', track)
    return
  }

  /*
   * CASO 1: La misma canción ya está cargada en el servicio.
   * No se vuelve a cargar audio ni se toca el estado de loading.
   * El store decide explícitamente si debe reproducirse.
   */
  if (
    state.currentSong.id === track.id &&
    state.audioService &&
    state.audioService.currentTrackId === track.id &&
    state.audioService.howl &&
    !state.loadingTrack
  ) {
    console.log('[selectTrack] Misma canción ya cargada - reutilizando audio existente', {
      isPlaying: state.isPlaying,
      repeatMode: state.repeatMode
    })

    // Asegurar que la UI refleje la canción actual
    commit('SET_CURRENT_SONG', track)

    // Reiniciar la posición al inicio
    setTimeout(() => {
      if (state.audioService && state.audioService.howl) {
        console.log('[selectTrack] Reiniciando canción desde el inicio')
        state.audioService.howl.seek(0)

        // El store ordena reproducir si no estaba sonando
        if (!state.isPlaying) {
          state.audioService.play()
          commit('SET_PLAYING_STATE', true)
        }

        console.log('[selectTrack] Canción reiniciada correctamente')
      }
    }, 100)

    return
  }

  try {
    console.log('[selectTrack] Iniciando proceso de carga de canción', {
      isDifferentTrack: state.currentSong.id !== track.id
    })

    /*
     * Solo se muestra estado de carga si realmente es una canción distinta.
     * Esto evita parpadeos de loading cuando se reutiliza caché.
     */
    if (state.currentSong.id !== track.id) {
      commit('SET_LOADING_TRACK', true)
      commit('SET_CHUNKS_PROGRESS', { loaded: 0, total: 0, progress: 0 })
    }

    // Actualizar canción actual en el store (UI)
    commit('SET_CURRENT_SONG', track)

    // Inicializar servicios si aún no existen
    if (!state.audioService) {
      console.log('[selectTrack] Inicializando AudioPlayerService')
      await dispatch('initializeAudioService')
    }

    if (!state.playlistManager) {
      console.log('[selectTrack] Inicializando PlaylistManager')
      await dispatch('initializePlaylistManager')
    }

    console.log('[selectTrack] Estableciendo canción actual en el playlist manager')
    state.playlistManager.setCurrentSong(track.id)

    /*
     * start() únicamente carga y prepara el audio.
     * No reproduce automáticamente.
     */
    console.log('[selectTrack] Cargando audio a través del AudioPlayerService')
    const success = await state.audioService.start(track.id, state.volume)

    if (success) {
      console.log('[selectTrack] Audio cargado correctamente, iniciando reproducción')

      // El store decide explícitamente reproducir
      state.audioService.play()
      commit('SET_PLAYING_STATE', true)

      // Si se reutilizó caché, el loading se desactiva inmediatamente
      if (state.audioService.howl) {
        commit('SET_LOADING_TRACK', false)
      }
    } else {
      console.warn('[selectTrack] El AudioPlayerService no pudo iniciar la canción')
      commit('SET_PLAYING_STATE', false)
      commit('SET_LOADING_TRACK', false)
    }

  } catch (error) {
    console.error('[selectTrack] ERROR durante la selección de canción:', error)
    commit('SET_PLAYING_STATE', false)
    commit('SET_LOADING_TRACK', false)

  } finally {
    console.log('[selectTrack] Proceso de selección finalizado', {
      currentTrackId: state.currentSong.id,
      isLoading: state.loadingTrack
    })
  }
},


  togglePlayback({ state, commit }) {
    if (!state.audioService || state.loadingTrack) {
      return
    }

    try {
      const shouldPlay = !state.isPlaying

      if (shouldPlay) {
        state.audioService.play()
      } else {
        state.audioService.pause()
      }

      commit('SET_PLAYING_STATE', shouldPlay)
    } catch (error) {
      console.error('❌ Error toggling playback:', error)
    }
  },

  async playNextTrack({ state, dispatch }) {
    if (!state.playlistManager) {
      return
    }

    try {
      state.playlistManager.next()
      const nextSong = state.playlistManager.getCurrentSong()
      
      if (nextSong) {
        await dispatch('selectTrack', nextSong)
      }
    } catch (error) {
      console.error('❌ Error playing next track:', error)
    }
  },

  async playPreviousTrack({ state, dispatch }) {
    if (!state.playlistManager) {
      return
    }

    try {
      if (state.currentTime > 3) {
        state.audioService.seekToPosition(0)
        return
      }

      state.playlistManager.prev()
      const prevSong = state.playlistManager.getCurrentSong()
      
      if (prevSong) {
        await dispatch('selectTrack', prevSong)
      }
    } catch (error) {
      console.error('❌ Error playing previous track:', error)
    }
  },

  seekToPosition({ state, commit }, positionPercent) {
    if (!state.audioService || !state.duration || state.loadingTrack) {
      return
    }

    const targetTime = (positionPercent / 100) * state.duration
    commit('SET_PLAYBACK_PROGRESS', {
      currentTime: targetTime,
      duration: state.duration,
      progress: positionPercent
    })

    state.audioService.seekToPosition(positionPercent)
  },

  setVolume({ state, commit }, event) {
  // Permitir que venga número directo o evento del input range
  const volume = typeof event === 'number'
    ? event
    : parseInt(event.target.value, 10)

  // Normalizar a rango 0–100
  const normalizedVolume = Math.max(0, Math.min(100, volume))

  // Actualizar estado global
  commit('SET_VOLUME', normalizedVolume)

  // Gestión automática de mute
  if (normalizedVolume > 0 && state.isMuted) {
    commit('SET_MUTED', false)
  }

  if (normalizedVolume === 0 && !state.isMuted) {
    commit('SET_MUTED', true)
  }

  // Aplicar volumen inmediatamente al servicio de audio
  // Esto debe funcionar en play, pause o durante carga
  if (state.audioService) {
    state.audioService.setVolume(normalizedVolume)
  }
},

  toggleMute({ state, commit }) {
    if (state.isMuted) {
      // Desmutear: restaurar volumen anterior
      const volumeToRestore = state.previousVolume > 0 ? state.previousVolume : 50
      commit('SET_VOLUME', volumeToRestore)
      commit('SET_MUTED', false)
      
      if (state.audioService) {
        state.audioService.setVolume(volumeToRestore)
      }
    } else {
      // Mutear: guardar volumen actual y poner a 0
      commit('SET_PREVIOUS_VOLUME', state.volume)
      commit('SET_VOLUME', 0)
      commit('SET_MUTED', true)
      
      if (state.audioService) {
        state.audioService.setVolume(0)
      }
    }
  },

  async toggleShuffle({ commit, state, dispatch, rootState }) {
    const newShuffleState = !state.isShuffleActive
    commit('SET_SHUFFLE_ACTIVE', newShuffleState)

    if (!state.playlistManager) {
      await dispatch('initializePlaylistManager')
    }

    if (newShuffleState) {
      // Guardar orden original
      commit('SET_ORIGINAL_TRACK_ORDER', [...state.playlistManager.tracks])
      
      // Mezclar manteniendo la canción actual al inicio
      const currentSongId = state.currentSong?.id
      const tracks = [...state.playlistManager.tracks]
      
      // Separar la canción actual del resto
      const currentTrackIndex = tracks.findIndex(t => t.id === currentSongId)
      let currentTrack = null
      let otherTracks = tracks
      
      if (currentTrackIndex >= 0) {
        currentTrack = tracks[currentTrackIndex]
        otherTracks = tracks.filter((_, i) => i !== currentTrackIndex)
      }
      
      // Mezclar el resto
      for (let i = otherTracks.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1))
        const tmp = otherTracks[i]
        otherTracks[i] = otherTracks[j]
        otherTracks[j] = tmp
      }
      
      // Reconstruir con canción actual al inicio
      const shuffledTracks = currentTrack ? [currentTrack, ...otherTracks] : otherTracks
      
      state.playlistManager.setSongs(shuffledTracks)
      state.playlistManager.currentIndex = 0
      
      // Actualizar store de tracks
      if (rootState.tracks) {
        rootState.tracks.tracks = [...shuffledTracks]
      }
      
      console.log('🔀 Playlist shuffled (current track first)')
    } else {
      // Restaurar orden original
      if (state.originalTrackOrder.length > 0) {
        state.playlistManager.setSongs(state.originalTrackOrder)
        
        if (rootState.tracks) {
          rootState.tracks.tracks = [...state.originalTrackOrder]
        }
        
        if (state.currentSong?.id) {
          state.playlistManager.setCurrentSong(state.currentSong.id)
        }
        
        console.log('📋 Original order restored')
      }
    }
  },

  toggleRepeat({ commit, state }) {
    let newMode
    switch (state.repeatMode) {
      case REPEAT_MODES.OFF:
        newMode = REPEAT_MODES.ALL
        break
      case REPEAT_MODES.ALL:
        newMode = REPEAT_MODES.ONE
        break
      case REPEAT_MODES.ONE:
        newMode = REPEAT_MODES.OFF
        break
      default:
        newMode = REPEAT_MODES.OFF
    }
    
    commit('SET_REPEAT_MODE', newMode)
    console.log(`🔁 Repeat mode: ${newMode}`)
  },

  updatePlaylistFromTracks({ state, commit, rootState }) {
    if (state.playlistManager && rootState.tracks && rootState.tracks.tracks) {
      state.playlistManager.setSongs(rootState.tracks.tracks)
      
      // Solo actualizar el orden original si no estamos en shuffle
      if (!state.isShuffleActive) {
        commit('SET_ORIGINAL_TRACK_ORDER', rootState.tracks.tracks)
      }
    }
  },

  stopTrack({ commit, state }) {
    if (state.audioService && state.audioService.howl) {
      state.audioService.howl.stop()
    }
    commit('SET_CURRENT_SONG', null)
    commit('SET_PLAYING_STATE', false)
    commit('SET_PLAYBACK_TIME', { currentTime: 0, duration: 0 })
    commit('SET_PLAYBACK_PROGRESS', { currentTime: 0, duration: 0, progress: 0 })
  },

  destroyPlayer({ state, commit }) {
    if (state.audioService) {
      state.audioService.destroy()
    }

    commit('SET_AUDIO_SERVICE', null)
    commit('SET_PLAYLIST_MANAGER', null)
    commit('SET_CURRENT_SONG', {})
    commit('SET_PLAYING_STATE', false)
    commit('SET_PLAYBACK_TIME', { currentTime: 0, duration: 0 })
    commit('SET_PLAYBACK_PROGRESS', { currentTime: 0, duration: 0, progress: 0 })
    commit('SET_CHUNKS_PROGRESS', { loaded: 0, total: 0, progress: 0 })
    commit('SET_REPEAT_MODE', REPEAT_MODES.OFF)
    commit('SET_SHUFFLE_ACTIVE', false)
  }
}

const getters = {
  isAudioServiceReady: state => !!state.audioService && !state.loadingTrack,
  currentSongTitle: state => state.currentSong.title || 'Sin título',
  currentSongArtist: state => {
    if (Array.isArray(state.currentSong.artist_names)) {
      return state.currentSong.artist_names.join(', ')
    }
    return state.currentSong.artist || 'Artista desconocido'
  },
  currentSongAlbum: state => state.currentSong.album_name || state.currentSong.album || 'Álbum desconocido',
  loadingProgress: state => state.loadProgress,
  repeatModeIcon: () => 'fa-redo',
  isRepeatActive: state => state.repeatMode !== REPEAT_MODES.OFF,
  repeatModeLabel: state => {
    switch (state.repeatMode) {
      case REPEAT_MODES.ONE:
        return 'Repetir: Una canción'
      case REPEAT_MODES.ALL:
        return 'Repetir: Todas'
      default:
        return 'Repetir: Desactivado'
    }
  },
  volumeIcon: state => {
  if (state.isMuted || state.volume === 0) {
    return 'fas fa-volume-mute'
  } else if (state.volume > 0 && state.volume <= 30) {
    return 'fas fa-volume-off'  // Una onda de sonido
  } else if (state.volume > 30 && state.volume <= 70) {
    return 'fas fa-volume-low'       // Dos ondas de sonido  
  } else {
    return 'fas fa-volume-high'    // Tres ondas de sonido (bocinita completa)
  }
},
  volumeColor: state => {
    if (state.isMuted || state.volume === 0) {
      return '#6c757d'
    }
    // Interpolación de blanco a azul según el volumen
    const ratio = state.volume / 100
    const r = Math.round(255 - (255 - 13) * ratio)
    const g = Math.round(255 - (255 - 110) * ratio)
    const b = Math.round(255 - (255 - 253) * ratio)
    return `rgb(${r}, ${g}, ${b})`
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}