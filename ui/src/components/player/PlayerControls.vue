<!-- //PlayerControls.vue -->
<template>
  <div class="player-controls-container">
    <div class="control-buttons">
      <button 
        class="control-button" 
        :class="{ 'control-button-active': isShuffleActive }"
        @click="toggleShuffle"
        :disabled="loadingTrack"
        :title="isShuffleActive ? 'Desactivar aleatorio' : 'Activar aleatorio'"
        aria-label="Activar mezcla aleatoria"
      >
        <i class="fas fa-random"></i>
      </button>
      <button 
        class="control-button" 
        @click="playPreviousTrack"
        :disabled="loadingTrack || !currentSong.id"
        title="Canción anterior"
        aria-label="Reproducir canción anterior"
      >
        <i class="fas fa-step-backward"></i>
      </button>
      <button 
        class="control-button control-button-primary" 
        @click="togglePlayback"
        :disabled="loadingTrack || !currentSong.id"
        :title="isPlaying ? 'Pausar' : 'Reproducir'"
        aria-label="Reproducir o pausar"
      >
        <i v-if="loadingTrack" class="fas fa-spinner fa-spin"></i>
        <i v-else class="fas" :class="isPlaying ? 'fa-pause-circle' : 'fa-play-circle'"></i>
      </button>
      <button 
        class="control-button" 
        @click="playNextTrack"
        :disabled="loadingTrack || !currentSong.id"
        title="Siguiente canción"
        aria-label="Reproducir siguiente canción"
      >
        <i class="fas fa-step-forward"></i>
      </button>
      <button 
        class="control-button" 
        :class="{ 'control-button-active': isRepeatActive }"
        @click="toggleRepeat"
        :disabled="loadingTrack"
        :title="repeatModeLabel"
        aria-label="Cambiar modo de repetición"
      >
        <i class="fas" :class="repeatModeIcon"></i>
        <span v-if="repeatMode === 'one'" class="repeat-one-indicator">1</span>
      </button>
    </div>

    <div v-if="loadingTrack" class="loading-progress-container">
      <div class="progress" style="height: 6px;">
        <div 
          class="progress-bar progress-bar-striped progress-bar-animated" 
          :style="{ width: loadingProgress + '%' }"
        ></div>
      </div>
      <small class="text-muted mt-1">
        Cargando: {{ Math.round(loadingProgress) }}% 
        ({{ chunksLoaded }}/{{ totalChunks }} chunks)
      </small>
    </div>

    <div v-if="!loadingTrack" class="progress-container">
      <span class="progress-time">{{ formattedElapsedTime || '0:00' }}</span>
      
      <div class="progress-slider-wrapper">
        <input 
          ref="progressSlider"
          type="range" 
          class="progress-slider" 
          min="0" 
          max="100" 
          step="0.1"
          :value="currentProgress" 
          @input="onSliderInput"
          @change="onSliderChange"
          @mousedown="onSliderMouseDown"
          @mouseup="onSliderMouseUp"
          @touchstart="onSliderMouseDown"
          @touchend="onSliderMouseUp"
          :disabled="!currentSong.id || loadingTrack"
          aria-label="Barra de progreso de la canción"
        >
        <div class="slider-progress" :style="{ width: currentProgress + '%' }"></div>
      </div>
      
      <span class="progress-time">{{ formattedTotalDuration || '0:00' }}</span>
    </div>
  </div>
</template>

<script>
import { mapState, mapActions, mapGetters } from 'vuex'

export default {
  name: 'PlayerControls',
  data() {
    return {
      isDragging: false,
      dragProgress: 0
    }
  },
  computed: {
    ...mapState('player', [
      'isPlaying',
      'formattedElapsedTime',
      'formattedTotalDuration',
      'playbackProgress',
      'isShuffleActive',
      'repeatMode',
      'loadingTrack',
      'currentSong',
      'chunksLoaded',
      'totalChunks',
      'loadProgress',
      'currentTime',
      'duration'
    ]),
    ...mapGetters('player', [
      'currentSongTitle',
      'currentSongArtist',
      'loadingProgress',
      'repeatModeIcon',
      'isRepeatActive',
      'repeatModeLabel'
    ]),
    
    currentProgress() {
      return this.isDragging ? this.dragProgress : (this.playbackProgress || 0)
    }
  },

  methods: {
    ...mapActions('player', [
      'togglePlayback',
      'playNextTrack',
      'playPreviousTrack',
      'seekToPosition',
      'toggleShuffle',
      'toggleRepeat'
    ]),
    
    onSliderMouseDown(event) {
      this.isDragging = true
      this.updateDragProgress(event)
    },
    
    onSliderMouseUp(event) {
      this.isDragging = false
      this.handleSeek(event)
    },
    
    onSliderInput(event) {
      if (this.isDragging) {
        this.updateDragProgress(event)
      }
    },
    
    onSliderChange(event) {
      if (!this.isDragging) {
        this.handleSeek(event)
      }
    },
    
    updateDragProgress(event) {
      const slider = event.target
      const value = parseFloat(slider.value)
      this.dragProgress = value
    },
    
    handleSeek(event) {
      const positionPercent = parseFloat(event.target.value)
      this.seekToPosition(positionPercent)
    },
    
    formatTime(seconds) {
      if (!isFinite(seconds) || seconds < 0) return '0:00'
      const mins = Math.floor(seconds / 60)
      const secs = Math.floor(seconds % 60)
      return `${mins}:${secs.toString().padStart(2, '0')}`
    }
  },
  
  mounted() {
    console.log('✅ PlayerControls mounted')
  }
}
</script>
<style scoped>
.player-controls-container {
  padding: 1rem 0;
}

.control-buttons {
  display: flex;
  justify-content: center;
  align-items: center;
  margin-bottom: 1rem;
  gap: 0.5rem;
}

.control-button {
  background: none;
  border: none;
  color: var(--color-gray-light);
  font-size: 1.2rem;
  padding: 0.5rem;
  cursor: pointer;
  transition: all 0.2s ease;
  border-radius: 50%;
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
}

.control-button:hover:not(:disabled) {
  color: var(--color-primary);
  background-color: rgba(255, 255, 255, 0.1);
  transform: scale(1.05);
}

.control-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.control-button-primary {
  font-size: 2rem;
  color: var(--color-primary);
  width: 50px;
  height: 50px;
}

.control-button-primary:hover:not(:disabled) {
  color: var(--color-primary-dark);
  transform: scale(1.15);
}

.control-button-active {
  color: var(--color-primary) !important;
}

.repeat-one-indicator {
  position: absolute;
  bottom: 2px;
  right: 2px;
  font-size: 0.6rem;
  font-weight: bold;
  background: var(--color-primary);
  color: white;
  border-radius: 50%;
  width: 14px;
  height: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
}

/* Barra de progreso de carga */
.loading-progress-container {
  margin: 0.5rem 0;
  text-align: center;
}

.loading-progress-container .progress {
  background-color: var(--color-dark-lighter);
  border-radius: 3px;
  overflow: hidden;
}

.loading-progress-container .progress-bar {
  background: linear-gradient(90deg, 
    var(--color-primary) 0%, 
    var(--color-primary-dark) 50%, 
    var(--color-primary) 100%);
  transition: width 0.3s ease;
}

.loading-progress-container small {
  display: block;
  margin-top: 0.5rem;
  font-size: 0.8rem;
}

/* Barra de progreso de reproducción */
.progress-container {
  display: flex;
  align-items: center;
  margin: 1rem 0;
  gap: 0.5rem;
}

.progress-time {
  color: var(--color-gray);
  font-size: 0.75rem;
  min-width: 40px;
  text-align: center;
  font-variant-numeric: tabular-nums;
}

.progress-slider-wrapper {
  position: relative;
  flex: 1;
  height: 20px;
  display: flex;
  align-items: center;
}

.slider-progress {
  position: absolute;
  height: 4px;
  background: var(--color-primary);
  border-radius: 2px;
  pointer-events: none;
  z-index: 1;
  transition: width 0.1s ease;
  left: 0;
  top: 50%;
  transform: translateY(-50%);
}

.progress-slider {
  width: 100%;
  height: 100%;
  position: relative;
  z-index: 2;
  margin: 0;
  background: transparent;
  -webkit-appearance: none;
  appearance: none;
  outline: none;
  cursor: pointer;
}

.progress-slider:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.progress-slider::-webkit-slider-track {
  background: transparent;
  border: none;
  height: 4px;
  -webkit-appearance: none;
}

.progress-slider::-moz-range-track {
  background: transparent;
  border: none;
  height: 4px;
}

.progress-slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 16px;
  height: 16px;
  background: var(--color-primary);
  border-radius: 50%;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.3);
  transition: all 0.2s ease;
  position: relative;
  z-index: 3;
}

.progress-slider::-webkit-slider-thumb:hover {
  background: var(--color-primary-dark);
  transform: scale(1.2);
  box-shadow: 0 3px 8px rgba(0, 0, 0, 0.4);
}

.progress-slider::-moz-range-thumb {
  width: 16px;
  height: 16px;
  background: var(--color-primary);
  border-radius: 50%;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.3);
  transition: all 0.2s ease;
  position: relative;
  z-index: 3;
}

.progress-slider::-moz-range-thumb:hover {
  background: var(--color-primary-dark);
  transform: scale(1.2);
  box-shadow: 0 3px 8px rgba(0, 0, 0, 0.4);
}

.progress-slider-wrapper::before {
  content: '';
  position: absolute;
  left: 0;
  top: 50%;
  transform: translateY(-50%);
  width: 100%;
  height: 4px;
  background: var(--color-dark-lighter);
  border-radius: 2px;
  pointer-events: none;
  z-index: 0;
}

.fa-spinner {
  color: var(--color-primary);
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

/* Responsive */
@media (max-width: 768px) {
  .control-button {
    width: 36px;
    height: 36px;
    font-size: 1rem;
  }
  
  .control-button-primary {
    font-size: 1.8rem;
    width: 46px;
    height: 46px;
  }
  
  .progress-container {
    margin: 0.5rem 0;
  }
  
  .progress-slider-wrapper {
    height: 18px;
  }
  
  .progress-slider::-webkit-slider-thumb {
    width: 14px;
    height: 14px;
  }
  
  .progress-slider::-moz-range-thumb {
    width: 14px;
    height: 14px;
  }

  .repeat-one-indicator {
    width: 12px;
    height: 12px;
    font-size: 0.5rem;
  }
}
</style>