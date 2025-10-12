<template>
  <div class="current-track-container">
    <div class="current-track-info">
      <img 
        :src="currentTrackImage" 
        :alt="currentSong.title" 
        class="current-track-image"
      >
      <div class="current-track-title">{{ currentSong.title }}</div>
      <div class="current-track-details">
        {{ Array.isArray(currentSong.artist_names) ? currentSong.artist_names.join(', ') : '' }} • {{ currentSong.album_name }}
      </div>
    </div>
    
    <PlayerControls />
    
    <div class="volume-control-container">
      <i 
        class="fas volume-icon" 
        :class="volumeIcon"
        :style="{ color: volumeColor }"
        @click="toggleMute"
        :title="isMuted ? 'Activar sonido' : 'Silenciar'"
      ></i>
      <div class="volume-slider-wrapper">
        <input 
          type="range" 
          class="volume-slider" 
          min="0" 
          max="100" 
          :value="volume" 
          @input="handleVolumeChange"
          aria-label="Control de volumen"
        >
      </div>
      <span class="volume-percentage" :style="{ color: volumeColor }">{{ volume }}%</span>
    </div>
  </div>
</template>

<script>
import { mapState, mapActions, mapGetters } from 'vuex'
import PlayerControls from '@/components/player/PlayerControls.vue'
import { getTrackImageUrl } from '@/utils/imageHelper'

export default {
  name: 'CurrentTrack',
  components: {
    PlayerControls
  },
  computed: {
    ...mapState('player', ['currentSong', 'volume', 'isMuted']),
    ...mapGetters('player', ['volumeIcon', 'volumeColor']),
    
    currentTrackImage() {
      return getTrackImageUrl(this.currentSong.title)
    }
  },
  methods: {
    ...mapActions('player', ['setVolume', 'toggleMute']),
    
    handleVolumeChange(event) {
      this.setVolume(event)
    }
  }
}
</script>

<style scoped>
.volume-control-container {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 1rem;
}

.volume-icon {
  font-size: 1.25rem;
  cursor: pointer;
  transition: all 0.3s ease;
  min-width: 24px;
}

.volume-icon:hover {
  transform: scale(1.1);
  filter: brightness(1.2);
}

.volume-slider-wrapper {
  flex: 1;
  position: relative;
}

.volume-slider {
  width: 100%;
  height: 4px;
  -webkit-appearance: none;
  appearance: none;
  background: var(--color-dark-lighter);
  border-radius: 2px;
  outline: none;
  cursor: pointer;
}

.volume-slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 14px;
  height: 14px;
  background: white;
  border-radius: 50%;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
  transition: all 0.2s ease;
}

.volume-slider::-webkit-slider-thumb:hover,
.volume-slider::-webkit-slider-thumb:active {
  background: #0d6efd;
  border-color: #0d6efd;
  transform: scale(1.2);
}

.volume-slider::-moz-range-thumb {
  width: 14px;
  height: 14px;
  background: white;
  border-radius: 50%;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
  transition: all 0.2s ease;
}

.volume-slider::-moz-range-thumb:hover,
.volume-slider::-moz-range-thumb:active {
  background: #0d6efd;
  border-color: #0d6efd;
  transform: scale(1.2);
}

.volume-percentage {
  font-size: 0.875rem;
  min-width: 45px;
  text-align: right;
  font-weight: 500;
  transition: color 0.3s ease;
}
</style>
