<template>
  <div class="player-controls-container">
    <div class="control-buttons">
      <button 
        class="control-button" 
        :class="{ 'control-button-active': isShuffleActive }"
        @click="toggleShuffle"
        aria-label="Activar mezcla aleatoria"
      >
        <i class="fas fa-random"></i>
      </button>
      <button 
        class="control-button" 
        @click="playPreviousTrack"
        aria-label="Reproducir canción anterior"
      >
        <i class="fas fa-step-backward"></i>
      </button>
      <button 
        class="control-button control-button-primary" 
        @click="togglePlayback"
        aria-label="Reproducir o pausar"
      >
        <i class="fas" :class="isPlaying ? 'fa-pause-circle' : 'fa-play-circle'"></i>
      </button>
      <button 
        class="control-button" 
        @click="playNextTrack"
        aria-label="Reproducir siguiente canción"
      >
        <i class="fas fa-step-forward"></i>
      </button>
      <button 
        class="control-button" 
        :class="{ 'control-button-active': isRepeatActive }"
        @click="toggleRepeat"
        aria-label="Activar repetición"
      >
        <i class="fas fa-redo"></i>
      </button>
    </div>

    <div class="progress-container">
      <span class="progress-time">{{ formattedElapsedTime }}</span>
      <input 
        type="range" 
        class="progress-slider" 
        min="0" 
        max="100" 
        :value="playbackProgress" 
        @input="seekToPosition"
        aria-label="Barra de progreso de la canción"
      >
      <span class="progress-time">{{ formattedTotalDuration }}</span>
    </div>
  </div>
</template>

<script>
import { mapState, mapActions } from 'vuex'

export default {
  name: 'PlayerControls',
  computed: {
    ...mapState('player', [
      'isPlaying',
      'formattedElapsedTime',
      'formattedTotalDuration',
      'playbackProgress',
      'isShuffleActive',
      'isRepeatActive'
    ])
  },
  methods: {
    ...mapActions('player', [
      'togglePlayback',
      'playNextTrack',
      'playPreviousTrack',
      'seekToPosition',
      'toggleShuffle',
      'toggleRepeat'
    ])
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>