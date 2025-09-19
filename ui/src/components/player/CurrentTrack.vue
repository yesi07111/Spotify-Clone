<template>
  <div class="current-track-container">
    <div class="current-track-info">
      <img 
        :src="currentImageUrl" 
        :alt="currentTrackTitle" 
        class="current-track-image"
        @error="handleImageError"
      >
      <h4 class="current-track-title">{{ currentTrackTitle }}</h4>
      <p class="current-track-details">{{ currentTrackArtist }} - {{ currentTrackAlbum }}</p>
    </div>
    <PlayerControls />
    <VolumeControl />
  </div>
</template>

<script>
import { mapState } from 'vuex'
import { getTrackImageUrl, getRandomDefaultImage } from '@/utils/imageHelper'
import PlayerControls from './PlayerControls.vue'
import VolumeControl from './VolumeControl.vue'

export default {
  name: 'CurrentTrack',
  components: {
    PlayerControls,
    VolumeControl
  },
  data() {
    return {
      currentImageUrl: '',
      imageError: false
    }
  },
  computed: {
    ...mapState('player', {
      currentTrack: state => state.currentTrack,
      isPlaying: state => state.isPlaying
    }),
    currentTrackTitle() {
      return this.currentTrack?.title || 'Selecciona una canción'
    },
    currentTrackArtist() {
      return this.currentTrack?.artist || 'Artista'
    },
    currentTrackAlbum() {
      return this.currentTrack?.album || 'Álbum'
    }
  },
  watch: {
    currentTrack: {
      handler(newTrack) {
        if (newTrack && newTrack.title) {
          this.imageError = false
          this.currentImageUrl = getTrackImageUrl(newTrack.title, newTrack.id)
        }
      },
      deep: true,
      immediate: true
    }
  },
  methods: {
    handleImageError() {
      if (!this.imageError && this.currentTrack?.id) {
        this.imageError = true
        this.currentImageUrl = getRandomDefaultImage(this.currentTrack.id)
      }
    }
  }
}
</script>