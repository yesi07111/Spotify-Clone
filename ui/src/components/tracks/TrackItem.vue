<template>
  <div class="track-item" @click="selectTrack">
    <img 
      :src="currentImageUrl" 
      :alt="trackTitle" 
      class="track-item-image"
      @error="handleImageError"
    >
    <div class="track-item-info">
      <div class="track-item-title">{{ trackTitle }}</div>
      <div class="track-item-details">{{ trackArtist }} • {{ trackAlbum }}</div>
    </div>
    <div class="track-item-duration">{{ trackDuration }}</div>
  </div>
</template>

<script>
import { getTrackImageUrl, getRandomDefaultImage } from '@/utils/imageHelper'

export default {
  name: 'TrackItem',
  props: {
    track: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      currentImageUrl: '',
      imageError: false
    }
  },
  computed: {
    trackTitle() {
      return this.track.title || 'Título desconocido'
    },
    trackArtist() {
      return this.track.artist || 'Artista desconocido'
    },
    trackAlbum() {
      return this.track.album || 'Álbum desconocido'
    },
    trackDuration() {
      return this.track.duration || '0:00'
    }
  },
  mounted() {
    this.loadImage()
  },
  methods: {
    selectTrack() {
      this.$emit('track-selected', this.track)
    },
    loadImage() {
      this.currentImageUrl = getTrackImageUrl(this.track.title, this.track.id)
    },
    handleImageError() {
      if (!this.imageError) {
        this.imageError = true
        this.currentImageUrl = getRandomDefaultImage(this.track.id)
      }
    }
  },
  watch: {
    track: {
      handler() {
        this.imageError = false
        this.loadImage()
      },
      deep: true
    }
  }
}
</script>