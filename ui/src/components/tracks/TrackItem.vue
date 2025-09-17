<template>
  <div class="track-item" @click="selectTrack">
    <img :src="trackImageUrl" :alt="trackTitle" class="track-item-image">
    <div class="track-item-info">
      <div class="track-item-title">{{ trackTitle }}</div>
      <div class="track-item-details">{{ trackArtist }} • {{ trackAlbum }}</div>
    </div>
    <div class="track-item-duration">{{ trackDuration }}</div>
  </div>
</template>

<script>
import { getTrackImageUrl } from '@/utils/imageHelper'

export default {
  name: 'TrackItem',
  props: {
    track: {
      type: Object,
      required: true
    }
  },
  computed: {
    trackImageUrl() {
      return getTrackImageUrl(this.track.title, this.track.id)
    },
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
  methods: {
    selectTrack() {
      this.$emit('track-selected', this.track)
    }
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>