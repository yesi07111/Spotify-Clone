<!-- TrackItem.vue -->
<template>
  <div 
    class="track-item" 
    :class="{ 
      'current-track': isCurrentTrack,
      'playing': isCurrentTrack && isPlaying 
    }"
    @click="selectTrack"
  >
    <div class="track-item-play-indicator">
      <i v-if="isCurrentTrack && isPlaying" class="fas fa-volume-up text-success"></i>
      <i v-else-if="isCurrentTrack" class="fas fa-pause text-warning"></i>
      <i v-else class="fas fa-play-circle track-item-play-icon"></i>
    </div>
    
    <img 
      :src="currentImageUrl" 
      :alt="trackTitle" 
      class="track-item-image"
      @error="handleImageError"
    >
    
    <div class="track-item-info">
      <div class="track-item-title">
        {{ trackTitle }}
        <span v-if="isCurrentTrack" class="current-track-badge">
          <i class="fas fa-music"></i>
        </span>
      </div>
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
    },
    isCurrentTrack: {
      type: Boolean,
      default: false
    },
    isPlaying: {
      type: Boolean,
      default: false
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
      if (Array.isArray(this.track.artist_names)) {
        return this.track.artist_names.join(', ') || 'Artista desconocido'
      }
      return 'Artista desconocido'
    },
    trackAlbum() {
      return this.track.album_name || 'Álbum desconocido'
    },
    trackDuration() {
      const seconds = Number(this.track.duration_seconds) || 0
      const minutes = Math.floor(seconds / 60)
      const remainingSeconds = seconds % 60
      return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`
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
      this.currentImageUrl = getTrackImageUrl(this.track.title)
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
<style scoped>
.track-item {
  display: flex;
  align-items: center;
  padding: 0.75rem;
  border-radius: 0.5rem;
  cursor: pointer;
  transition: all 0.2s ease;
  position: relative;
}

.track-item:hover {
  background-color: rgba(255, 255, 255, 0.05);
}

.track-item.current-track {
  background-color: rgba(13, 110, 253, 0.1);
  border-left: 3px solid #0d6efd;
}

.track-item.current-track.playing {
  background-color: rgba(13, 110, 253, 0.15);
  border-left-color: #0d6efd;
}

.track-item-play-indicator {
  width: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 0.75rem;
  color: #0d6efd;
}

.track-item-play-icon {
  opacity: 0;
  transition: opacity 0.2s ease;
  color: #6c757d;
}

.track-item:hover .track-item-play-icon {
  opacity: 1;
}

.track-item-image {
  width: 50px;
  height: 50px;
  border-radius: 0.375rem;
  object-fit: cover;
  margin-right: 0.75rem;
}

.track-item-info {
  flex: 1;
  min-width: 0;
}

.track-item-title {
  font-weight: 500;
  color: #fff;
  margin-bottom: 0.25rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: flex;
  align-items: center;
}

.current-track-badge {
  margin-left: 0.5rem;
  color: #0d6efd;
  font-size: 0.875rem;
}

.track-item-details {
  font-size: 0.875rem;
  color: #6c757d;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.track-item-duration {
  color: #6c757d;
  font-size: 0.875rem;
  margin-left: 1rem;
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.6;
  }
}

.track-item.playing .track-item-play-indicator i {
  animation: pulse 1.5s ease-in-out infinite;
}
</style>