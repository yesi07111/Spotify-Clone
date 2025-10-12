<template>
  <div class="tracks-list-container">
    <div class="search-filter-container">
      <div class="row">
        <div class="col-md-6">
          <div class="input-group">
            <input
              type="text"
              class="form-control"
              placeholder="Buscar canciones..."
              :value="searchQuery"
              @input="handleSearch"
              @keyup.enter="handleSearch"
            />
            <button class="btn btn-outline-secondary" type="button">
              <i class="fas fa-search"></i>
            </button>
          </div>
        </div>
        <div class="col-md-6 text-end">
          <button 
            class="btn btn-outline-light me-2" 
            :class="{ 'btn-primary': hasActiveFilters }"
            @click="openFilterModal"
          >
            <i class="fas fa-filter me-2"></i>
            Filtrar
            <span v-if="hasActiveFilters" class="badge bg-danger ms-2">Activo</span>
          </button>
          <button class="btn btn-primary" @click="openUploadModal">
            <i class="fas fa-upload me-2"></i>Subir Canción
          </button>
        </div>
      </div>
    </div>

    <div v-if="hasActiveFilters" class="active-filters-bar">
      <span class="me-2">Filtros activos:</span>
      <button class="btn btn-sm btn-outline-danger" @click="clearFilters">
        <i class="fas fa-times me-1"></i>Limpiar filtros
      </button>
    </div>

    <div class="tracks-list">
      <TrackItem 
        v-for="track in filteredTracks" 
        :key="track.id" 
        :track="track"
        :isCurrentTrack="currentSong && track.id === currentSong.id"
        :isPlaying="isPlaying && currentSong && track.id === currentSong.id"
        @track-selected="handleTrackSelection"
      />
    </div>

    <TrackFilterModal ref="filterModal" />

    <div
      v-if="showUploadModal"
      class="modal fade show"
      style="display: block"
      role="dialog"
      @click.self="closeUploadModal"
    >
      <div class="modal-dialog modal-lg">
        <div class="modal-content bg-dark text-white">
          <div class="modal-header">
            <h5 class="modal-title">Subir Canción</h5>
            <button type="button" class="btn-close btn-close-white" @click="closeUploadModal"></button>
          </div>
          <div class="modal-body">
            <AddTrack @created="onTrackCreated" />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { mapGetters, mapActions, mapState } from 'vuex'
import TrackItem from './TrackItem.vue'
import TrackFilterModal from '@/components/filters/TrackFilter.vue'
import AddTrack from '@/components/forms/AddTrack.vue'

export default {
  name: 'TrackList',
  components: { TrackItem, TrackFilterModal, AddTrack },
  data() {
    return {
      showUploadModal: false
    }
  },
  computed: {
    ...mapGetters('tracks', ['filteredTracks', 'hasActiveFilters']),
    ...mapState('player', ['loadingTrack', 'isPlaying', 'currentSong']),
    ...mapState('tracks', ['searchQuery'])
  },
  methods: {
    ...mapActions('player', ['selectTrack', 'initializeAudioService']),
    ...mapActions('tracks', ['fetchSongs', 'updateSearchQuery', 'clearAllFilters']),
    
    async handleTrackSelection(track) {
      try {
        if (!this.$store.getters['player/isAudioServiceReady']) {
          await this.initializeAudioService()
        }
        await this.selectTrack(track)
      } catch (error) {
        console.error('Error selecting track:', error)
        alert('Error al reproducir la canción')
      }
    },
    
    handleSearch(event) {
      this.updateSearchQuery(event.target.value)
    },
    
    openFilterModal() {
      if (this.$refs.filterModal) {
        this.$refs.filterModal.openModal()
      }
    },
    
    openUploadModal() {
      this.showUploadModal = true
    },
    
    closeUploadModal() {
      this.showUploadModal = false
    },
    
    async onTrackCreated() {
      this.closeUploadModal()
      await this.fetchSongs()
    },
    
    clearFilters() {
      this.clearAllFilters()
    }
  },
  async mounted() {
    await this.fetchSongs()
  }
}
</script>

<style scoped>
.active-filters-bar {
  padding: 0.5rem 1rem;
  background: rgba(220, 53, 69, 0.1);
  border-left: 3px solid #dc3545;
  margin: 1rem 0;
  border-radius: 4px;
  display: flex;
  align-items: center;
}
</style>
