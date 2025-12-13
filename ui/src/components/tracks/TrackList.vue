<!-- TrackList.vue - Versión actualizada -->
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

    <!-- Mensaje de carga -->
    <div v-if="loading" class="text-center py-5">
      <i class="fas fa-spinner fa-spin fa-2x text-primary"></i>
      <p class="mt-2">Cargando canciones...</p>
    </div>

    <!-- Lista de canciones o mensaje de error -->
    <div v-else>
      <div v-if="errorLoading" class="alert alert-warning">
        <i class="fas fa-exclamation-triangle me-2"></i>
        No se pudieron cargar las canciones. Mostrando datos de ejemplo.
      </div>
      
      <!-- Mostrar canciones dummy si dummySongs es true y no hay canciones -->
      <div v-if="showDummySongs" class="alert alert-info">
        <i class="fas fa-info-circle me-2"></i>
        Mostrando canciones de ejemplo para demostración.
      </div>
      
      <div v-if="tracksToDisplay.length === 0 && !showDummySongs" class="text-center py-5">
        <i class="fas fa-music fa-3x text-muted"></i>
        <p class="mt-2">No hay canciones disponibles</p>
      </div>
      
      <div v-else class="tracks-list">
        <TrackItem 
          v-for="track in tracksToDisplay" 
          :key="track.id" 
          :track="track"
          :isCurrentTrack="currentSong && track.id === currentSong.id"
          :isPlaying="isPlaying && currentSong && track.id === currentSong.id"
          @track-selected="handleTrackSelection"
          @edit-track="handleEditTrack"
          @delete-track="handleDeleteTrack"
        />
      </div>
    </div>

    <TrackFilterModal ref="filterModal" />

    <!-- Modal para subir canción -->
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

    <!-- Modal para editar canción -->
    <EditTrack
      :show-modal="showEditModal"
      :track="selectedTrack"
      @close="closeEditModal"
      @track-updated="onTrackUpdated"
    />

    <!-- Modal para confirmar eliminación -->
    <div
      v-if="showDeleteModal"
      class="modal fade show"
      style="display: block; background-color: rgba(0,0,0,0.5)"
      role="dialog"
      @click.self="closeDeleteModal"
    >
      <div class="modal-dialog">
        <div class="modal-content bg-dark text-white">
          <div class="modal-header">
            <h5 class="modal-title">Confirmar Eliminación</h5>
            <button type="button" class="btn-close btn-close-white" @click="closeDeleteModal"></button>
          </div>
          <div class="modal-body">
            <p>¿Estás seguro de que deseas eliminar la canción <strong>"{{ selectedTrack ? selectedTrack.title : '' }}"</strong>?</p>
            <p class="text-danger">
              <i class="fas fa-exclamation-triangle me-2"></i>
              Esta acción no se puede deshacer.
            </p>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" @click="closeDeleteModal" :disabled="deleting">
              Cancelar
            </button>
            <button type="button" class="btn btn-danger" @click="confirmDelete" :disabled="deleting">
              <span v-if="deleting">
                <i class="fas fa-spinner fa-spin me-2"></i>Eliminando...
              </span>
              <span v-else>
                <i class="fas fa-trash me-2"></i>Eliminar
              </span>
            </button>
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
import EditTrack from '@/components/forms/EditTrack.vue'
import ApiService from '@/services/ApiService'

export default {
  name: 'TrackList',
  components: { 
    TrackItem, 
    TrackFilterModal, 
    AddTrack,
    EditTrack 
  },
  data() {
    return {
      showUploadModal: false,
      showEditModal: false,
      showDeleteModal: false,
      selectedTrack: null,
      loading: false,
      errorLoading: false,
      deleting: false,
      
      // Variable para mostrar canciones dummy
      dummySongs: true, // Cambia a true para mostrar canciones dummy cuando no hay canciones
      
      // Canciones dummy de ejemplo
      dummyTracks: [
        {
          id: -1,
          title: 'Bohemian Rhapsody',
          artist_names: ['Queen'],
          album_name: 'A Night at the Opera',
          duration_seconds: 354,
          artist: [-1],
          album: -1,
          isDummy: true
        },
        {
          id: -2,
          title: 'Imagine',
          artist_names: ['John Lennon'],
          album_name: 'Imagine',
          duration_seconds: 183,
          artist: [-2],
          album: -2,
          isDummy: true
        },
        {
          id: -3,
          title: 'Blinding Lights',
          artist_names: ['The Weeknd'],
          album_name: 'After Hours',
          duration_seconds: 200,
          artist: [-3],
          album: -3,
          isDummy: true
        }
      ]
    }
  },
  computed: {
    ...mapGetters('tracks', ['filteredTracks', 'hasActiveFilters']),
    ...mapState('player', ['loadingTrack', 'isPlaying', 'currentSong']),
    ...mapState('tracks', ['searchQuery', 'tracks', 'allTracks']),
    
    // Computed para determinar si mostrar canciones dummy
    showDummySongs() {
      return this.dummySongs && (this.filteredTracks.length === 0 || this.errorLoading)
    },
    
    // Computed para determinar qué canciones mostrar
    tracksToDisplay() {
      if (this.showDummySongs) {
        return this.dummyTracks
      }
      return this.filteredTracks
    }
  },
  methods: {
    ...mapActions('player', ['selectTrack', 'initializeAudioService']),
    ...mapActions('tracks', ['fetchSongs', 'updateSearchQuery', 'clearAllFilters', 'setTracks']),
    
    async handleTrackSelection(track) {
      // Si es una canción dummy, mostrar mensaje en lugar de reproducir
      if (track.isDummy) {
        alert('Esta es una canción de demostración. Las canciones reales se pueden reproducir cuando se carguen desde el servidor.')
        return
      }
      
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
    
    handleEditTrack(track) {
      // Si es una canción dummy, mostrar mensaje
      if (track.isDummy) {
        alert('Esta es una canción de demostración. La edición solo está disponible para canciones reales.')
        return
      }
      
      this.selectedTrack = track
      this.showEditModal = true
    },
    
    handleDeleteTrack(track) {
      // Si es una canción dummy, mostrar mensaje
      if (track.isDummy) {
        alert('Esta es una canción de demostración. La eliminación solo está disponible para canciones reales.')
        return
      }
      
      this.selectedTrack = track
      this.showDeleteModal = true
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
    
    closeEditModal() {
      this.showEditModal = false
      this.selectedTrack = null
    },
    
    closeDeleteModal() {
      this.showDeleteModal = false
      this.selectedTrack = null
      this.deleting = false
    },
    
    async confirmDelete() {
      if (!this.selectedTrack) return
      
      this.deleting = true
      
      try {
        await ApiService.deleteTrack(this.selectedTrack.id)
        
        // Actualizar la lista eliminando la canción
        const currentTracks = this.allTracks || this.tracks || []
        const updatedTracks = currentTracks.filter(track => track.id !== this.selectedTrack.id)
        
        // Actualizar tanto tracks como allTracks en el store
        this.setTracks(updatedTracks)
        this.$store.commit('tracks/SET_ALL_TRACKS', updatedTracks)
        
        // Si la canción actual que se está reproduciendo es la eliminada, parar la reproducción
        if (this.currentSong && this.currentSong.id === this.selectedTrack.id) {
          console.log('La canción que se está reproduciendo fue eliminada')
        }
        
        // Cerrar modal después de un breve retraso
        setTimeout(() => {
          this.closeDeleteModal()
          
          // Mostrar notificación de éxito
          alert('Canción eliminada exitosamente')
        }, 500)
        
      } catch (error) {
        console.error('Error deleting track:', error)
        alert('Error al eliminar la canción: ' + (error.response?.data?.message || error.message))
        this.deleting = false
      }
    },
    
    async onTrackCreated() {
      this.closeUploadModal()
      await this.loadTracks()
    },
    
    async onTrackUpdated() {
      this.closeEditModal()
      await this.loadTracks()
    },
    
    clearFilters() {
      this.clearAllFilters()
    },
    
    async loadTracks() {
      this.loading = true
      this.errorLoading = false
      
      try {
        await this.fetchSongs()
        
        // Si se cargaron canciones exitosamente, verificar si hay canciones
        const currentTracks = this.allTracks || this.tracks || []
        if (currentTracks.length === 0 && this.dummySongs) {
          console.log('No hay canciones en el servidor, pero dummySongs es true - mostrando canciones de ejemplo')
        }
      } catch (error) {
        console.error('Error loading tracks:', error)
        this.errorLoading = true
        
        // Usar datos dummy si la API falla y dummySongs es true
        if (this.dummySongs) {
          console.log('Mostrando canciones dummy debido a error en la API')
        }
      } finally {
        this.loading = false
      }
    }
  },
  async mounted() {
    await this.loadTracks()
    
    // Mostrar mensaje si estamos usando canciones dummy
    if (this.dummySongs && (this.filteredTracks.length === 0 || this.errorLoading)) {
      console.log('Mostrando canciones de demostración')
    }
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

/* Estilos adicionales para las canciones dummy */
.alert-info {
  background-color: rgba(13, 110, 253, 0.1);
  border-color: rgba(13, 110, 253, 0.2);
  color: #0dcaf0;
}
</style>