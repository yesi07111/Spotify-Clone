<!-- TrackList.vue-->
<template>
  <div class="tracks-list-container">
    <AuthRequiredMessage
  v-if="showAuthMessage"
  :title="authMessageConfig.title"
  :message="authMessageConfig.message"
  :reasons="authMessageConfig.reasons"
  @close="showAuthMessage = false"
  @login="openAuthModal"
  />

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
             
           <!-- Dropdown para subir contenido -->
          <div class="btn-group">
            <button 
              type="button" 
              class="btn btn-primary dropdown-toggle" 
              data-bs-toggle="dropdown" 
              aria-expanded="false"
            >
              <i class="fas fa-upload me-2"></i>Subir
            </button>
            <ul class="dropdown-menu dropdown-menu-end">
              <li>
                <button class="dropdown-item" type="button" @click="openUploadModal('track')">
                  <i class="fas fa-music me-2"></i>Canción
                </button>
              </li>
              <li>
                <button class="dropdown-item" type="button" @click="openUploadModal('album')">
                  <i class="fas fa-compact-disc me-2"></i>Álbum
                </button>
              </li>
              <li>
                <button class="dropdown-item" type="button" @click="openUploadModal('artist')">
                  <i class="fas fa-user me-2"></i>Artista
                </button>
              </li>
            </ul>
          </div>
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

    <!-- Contenido cuando NO está cargando -->
    <div v-else>
      <!-- Error de carga -->
      <div v-if="errorLoading" class="alert alert-warning">
        <i class="fas fa-exclamation-triangle me-2"></i>
        No se pudieron cargar las canciones. Mostrando datos de ejemplo.
      </div>

      <!-- Mostrar canciones dummy -->
      <div v-if="showDummySongs" class="alert alert-info">
        <i class="fas fa-info-circle me-2"></i>
        Mostrando canciones de ejemplo. Para subir tus propias canciones, iniciar sesión.
      </div>

      <!-- Estado vacío REAL -->
      <div
        v-if="hasTriedToLoad && !loading && tracksToDisplay.length === 0 && !showDummySongs"
        class="text-center py-5"
      >
        <i class="fas fa-music fa-3x text-muted"></i>
        <p class="mt-2">No hay canciones disponibles</p>
      </div>

      <!-- Lista de canciones -->
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

        <!-- Modal para crear álbum -->
    <div
      v-if="showAlbumModal"
      class="modal fade show"
      style="display: block"
      role="dialog"
      @click.self="closeAlbumModal"
    >
      <div class="modal-dialog modal-lg">
        <div class="modal-content bg-dark text-white">
          <div class="modal-header">
            <h5 class="modal-title">Crear Álbum</h5>
            <button type="button" class="btn-close btn-close-white" @click="closeAlbumModal"></button>
          </div>
          <div class="modal-body">
            <AddAlbum @created="onAlbumCreated" />
          </div>
        </div>
      </div>
    </div>

    <!-- Modal para crear artista -->
    <div
      v-if="showArtistModal"
      class="modal fade show"
      style="display: block"
      role="dialog"
      @click.self="closeArtistModal"
    >
      <div class="modal-dialog">
        <div class="modal-content bg-dark text-white">
          <div class="modal-header">
            <h5 class="modal-title">Crear Artista</h5>
            <button type="button" class="btn-close btn-close-white" @click="closeArtistModal"></button>
          </div>
          <div class="modal-body">
            <AddArtist @created="onArtistCreated" />
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
import AddAlbum from '@/components/forms/AddAlbum.vue'
import AddArtist from '@/components/forms/AddArtist.vue'
import ApiService from '@/services/ApiService'
import AuthRequiredMessage from '@/components/messages/AuthRequiredMessage.vue'
import { UIState } from '@/store/ui'

export default {
  name: 'TrackList',
  components: { 
    TrackItem, 
    TrackFilterModal, 
    AddTrack,
    EditTrack,
    AddAlbum,    
    AddArtist,    
    AuthRequiredMessage
  },
  data() {
    return {
      showUploadModal: false,
      showEditModal: false,
      showDeleteModal: false,
      showAlbumModal: false,
      showArtistModal: false,
      selectedTrack: null,
      loading: false,
      errorLoading: false,
      deleting: false,
      isLoadingTracks: false,
      hasTriedToLoad: false,
      
      // Estado para cartel de autenticación requerida
      showAuthMessage: false,
      authMessageConfig: {
        title: '',
        message: '',
        reasons: []
      },

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
      return !this.isAuthenticated && 
            (this.filteredTracks.length === 0 || this.errorLoading)
    },

    // Agregar computed para búsqueda en dummy
    dummyTracksFiltered() {
      if (!this.searchQuery) return this.dummyTracks
      
      const query = this.searchQuery.toLowerCase()
      return this.dummyTracks.filter(track => 
        track.title.toLowerCase().includes(query) ||
        track.artist_names.some(artist => artist.toLowerCase().includes(query)) ||
        track.album_name.toLowerCase().includes(query)
      )
    },
    
    // Computed para determinar qué canciones mostrar
    tracksToDisplay() {
      if (this.showDummySongs) {
        return this.dummyTracksFiltered
      }
      return this.filteredTracks
    },

    // Agregar computed para autenticación
  //   isAuthenticated() {
  //     const AuthService = require('@/services/AuthService').default
  //     return AuthService.isAuthenticated()
  // },
  isAuthenticated() {
      return UIState.isAuthenticated
    }
},

 watch: {
  isAuthenticated(newVal) {
    console.log('[TRACKLIST] isAuthenticated cambió:', newVal)

    if (newVal) {
      this.dummySongs = false
      this.loadTracks()
    } else {
      // ✅ En su lugar, usar acción del store
      this.$store.dispatch('tracks/clearTracksOnLogout')
      this.dummySongs = true
    }
  }
},
  methods: {
    ...mapActions('player', ['selectTrack', 'initializeAudioService']),
    ...mapActions('tracks', ['fetchSongs', 'updateSearchQuery', 'clearAllFilters', 'setTracks']),
    
    openAuthModal() {
      UIState.showAuthModal = true
    },

    async onLoginSuccess() {
  console.log('✅ [LOGIN SUCCESS] Evento recibido')
  
  this.showAuthMessage = false
  this.dummySongs = false
  
  // ✅ Resetear estado de carga
  this.isLoadingTracks = false
  this.loading = false
  this.errorLoading = false
  
  // ✅ Forzar recarga
  console.log('✅ [LOGIN SUCCESS] Recargando canciones...')
  await this.loadTracks()
  
  console.log(`✅ [LOGIN SUCCESS] Canciones cargadas: ${this.filteredTracks.length}`)
},

    async handleTrackSelection(track) {
      if (track.isDummy) {
        console.log('Mostrando toast...')
        this.requireAuth({
        title: 'Reproducir canciones',
        message: 'Necesitas iniciar sesión para subir tus propias canciones y reproducirlas.',
        reasons: [
          '¡Estos ejemplos son solo texto!',
          '¡No son canciones reales!',
          'Perdona la inconveniencia... 🙇🏻‍♀️🙇🏿‍♀️'
        ]
        })
        
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
      const query = event.target ? event.target.value : event
      console.log('[SEARCH] Búsqueda:', query)
      this.updateSearchQuery(query)
    },
    
    handleEditTrack(track) {
      if (track.isDummy) {
        this.requireAuth({
        title: 'Editar canciones',
        message: 'Necesitas iniciar sesión para subir tus propias canciones y si pones mal sus datos, ya podrás editarla.',
        reasons: [
          '¡Necesitamos estos ejemplos!',
          '¡Por favor no los edites!',
          '🙏🏻'
        ]
        })
        return
      }
      this.selectedTrack = track
      this.showEditModal = true
    },
    
    handleDeleteTrack(track) { 
      if (track.isDummy) {
        this.requireAuth({
        title: 'Borrar canciones',
        message: 'Necesitas iniciar sesión para subir tus propias canciones y ya si quieres, borrarlas.',
        reasons: [
          '¡Necesitamos estos ejemplos!',
          '¡Por favor no los borres!',
          '🙏🏻'
        ]
        })

        return
      }
      this.selectedTrack = track
      this.showDeleteModal = true
    },

    requireAuth({ title, message, reasons }) {
      this.authMessageConfig = {
        title,
        message,
        reasons
      }
      this.showAuthMessage = true
    },
    
    openFilterModal() {
      if (!this.isAuthenticated) {
        this.requireAuth({
          title: 'Filtrar canciones',
          message: 'Necesitas iniciar sesión para subir tus propias canciones y filtrarlas 😅.',
          reasons: [
            'No hay nada que filtrar aquí',
          ]
        })

        return
      }
      if (this.$refs.filterModal) {
        this.$refs.filterModal.openModal()
      }
    },

    openUploadModal(type = 'track') {
      if (!this.isAuthenticated) {
        this.requireAuth({
          title: 'Subir contenido',
          message: 'Necesitas iniciar sesión para subir contenido.',
          reasons: [
            'Sino, ¿cómo sabemos quien eres tu?',
            '¿O cuales canciones son tuyas?',
            'Think, Mark, think.jpg' 
          ]
        })
        return
      }
      
      switch(type) {
        case 'track':
          this.showUploadModal = true
          break
        case 'album':
          this.showAlbumModal = true
          break
        case 'artist':
          this.showArtistModal = true
          break
      }
    },

    // Agregar nuevos métodos:
    closeAlbumModal() {
      this.showAlbumModal = false
    },

    closeArtistModal() {
      this.showArtistModal = false
    },

    onAlbumCreated() {
      
    },

    onArtistCreated() {
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
        
        // Obtener tracks actualizados del servidor
        await this.fetchSongs()
        
        // Si la canción actual que se está reproduciendo es la eliminada, parar la reproducción
        if (this.currentSong && this.currentSong.id === this.selectedTrack.id) {
          this.$store.dispatch('player/stopTrack')
        }
        
        // Mostrar mensaje de éxito
        this.requireAuth({
          title: 'Éxito',
          message: 'La canción se eliminó con éxito.',
          reasons: [
    
          ]
        })
        
        this.closeDeleteModal()
        
      } catch (error) {
        console.error('Error deleting track:', error)
        this.requireAuth({
          title: 'Error',
          message: 'Error al eliminar la canción. ',
          reasons: [
          (error.response?.data?.message || error.message)
          ]
        })
        this.deleting = false
      }
    },

    clearFilters() {
      this.clearAllFilters()
    },
   
    // Elimina loadTracksWithRetry por completo y usa esta versión simplificada de loadTracks:
    async loadTracks() {
  console.log('🔍 [LOAD TRACKS] Iniciando...')
  console.log('🔍 [LOAD TRACKS] isLoadingTracks:', this.isLoadingTracks)
  console.log('🔍 [LOAD TRACKS] isAuthenticated:', this.isAuthenticated)
  
  if (this.isLoadingTracks) {
    console.log('⚠️ [LOAD TRACKS] Ya hay carga en progreso')
    return
  }
  
  this.isLoadingTracks = true
  this.loading = true
  this.errorLoading = false
  this.hasTriedToLoad = true
  
  try {
    console.log('🔍 [LOAD TRACKS] Llamando a fetchSongs()...')
    await this.fetchSongs()
    
    const currentTracks = this.allTracks || this.tracks || []
    console.log(`✅ [LOAD TRACKS] Canciones obtenidas: ${currentTracks.length}`)
    
    if (currentTracks.length > 0) {
      this.dummySongs = false
    } else if (this.isAuthenticated) {
      this.dummySongs = false
      console.log('ℹ️ [LOAD TRACKS] Autenticado pero sin canciones')
    }
    
  } catch (error) {
    console.error('❌ [LOAD TRACKS] Error:', error)
    this.errorLoading = true
    
    if (!this.isAuthenticated && this.dummySongs) {
      console.log('ℹ️ [LOAD TRACKS] Mostrando canciones dummy')
    }
  } finally {
    this.loading = false
    this.isLoadingTracks = false
    console.log('🔍 [LOAD TRACKS] Finalizado. dummySongs:', this.dummySongs)
  }
},

    async onTrackCreated() {
      this.closeUploadModal()
      console.log('[TRACK CREATED] Canción creada, recargando lista...')
      // Pequeño delay para asegurar que el backend procesó la creación
      await new Promise(resolve => setTimeout(resolve, 500))
      await this.loadTracks()
    },

    async onTrackUpdated() {
      this.closeEditModal()
      console.log('[TRACK UPDATED] Canción actualizada, recargando lista...')
      await new Promise(resolve => setTimeout(resolve, 300))
      await this.loadTracks()
    },

   mounted() {
    console.log('✅ [MOUNTED] TrackList montado')

    if (this.isAuthenticated) {
      this.dummySongs = false
      this.loadTracks()
    } else {
      this.dummySongs = true
    }
    }
  }
}
</script>

<style scoped>

.demo-alert {
  margin-top: 2rem;
  margin-bottom: 2rem;
  animation: slideDown 0.5s ease-out;
}

.alert-demo {
  background: linear-gradient(135deg, rgba(13, 110, 253, 0.15) 0%, rgba(13, 202, 240, 0.1) 100%);
  border: 1px solid rgba(13, 110, 253, 0.3);
  border-radius: 10px;
  color: #e9ecef;
}

.search-filter-container {
  margin-bottom: 1rem;
}

@keyframes slideDown {
  from {
    opacity: 0;
    transform: translateY(-20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

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
  margin-top: 2rem; /* Agregar este margen */
  margin-bottom: 1.5rem; /* Mantener separación */
}
</style>