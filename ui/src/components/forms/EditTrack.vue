<!-- components/forms/EditTrack.vue -->
<template>
  <div 
    v-if="showModal"
    class="modal fade show"
    style="display: block; background-color: rgba(0,0,0,0.5)"
    role="dialog"
    @click.self="closeModal"
  >
    <div class="modal-dialog modal-lg">
      <div class="modal-content bg-dark text-white">
        <div class="modal-header">
          <h5 class="modal-title">Editar Canción</h5>
          <button type="button" class="btn-close btn-close-white" @click="closeModal"></button>
        </div>
        <div class="modal-body">
          <form @submit.prevent="submitForm" novalidate>
            <div class="mb-3">
              <label for="editTrackTitle" class="form-label">Título de la Canción *</label>
              <input
                id="editTrackTitle"
                type="text"
                class="form-control"
                v-model="formData.title"
                placeholder="Ingresa el título de la canción"
                required
              />
            </div>

            <!-- Artista - Seleccionar existente -->
            <div class="mb-3">
              <label for="editArtistSelect" class="form-label">Artista *</label>
              <select
                id="editArtistSelect"
                class="form-select"
                v-model="formData.artistIds"
                multiple
                required
                :disabled="artists.length === 0"
              >
                <option v-if="artists.length === 0" disabled value="">No hay artistas disponibles</option>
                <option v-for="artist in artists" :key="artist.id" :value="artist.id">
                  {{ artist.name }}
                </option>
              </select>
              <div class="form-text" v-if="artists.length === 0">
                No hay artistas en la base de datos.
              </div>
            </div>

            <!-- Álbum - Seleccionar existente -->
            <div class="mb-3">
              <label for="editAlbumSelect" class="form-label">Álbum *</label>
              <select
                id="editAlbumSelect"
                class="form-select"
                v-model="formData.albumId"
                required
                :disabled="albums.length === 0"
              >
                <option v-if="albums.length === 0" disabled value="">No hay álbumes disponibles</option>
                <option v-for="album in albums" :key="album.id" :value="album.id">
                  {{ album.name }}
                </option>
              </select>
              <div class="form-text" v-if="albums.length === 0">
                No hay álbumes en la base de datos.
              </div>
            </div>

            <div class="d-flex justify-content-end gap-2">
              <button type="button" class="btn btn-secondary" @click="closeModal" :disabled="submitting">
                Cancelar
              </button>
              <button type="submit" class="btn btn-primary" :disabled="submitting">
                <span v-if="submitting">
                  <i class="fas fa-spinner fa-spin me-2"></i>Guardando...
                </span>
                <span v-else>
                  <i class="fas fa-save me-2"></i>Guardar Cambios
                </span>
              </button>
            </div>

            <div class="mt-3" v-if="errorMessage">
              <div class="alert alert-danger" role="alert">
                <i class="fas fa-exclamation-triangle me-2"></i>{{ errorMessage }}
              </div>
            </div>

            <div class="mt-3" v-if="successMessage">
              <div class="alert alert-success" role="alert">
                <i class="fas fa-check me-2"></i>{{ successMessage }}
              </div>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import ApiService from '@/services/ApiService'

export default {
  name: 'EditTrack',
  props: {
    showModal: {
      type: Boolean,
      default: false
    },
    track: {
      type: Object,
      default: null
    }
  },
  data() {
    return {
      formData: {
        title: '',
        albumId: null,
        artistIds: []
      },
      albums: [],
      artists: [],
      submitting: false,
      errorMessage: null,
      successMessage: null
    }
  },
  watch: {
    track: {
      immediate: true,
      handler(newTrack) {
        if (newTrack) {
          this.loadTrackData()
        }
      }
    }
  },
  methods: {
    async loadTrackData() {
      if (!this.track) return
      
      this.formData.title = this.track.title || ''
      this.formData.albumId = this.track.album || null
      this.formData.artistIds = this.track.artist || []
      
      await this.fetchAlbums()
      await this.fetchArtists()
    },
    
    async fetchAlbums() {
      try {
        this.albums = await ApiService.getAlbums()
      } catch (err) {
        console.error('Error fetching albums:', err)
        this.albums = []
      }
    },
    
    async fetchArtists() {
      try {
        this.artists = await ApiService.getArtists()
      } catch (err) {
        console.error('Error fetching artists:', err)
        this.artists = []
      }
    },
    
    async submitForm() {
      this.errorMessage = null
      this.successMessage = null

      // Validaciones
      if (!this.formData.title || !this.formData.albumId || this.formData.artistIds.length === 0) {
        this.errorMessage = 'Todos los campos son obligatorios'
        return
      }

      this.submitting = true

      try {
        // Preparar los datos para PATCH (solo campos modificados)
        const patchData = {}
        
        if (this.formData.title !== this.track.title) {
          patchData.title = this.formData.title
        }
        
        if (this.formData.albumId !== this.track.album) {
          patchData.album = this.formData.albumId
        }
        
        // Comparar arrays de artistas
        const currentArtists = Array.isArray(this.track.artist) ? this.track.artist : []
        const newArtists = Array.isArray(this.formData.artistIds) ? this.formData.artistIds : []
        
        if (JSON.stringify([...currentArtists].sort()) !== JSON.stringify([...newArtists].sort())) {
          patchData.artist = newArtists
        }
        
        // Solo enviar si hay cambios
        if (Object.keys(patchData).length > 0) {
          await ApiService.updateTrack(this.track.id, patchData)
          this.successMessage = 'Canción actualizada exitosamente'
          
          // Emitir evento de éxito
          this.$emit('track-updated')
          
          // Cerrar después de 2 segundos
          setTimeout(() => {
            this.closeModal()
          }, 2000)
        } else {
          this.successMessage = 'No se detectaron cambios'
          setTimeout(() => {
            this.closeModal()
          }, 1500)
        }
        
      } catch (err) {
        this.errorMessage = 'Error al actualizar la canción: ' + (err.response?.data?.message || err.message)
        console.error('Error updating track:', err)
      } finally {
        this.submitting = false
      }
    },
    
    closeModal() {
      this.$emit('close')
      this.resetForm()
    },
    
    resetForm() {
      this.formData = {
        title: '',
        albumId: null,
        artistIds: []
      }
      this.errorMessage = null
      this.successMessage = null
    }
  },
  mounted() {
    if (this.track) {
      this.loadTrackData()
    }
  }
}
</script>