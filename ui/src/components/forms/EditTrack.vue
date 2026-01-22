<!-- components/forms/EditTrack.vue - Template completo -->
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

            <!-- Artista - Seleccionar existente o crear nuevo -->
            <div class="mb-3">
              <div class="d-flex justify-content-between align-items-center mb-2">
                <label class="form-label">Artista</label>
                <button type="button" class="btn btn-sm btn-outline-info" @click="toggleNewArtist">
                  {{ showNewArtist ? 'Seleccionar existente' : 'Crear nuevo artista' }}
                </button>
              </div>
              
              <select
                v-if="!showNewArtist"
                id="editArtistSelect"
                class="form-select"
                v-model="formData.artistIds"
                multiple
                :disabled="artists.length === 0"
              >
                <option :value="null">Sin artista</option>
                <option v-for="artist in artists" :key="artist.id" :value="artist.id">
                  {{ artist.name }}
                </option>
              </select>
              
              <div v-else class="input-group">
                <input
                  type="text"
                  class="form-control"
                  v-model="newArtistName"
                  placeholder="Nombre del nuevo artista"
                />
                <button class="btn btn-outline-primary" type="button" @click="findOrCreateArtist">
                  <i class="fas fa-plus"></i>
                </button>
              </div>
              <div class="form-text">
                <span v-if="!showNewArtist && artists.length === 0">
                  No hay artistas en la base de datos. Crea uno nuevo.
                </span>
                <span v-else>
                  Puedes seleccionar múltiples artistas manteniendo presionada la tecla Ctrl
                </span>
              </div>
            </div>

            <!-- Álbum - Seleccionar existente o crear nuevo -->
            <div class="mb-3">
              <div class="d-flex justify-content-between align-items-center mb-2">
                <label class="form-label">Álbum</label>
                <button type="button" class="btn btn-sm btn-outline-info" @click="toggleNewAlbum">
                  {{ showNewAlbum ? 'Seleccionar existente' : 'Crear nuevo álbum' }}
                </button>
              </div>
              
              <select
                v-if="!showNewAlbum"
                id="editAlbumSelect"
                class="form-select"
                v-model="formData.albumId"
                :disabled="albums.length === 0"
              >
                <option :value="null">Sin álbum</option>
                <option v-for="album in albums" :key="album.id" :value="album.id">
                  {{ album.name }}
                </option>
              </select>
              
              <div v-else class="input-group">
                <input
                  type="text"
                  class="form-control"
                  v-model="newAlbumName"
                  placeholder="Nombre del nuevo álbum"
                />
                <button class="btn btn-outline-primary" type="button" @click="findOrCreateAlbum">
                  <i class="fas fa-plus"></i>
                </button>
              </div>
              <div class="form-text">
                <span v-if="!showNewAlbum && albums.length === 0">
                  No hay álbumes en la base de datos. Crea uno nuevo.
                </span>
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
import UploadService from '@/services/UploadService'

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
      successMessage: null,
      showNewArtist: false,
      showNewAlbum: false,
      newArtistName: '',
      newAlbumName: ''
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
      this.formData.artistIds = Array.isArray(this.track.artist) ? this.track.artist : []
      
      await this.fetchAlbums()
      await this.fetchArtists()
    },
    
    async fetchAlbums() {
      try {
        this.albums = await UploadService.getAlbums()
        if (this.albums.length === 0) {
          this.showNewAlbum = true
        }
      } catch (err) {
        console.error('fetchAlbums error', err)
        this.albums = []
        this.showNewAlbum = true
      }
    },

    async fetchArtists() {
      try {
        this.artists = await UploadService.getArtists()
        if (this.artists.length === 0) {
          this.showNewArtist = true
        }
      } catch (err) {
        console.error('fetchArtists error', err)
        this.artists = []
        this.showNewArtist = true
      }
    },

    
    toggleNewArtist() {
      this.showNewArtist = !this.showNewArtist
      this.newArtistName = ''
    },
    
    toggleNewAlbum() {
      this.showNewAlbum = !this.showNewAlbum
      this.newAlbumName = ''
    },
    
    async findOrCreateArtistByName(name) {
      const existing = this.artists.find(
        a => a.name.toLowerCase() === name.toLowerCase()
      )

      if (existing) return existing.id

      const created = await UploadService.createArtist({ name })
      this.artists.push(created)
      return created.id
    },

    async findOrCreateAlbumByName(name) {
      const existing = this.albums.find(
        a => a.name.toLowerCase() === name.toLowerCase()
      )

      if (existing) return existing.id

      const created = await UploadService.createAlbum({
        name,
        date: new Date().toISOString().split('T')[0],
        author: null
      })

      this.albums.push(created)
      return created.id
    },
        
    async submitForm() {
      this.errorMessage = null
      this.successMessage = null

      if (!this.formData.title) {
        this.errorMessage = 'El título es obligatorio'
        return
      }

      this.submitting = true

      try {
        if (this.showNewArtist && this.newArtistName.trim()) {
          const artistName = this.newArtistName.trim()

          const existingArtist = this.artists.find(
            a => a.name.toLowerCase() === artistName.toLowerCase()
          )

          let artistId
          if (existingArtist) {
            artistId = existingArtist.id
          } else {
            const createdArtist = await UploadService.createArtist({ name: artistName })
            this.artists.push(createdArtist)
            artistId = createdArtist.id
          }

          // Sustituye TODOS los artistas anteriores
          this.formData.artistIds = [artistId]
        }

        if (this.showNewAlbum && this.newAlbumName.trim()) {
          const albumName = this.newAlbumName.trim()

          const existingAlbum = this.albums.find(
            a => a.name.toLowerCase() === albumName.toLowerCase()
          )

          let albumId
          if (existingAlbum) {
            albumId = existingAlbum.id
          } else {
            const createdAlbum = await UploadService.createAlbum({
              name: albumName,
              date: new Date().toISOString().split('T')[0],
              author: this.formData.artistIds.length > 0
                ? this.formData.artistIds[0]
                : null
            })
            this.albums.push(createdAlbum)
            albumId = createdAlbum.id
          }

          this.formData.albumId = albumId
        }

        const patchData = {}

        if (this.formData.title !== this.track.title) {
          patchData.title = this.formData.title
        }

        if (this.formData.albumId !== this.track.album) {
          patchData.album = this.formData.albumId
        }

        const currentArtists = Array.isArray(this.track.artist) ? this.track.artist : []
        const newArtists = Array.isArray(this.formData.artistIds)
          ? this.formData.artistIds
          : []

        if (
          JSON.stringify([...currentArtists].sort()) !==
          JSON.stringify([...newArtists].sort())
        ) {
          patchData.artist = newArtists
        }

        if (Object.keys(patchData).length === 0) {
          this.errorMessage = 'No se detectaron cambios'
          return
        }

        const updated = await UploadService.updateTrack(this.track.id, patchData)

        this.$emit('track-updated', updated)
        this.closeModal()

      } catch (err) {
        this.errorMessage =
          'Error al actualizar la canción: ' +
          (err.response?.data?.message || err.message)
        console.error('[EDIT] Error updating track:', err)
      } finally {
        this.submitting = false
      }
    },
    
    closeModal() {
      this.resetForm()
      this.$emit('close')
    },
    
    resetForm() {
      this.formData = {
        title: '',
        albumId: null,
        artistIds: []
      }
      this.errorMessage = null
      this.successMessage = null
      this.showNewArtist = false
      this.showNewAlbum = false
      this.newArtistName = ''
      this.newAlbumName = ''
      this.submitting = false
    }
  },
  mounted() {
    if (this.track) {
      this.loadTrackData()
    }
  }
}
</script>