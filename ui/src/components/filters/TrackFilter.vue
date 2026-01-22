<!-- filters/TrackFilter.vue -->
<template>
  <div
    v-if="visible"
    class="modal fade show"
    style="display: block"
    role="dialog"
    @click.self="closeModal"
  >
    <div class="modal-dialog">
      <div class="modal-content bg-dark text-white">
        <div class="modal-header">
          <h5 class="modal-title">Filtrar por Artista y Álbum</h5>
          <button type="button" class="btn-close btn-close-white" @click="closeModal"></button>
        </div>

        <div class="modal-body">
          <form @submit.prevent="submitForm">
            <div class="mb-3">
              <label for="artistSelect" class="form-label">Seleccionar Artistas</label>
              <select
                id="artistSelect"
                v-model="selectedArtists"
                class="form-control"
                multiple
                size="5"
                :disabled="loadingArtists"
              >
                <option v-for="artist in artists" :key="artist.id" :value="artist.id">
                  {{ artist.name }}
                </option>
              </select>
              <div class="form-text" v-if="loadingArtists">Cargando artistas...</div>
              <div class="form-text">Mantén Ctrl/Cmd para seleccionar múltiples</div>
              <div class="text-danger small" v-if="errorArtists">{{ errorArtists }}</div>
            </div>

            <div class="mb-3">
              <label for="albumSelect" class="form-label">Seleccionar Álbum</label>
              <select
                id="albumSelect"
                v-model="selectedAlbum"
                class="form-control"
                :disabled="loadingAlbums"
              >
                <option :value="null">Todos los álbumes</option>
                <option v-for="album in albums" :key="album.id" :value="album.id">
                  {{ album.name }}
                </option>
              </select>
              <div class="form-text" v-if="loadingAlbums">Cargando álbumes...</div>
              <div class="text-danger small" v-if="errorAlbums">{{ errorAlbums }}</div>
            </div>

            <div class="modal-footer">
              <button type="button" class="btn btn-secondary" @click="closeModal">
                Cancelar
              </button>
              <button type="submit" class="btn btn-primary" :disabled="isSubmitting">
                Aplicar Filtros
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { mapActions } from 'vuex'


export default {
  name: 'TrackFilterModal',
  data() {
    return {
      selectedArtists: [],
      selectedAlbum: null,
      artists: [],
      albums: [],
      visible: false,
      loadingArtists: false,
      loadingAlbums: false,
      errorArtists: null,
      errorAlbums: null,
      isSubmitting: false
    }
  },
  methods: {
    ...mapActions('tracks', ['setFilters']),
    
    async openModal() {
      this.visible = true
      this.selectedArtists = []
      this.selectedAlbum = null
      await Promise.all([this.loadArtists(), this.loadAlbums()])
    },
    
    closeModal() {
      this.visible = false
    },
    
    async loadArtists() {
  this.loadingArtists = true
  this.errorArtists = null
  try {
    const ApiService = require('@/services/ApiService').default
    const response = await ApiService.getArtists()
    this.artists = Array.isArray(response.data) ? response.data : []
    console.log('[FILTER] Artistas cargados:', this.artists.length)
  } catch (err) {
    this.errorArtists = 'Error al cargar artistas'
    console.error('Error loading artists:', err)
  } finally {
    this.loadingArtists = false
  }
},

async loadAlbums() {
  this.loadingAlbums = true
  this.errorAlbums = null
  try {
    const ApiService = require('@/services/ApiService').default
    const response = await ApiService.getAlbums()
    this.albums = Array.isArray(response.data) ? response.data : []
    console.log('[FILTER] Álbumes cargados:', this.albums.length)
  } catch (err) {
    this.errorAlbums = 'Error al cargar álbumes'
    console.error('Error loading albums:', err)
  } finally {
    this.loadingAlbums = false
  }
},
    
    async submitForm() {
      this.isSubmitting = true
      try {
        await this.setFilters({
          artists: this.selectedArtists,
          album: this.selectedAlbum
        })
        this.closeModal()
      } catch (err) {
        console.error('Error applying filters:', err)
      } finally {
        this.isSubmitting = false
      }
    }
  }
}
</script>

<style scoped>
.modal-content {
  border-radius: 8px;
}

select[multiple] {
  min-height: 150px;
}

.form-control:focus {
  border-color: #0d6efd;
  box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
}
</style>