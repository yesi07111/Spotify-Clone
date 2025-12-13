<!-- AddTrack -->
<template>
  <div class="add-track-form card p-3 bg-dark text-white">
    <h5 class="mb-3">Subir Canción</h5>

    <form @submit.prevent="submitForm" novalidate>
      <div class="mb-3">
        <label for="trackTitle" class="form-label">Título de la Canción *</label>
        <input
          id="trackTitle"
          type="text"
          class="form-control"
          v-model="title"
          placeholder="Ingresa el título de la canción"
          required
        />
      </div>

      <!-- Artista - Seleccionar existente o crear nuevo -->
      <div class="mb-3">
        <div class="d-flex justify-content-between align-items-center mb-2">
          <label for="artistSelect" class="form-label">Artista *</label>
          <button type="button" class="btn btn-sm btn-outline-info" @click="showNewArtist = !showNewArtist">
            {{ showNewArtist ? 'Seleccionar existente' : 'Crear nuevo artista' }}
          </button>
        </div>
        
        <select
          v-if="!showNewArtist"
          id="artistSelect"
          class="form-select"
          v-model="artistIds"
          multiple
          required
          :disabled="artists.length === 0"
        >
          <option v-if="artists.length === 0" disabled value="">No hay artistas disponibles</option>
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
            required
          />
          <button class="btn btn-outline-primary" type="button" @click="findOrCreateArtist">
            <i class="fas fa-plus"></i>
          </button>
        </div>
        <div class="form-text" v-if="!showNewArtist && artists.length === 0">
          No hay artistas en la base de datos. Crea uno nuevo.
        </div>
      </div>

      <!-- Álbum - Seleccionar existente o crear nuevo -->
      <div class="mb-3">
        <div class="d-flex justify-content-between align-items-center mb-2">
          <label for="albumSelect" class="form-label">Álbum *</label>
          <button type="button" class="btn btn-sm btn-outline-info" @click="showNewAlbum = !showNewAlbum">
            {{ showNewAlbum ? 'Seleccionar existente' : 'Crear nuevo álbum' }}
          </button>
        </div>
        
        <select
          v-if="!showNewAlbum"
          id="albumSelect"
          class="form-select"
          v-model="albumId"
          required
          :disabled="albums.length === 0"
        >
          <option v-if="albums.length === 0" disabled value="">No hay álbumes disponibles</option>
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
            required
          />
          <button class="btn btn-outline-primary" type="button" @click="findOrCreateAlbum">
            <i class="fas fa-plus"></i>
          </button>
        </div>
        <div class="form-text" v-if="!showNewAlbum && albums.length === 0">
          No hay álbumes en la base de datos. Crea uno nuevo.
        </div>
      </div>

      <div class="mb-3">
        <label for="fileInput" class="form-label">Archivo de Audio *</label>
        <input
          id="fileInput"
          type="file"
          class="form-control"
          @change="handleFileChange"
          accept="audio/*"
          required
        />
        <div class="form-text">Formatos soportados: MP3, WAV, AAC, etc.</div>
      </div>

      <div class="d-flex justify-content-end gap-2">
        <button type="button" class="btn btn-secondary" @click="resetForm" :disabled="submitting">
          Limpiar
        </button>
        <button type="submit" class="btn btn-primary" :disabled="submitting">
          <span v-if="submitting">
            <i class="fas fa-spinner fa-spin me-2"></i>Subiendo...
          </span>
          <span v-else>
            <i class="fas fa-upload me-2"></i>Subir Canción
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
</template>

<script>
import { mapActions } from 'vuex'
import UploadService from '@/services/UploadService'

export default {
  name: 'AddTrack',
  data() {
    return {
      title: '',
      albumId: null,
      artistIds: [],
      file: null,
      albums: [],
      artists: [],
      loading: false,
      submitting: false,
      errorMessage: null,
      successMessage: null,
      showNewArtist: false,
      showNewAlbum: false,
      newArtistName: '',
      newAlbumName: ''
    }
  },
  methods: {
    ...mapActions('tracks', ['refreshSongs']),
    
    async fetchAlbums() {
      try {
        this.albums = await UploadService.getAlbums()
      } catch (err) {
        this.albums = []
        console.error('fetchAlbums error', err)
      }
    },
    
    async fetchArtists() {
      try {
        this.artists = await UploadService.getArtists()
        // Si no hay artistas, mostrar automáticamente la opción de crear nuevo
        if (this.artists.length === 0) {
          this.showNewArtist = true
        }
      } catch (err) {
        this.artists = []
        this.showNewArtist = true
        console.error('fetchArtists error', err)
      }
    },
    
    handleFileChange(event) {
      const f = event.target.files && event.target.files[0]
      this.file = f || null
    },
    
    async findOrCreateArtist() {
      if (!this.newArtistName.trim()) {
        this.errorMessage = 'Por favor ingresa un nombre para el artista'
        return
      }

      try {
        // Buscar si ya existe un artista con el mismo nombre
        const existingArtist = this.artists.find(
          artist => artist.name.toLowerCase() === this.newArtistName.toLowerCase()
        )

        if (existingArtist) {
          // Si existe, seleccionarlo
          this.artistIds = [existingArtist.id]
          this.newArtistName = ''
          this.showNewArtist = false
          this.successMessage = `Artista "${existingArtist.name}" seleccionado`
        } else {
          // Si no existe, crear nuevo
          const newArtist = await UploadService.createArtist({ name: this.newArtistName })
          this.artists.push(newArtist)
          this.artistIds = [newArtist.id]
          this.newArtistName = ''
          this.showNewArtist = false
          this.successMessage = `Artista "${newArtist.name}" creado exitosamente`
        }
        
        // Limpiar mensaje después de 3 segundos
        setTimeout(() => {
          this.successMessage = null
        }, 3000)
      } catch (err) {
        this.errorMessage = 'Error al crear el artista: ' + (err.response?.data || err.message)
        console.error('findOrCreateArtist error', err)
      }
    },
    
    async findOrCreateAlbum() {
      if (!this.newAlbumName.trim()) {
        this.errorMessage = 'Por favor ingresa un nombre para el álbum'
        return
      }

      if (this.artistIds.length === 0) {
        this.errorMessage = 'Primero debes seleccionar o crear un artista para el álbum'
        return
      }

      try {
        // Buscar si ya existe un álbum con el mismo nombre
        const existingAlbum = this.albums.find(
          album => album.name.toLowerCase() === this.newAlbumName.toLowerCase()
        )

        if (existingAlbum) {
          // Si existe, seleccionarlo
          this.albumId = existingAlbum.id
          this.newAlbumName = ''
          this.showNewAlbum = false
          this.successMessage = `Álbum "${existingAlbum.name}" seleccionado`
        } else {
          // Si no existe, crear nuevo
          const newAlbum = await UploadService.createAlbum({
            name: this.newAlbumName,
            date: new Date().toISOString().split('T')[0],
            author: this.artistIds[0] // Usar el primer artista
          })
          this.albums.push(newAlbum)
          this.albumId = newAlbum.id
          this.newAlbumName = ''
          this.showNewAlbum = false
          this.successMessage = `Álbum "${newAlbum.name}" creado exitosamente`
        }
        
        // Limpiar mensaje después de 3 segundos
        setTimeout(() => {
          this.successMessage = null
        }, 3000)
      } catch (err) {
        this.errorMessage = 'Error al crear el álbum: ' + (err.response?.data || err.message)
        console.error('findOrCreateAlbum error', err)
      }
    },
  
    async convertFileToBase64(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader()
        reader.onload = () => {
          const base64 = reader.result.split(',')[1] || ''
          resolve(base64)
        }
        reader.onerror = (e) => reject(e)
        reader.readAsDataURL(file)
      })
    },
    
    async submitForm() {
      this.errorMessage = null
      this.successMessage = null

      // Validaciones
      if (!this.title || !this.file) {
        this.errorMessage = 'El título y el archivo son obligatorios'
        return
      }

      if (this.showNewArtist && !this.newArtistName) {
        this.errorMessage = 'Debes crear un nuevo artista o seleccionar uno existente'
        return
      }

      if (!this.showNewArtist && this.artistIds.length === 0) {
        this.errorMessage = 'Debes seleccionar al menos un artista'
        return
      }

      if (this.showNewAlbum && !this.newAlbumName) {
        this.errorMessage = 'Debes crear un nuevo álbum o seleccionar uno existente'
        return
      }

      if (!this.showNewAlbum && !this.albumId) {
        this.errorMessage = 'Debes seleccionar un álbum'
        return
      }

      this.submitting = true

      try {
        let finalArtistIds = [...this.artistIds]
        let finalAlbumId = this.albumId

        // Crear nuevo artista si es necesario (con búsqueda primero)
        if (this.showNewArtist && this.newArtistName) {
          const existingArtist = this.artists.find(
            artist => artist.name.toLowerCase() === this.newArtistName.toLowerCase()
          )

          if (existingArtist) {
            finalArtistIds = [existingArtist.id]
          } else {
            const newArtist = await UploadService.createArtist({ name: this.newArtistName })
            finalArtistIds = [newArtist.id]
            this.artists.push(newArtist)
          }
        }

        // Crear nuevo álbum si es necesario (con búsqueda primero)
        if (this.showNewAlbum && this.newAlbumName) {
          if (finalArtistIds.length === 0) {
            throw new Error('Necesitas al menos un artista para crear un álbum')
          }

          const existingAlbum = this.albums.find(
            album => album.name.toLowerCase() === this.newAlbumName.toLowerCase()
          )

          if (existingAlbum) {
            finalAlbumId = existingAlbum.id
          } else {
            const newAlbum = await UploadService.createAlbum({
              name: this.newAlbumName,
              date: new Date().toISOString().split('T')[0],
              author: finalArtistIds[0]
            })
            finalAlbumId = newAlbum.id
            this.albums.push(newAlbum)
          }
        }

        const fileBase64 = await this.convertFileToBase64(this.file)
        const trackData = {
          title: this.title,
          album: finalAlbumId,
          artist: finalArtistIds,
          file_base64: fileBase64
        }

        const created = await UploadService.uploadTrack(this.file, trackData)
        
        this.successMessage = '¡Canción subida exitosamente!'
        this.resetFormFields()
        
        try {
          await this.refreshSongs()
        } catch (err) {
          console.error('refreshSongs failed', err)
        }
        
        this.$emit('created', created)
        
        // Cerrar automáticamente después de 2 segundos
        setTimeout(() => {
          this.$emit('created')
        }, 2000)
        
      } catch (err) {
        this.errorMessage = 'Error al subir la canción: ' + (err.response?.data?.message || err.message)
        console.error('submitForm error', err)
      } finally {
        this.submitting = false
      }
    },
    
    resetForm() {
      this.resetFormFields()
      this.errorMessage = null
      this.successMessage = null
    },
    
    resetFormFields() {
      this.title = ''
      this.albumId = null
      this.artistIds = []
      this.file = null
      this.newArtistName = ''
      this.newAlbumName = ''
      this.showNewArtist = this.artists.length === 0
      this.showNewAlbum = this.albums.length === 0
      const input = this.$el.querySelector('#fileInput')
      if (input) input.value = ''
    }
  },
  mounted() {
    this.fetchAlbums()
    this.fetchArtists()
    // Inicializar mostrando creación si no hay datos
    this.showNewArtist = this.artists.length === 0
    this.showNewAlbum = this.albums.length === 0
  }
}
</script>