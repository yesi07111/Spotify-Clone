<!-- components/forms/AddArtist.vue -->
<template>
  <div class="add-artist-form card p-3 bg-dark text-white">
    <!-- Tabs -->
    <ul class="nav nav-tabs mb-3">
      <li class="nav-item">
        <button
          class="nav-link"
          :class="{ active: activeTab === 'create' }"
          @click="activeTab = 'create'"
        >
          Crear
        </button>
      </li>
      <li class="nav-item">
        <button
          class="nav-link"
          :class="{ active: activeTab === 'delete' }"
          @click="activeTab = 'delete'"
        >
          Borrar
        </button>
      </li>
    </ul>

    <!-- ================= CREAR ================= -->
    <div v-if="activeTab === 'create'">
      <h5 class="mb-3">Crear Artista</h5>

      <form @submit.prevent="submitCreate" novalidate>
        <div class="mb-3">
          <label class="form-label">Nombre del Artista *</label>
          <input
            type="text"
            class="form-control"
            v-model="name"
            placeholder="Ingresa el nombre del artista"
            required
          />
        </div>

        <div class="d-flex justify-content-end gap-2">
          <button type="button" class="btn btn-secondary" @click="resetCreate">
            Limpiar
          </button>
          <button type="submit" class="btn btn-primary" :disabled="submitting">
            Crear Artista
          </button>
        </div>
      </form>
    </div>

    <!-- ================= BORRAR ================= -->
    <div v-else>
      <h5 class="mb-3">Borrar Artista</h5>

      <div class="mb-3">
        <label class="form-label">Selecciona un artista *</label>
        <select class="form-select" v-model="artistToDelete">
          <option :value="null">Selecciona...</option>
          <option v-for="artist in artists" :key="artist.id" :value="artist.id">
            {{ artist.name }}
          </option>
        </select>
      </div>

      <div class="d-flex justify-content-end">
        <button
          class="btn btn-danger"
          :disabled="!artistToDelete || submitting"
          @click="deleteArtist"
        >
          Borrar Artista
        </button>
      </div>
    </div>

    <!-- Mensajes -->
    <div class="mt-3" v-if="errorMessage">
      <div class="alert alert-danger">{{ errorMessage }}</div>
    </div>

    <div class="mt-3" v-if="successMessage">
      <div class="alert alert-success">{{ successMessage }}</div>
    </div>
  </div>
</template>

<script>
import UploadService from '@/services/UploadService'
import { mapActions } from 'vuex'

export default {
  name: 'AddArtist',
  data() {
    return {
      activeTab: 'create',

      name: '',
      artists: [],
      artistToDelete: null,

      submitting: false,
      errorMessage: null,
      successMessage: null
    }
  },
  methods: {
    ...mapActions('tracks', ['refreshSongs']),

    async fetchArtists() {
      this.artists = await UploadService.getArtists()
    },

    async submitCreate() {
      this.errorMessage = null
      this.successMessage = null

      if (!this.name.trim()) {
        this.errorMessage = 'El nombre del artista es obligatorio'
        return
      }

      this.submitting = true
      try {
        await UploadService.createArtist({ name: this.name })
        this.successMessage = 'Artista creado exitosamente'
        this.name = ''
        this.fetchArtists()
      } catch (err) {
        this.errorMessage = 'Error al crear artista'
      } finally {
        this.submitting = false
      }
    },

    async deleteArtist() {
      this.errorMessage = null
      this.successMessage = null

      this.submitting = true
      try {
        await UploadService.deleteArtist(this.artistToDelete)
        this.successMessage = 'Artista eliminado correctamente'
        this.artistToDelete = null
        this.fetchArtists()

        try {
          await this.refreshSongs()
        } catch (err) {
          console.error('refreshSongs failed', err)
        }

      } catch (err) {
        this.errorMessage = 'Error al borrar artista'
      } finally {
        this.submitting = false
      }
    },

    resetCreate() {
      this.name = ''
      this.errorMessage = null
      this.successMessage = null
    }
  },
  mounted() {
    this.fetchArtists()
  }
}
</script>
