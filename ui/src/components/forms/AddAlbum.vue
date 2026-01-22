<!-- components/forms/AddAlbum.vue -->
<template>
  <div class="card bg-dark text-white p-3">
    <!-- Tabs -->
    <ul class="nav nav-tabs mb-3">
      <li class="nav-item">
        <button
          class="nav-link"
          :class="{ active: activeTab === 'create' }"
          @click="activeTab = 'create'"
        >
          Crear Álbum
        </button>
      </li>
      <li class="nav-item">
        <button
          class="nav-link text-danger"
          :class="{ active: activeTab === 'delete' }"
          @click="activeTab = 'delete'"
        >
          Eliminar Álbum
        </button>
      </li>
    </ul>

    <!-- ================= CREAR ================= -->
    <div v-if="activeTab === 'create'">
      <h5 class="mb-3">Crear Álbum</h5>

      <form @submit.prevent="submitForm" novalidate>
        <div class="mb-3">
          <label class="form-label">Nombre del Álbum *</label>
          <input
            type="text"
            class="form-control"
            v-model="name"
            placeholder="Ingresa el nombre del álbum"
            required
          />
        </div>

        <!-- Autor -->
        <div class="mb-3">
          <div class="d-flex justify-content-between align-items-center mb-2">
            <label class="form-label">Autor (Artista)</label>
            <button
              type="button"
              class="btn btn-sm btn-outline-info"
              @click="toggleNewAuthor"
            >
              {{ showNewAuthor ? 'Seleccionar existente' : 'Crear nuevo autor' }}
            </button>
          </div>

          <select
            v-if="!showNewAuthor"
            class="form-select"
            v-model="authorId"
          >
            <option :value="null">Sin autor</option>
            <option
              v-for="author in authors"
              :key="author.id"
              :value="author.id"
            >
              {{ author.name }}
            </option>
          </select>

          <div v-else class="input-group">
            <input
              type="text"
              class="form-control"
              v-model="newAuthorName"
              placeholder="Nombre del nuevo autor"
            />
            <button
              class="btn btn-outline-primary"
              type="button"
              @click="findOrCreateAuthor"
            >
              <i class="fas fa-plus"></i>
            </button>
          </div>
        </div>

        <div class="mb-3">
          <label class="form-label">Fecha de lanzamiento</label>
          <input
            type="date"
            class="form-control"
            v-model="date"
          />
        </div>

        <div class="d-flex justify-content-end gap-2">
          <button
            type="button"
            class="btn btn-secondary"
            @click="resetForm"
            :disabled="submitting"
          >
            Limpiar
          </button>
          <button
            type="submit"
            class="btn btn-primary"
            :disabled="submitting"
          >
            <span v-if="submitting">
              <i class="fas fa-spinner fa-spin me-2"></i>Creando...
            </span>
            <span v-else>
              <i class="fas fa-plus me-2"></i>Crear Álbum
            </span>
          </button>
        </div>
      </form>
    </div>

    <!-- ================= ELIMINAR ================= -->
    <div v-if="activeTab === 'delete'">
      <h5 class="mb-3 text-danger">Eliminar Álbum</h5>

      <div class="mb-3">
        <label class="form-label">Selecciona un álbum</label>
        <select
          class="form-select"
          v-model="albumToDelete"
        >
          <option :value="null">-- Selecciona un álbum --</option>
          <option
            v-for="album in albums"
            :key="album.id"
            :value="album.id"
          >
            {{ album.name }}
          </option>
        </select>
      </div>

      <div class="alert alert-warning">
        Esta acción <strong>no eliminará los tracks</strong>.
        El álbum quedará desvinculado.
      </div>

      <button
        class="btn btn-danger"
        :disabled="!albumToDelete || deleting"
        @click="deleteAlbum"
      >
        <span v-if="deleting">
          <i class="fas fa-spinner fa-spin me-2"></i>Eliminando...
        </span>
        <span v-else>
          <i class="fas fa-trash me-2"></i>Eliminar Álbum
        </span>
      </button>
    </div>

    <!-- Mensajes -->
    <div class="mt-3" v-if="errorMessage">
      <div class="alert alert-danger">
        {{ errorMessage }}
      </div>
    </div>

    <div class="mt-3" v-if="successMessage">
      <div class="alert alert-success">
        {{ successMessage }}
      </div>
    </div>
  </div>
</template>

<script>
import UploadService from '@/services/UploadService'
import { mapActions } from 'vuex'

export default {
  name: 'AddAlbum',

  data() {
    return {
      activeTab: 'create',

      name: '',
      authorId: null,
      date: new Date().toISOString().split('T')[0],

      authors: [],
      albums: [],

      showNewAuthor: false,
      newAuthorName: '',

      albumToDelete: null,

      submitting: false,
      deleting: false,
      errorMessage: null,
      successMessage: null
    }
  },

  methods: {
    ...mapActions('tracks', ['refreshSongs']),
    

    async fetchAuthors() {
      try {
        this.authors = await UploadService.getArtists()
        if (this.authors.length === 0) {
          this.showNewAuthor = true
        }
      } catch {
        this.showNewAuthor = true
      }
    },

    async fetchAlbums() {
      try {
        this.albums = await UploadService.getAlbums()
      } catch (err) {
        console.error('fetchAlbums error', err)
      }
    },

    toggleNewAuthor() {
      this.showNewAuthor = !this.showNewAuthor
      this.newAuthorName = ''
    },

    async findOrCreateAuthor() {
      if (!this.newAuthorName.trim()) return

      const existing = this.authors.find(
        a => a.name.toLowerCase() === this.newAuthorName.toLowerCase()
      )

      if (existing) {
        this.authorId = existing.id
      } else {
        const created = await UploadService.createArtist({ name: this.newAuthorName })
        this.authors.push(created)
        this.authorId = created.id
      }

      this.showNewAuthor = false
      this.newAuthorName = ''
    },

    async submitForm() {
      if (!this.name) {
        this.errorMessage = 'El nombre del álbum es obligatorio'
        return
      }

      if (this.newAuthorName) {
        this.findOrCreateAuthor()
      }

      this.submitting = true
      this.errorMessage = null

      try {
        await UploadService.createAlbum({
          name: this.name,
          date: this.date,
          author: this.authorId
        })

        this.successMessage = '¡Álbum creado exitosamente!'
        this.resetFormFields()
        this.fetchAlbums()

      } catch (err) {
        this.errorMessage =
          err.response?.data?.error ||
          err.response?.data?.message ||
          err.message
      } finally {
        this.submitting = false
      }
    },

    async deleteAlbum() {
      if (!this.albumToDelete) return

      this.deleting = true
      this.errorMessage = null

      try {
        await UploadService.deleteAlbum(this.albumToDelete)
        this.successMessage = 'Álbum eliminado correctamente'
        this.albumToDelete = null
        this.fetchAlbums()

        try {
          await this.refreshSongs()
        } catch (err) {
          console.error('refreshSongs failed', err)
        }

      } catch (err) {
        this.errorMessage =
          err.response?.data?.error ||
          err.response?.data?.message ||
          err.message
      } finally {
        this.deleting = false
      }
    },

    resetForm() {
      this.resetFormFields()
      this.errorMessage = null
      this.successMessage = null
    },

    resetFormFields() {
      this.name = ''
      this.authorId = null
      this.date = new Date().toISOString().split('T')[0]
      this.newAuthorName = ''
      this.showNewAuthor = this.authors.length === 0
    }
  },

  mounted() {
    this.fetchAuthors()
    this.fetchAlbums()
  }
}
</script>
