<template>
  <div class="add-album-form card p-3 bg-dark text-white">
    <h5 class="mb-3">Add Album</h5>

    <form @submit.prevent="submitForm" novalidate>
      <div class="mb-3">
        <label for="albumName" class="form-label">Album Name</label>
        <input
          id="albumName"
          type="text"
          class="form-control"
          v-model="name"
          required
        />
      </div>

      <div class="mb-3">
        <label for="authorSelect" class="form-label">Author</label>
        <select
          id="authorSelect"
          class="form-select"
          v-model="authorId"
          required
        >
          <option :value="null">Select author</option>
          <option v-for="a in authors" :key="a.id" :value="a.id">
            {{ a.name }}
          </option>
        </select>
      </div>

      <div class="d-flex justify-content-end gap-2">
        <button type="button" class="btn btn-secondary" @click="resetForm" :disabled="submitting">Reset</button>
        <button type="submit" class="btn btn-primary" :disabled="submitting">
          <span v-if="submitting">Saving...</span>
          <span v-else>Submit</span>
        </button>
      </div>

      <div class="mt-2" v-if="errorMessage">
        <small class="text-danger">{{ errorMessage }}</small>
      </div>

      <div class="mt-2" v-if="successMessage">
        <small class="text-success">{{ successMessage }}</small>
      </div>
    </form>
  </div>
</template>

<script>
import { API_BASE_URL } from '@/utils/constants'
import UploadService from '@/services/UploadService'

export default {
  name: 'AddAlbum',
  data() {
    return {
      name: '',
      authorId: null,
      authors: [],
      submitting: false,
      errorMessage: null,
      successMessage: null
    }
  },
  methods: {
    async fetchAuthors() {
      try {
        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/artists/`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        this.authors = Array.isArray(data) ? data : (data.results || [])
      } catch (err) {
        this.authors = []
        this.errorMessage = 'No se pudieron cargar los autores'
        console.error('fetchAuthors error', err)
      }
    },
    async submitForm() {
      this.errorMessage = null
      this.successMessage = null

      if (!this.name || !this.authorId) {
        this.errorMessage = 'Completa todos los campos requeridos.'
        return
      }

      this.submitting = true

      try {
        const payload = {
          name: this.name,
          date: new Date().toISOString().split('T')[0],
          author: this.authorId
        }

        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/albums/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        })

        if (!res.ok) {
          const errText = await res.text()
          throw new Error(errText || `HTTP ${res.status}`)
        }

        // const created = await res.json()
        const created = await UploadService.createAlbum(payload)
        this.successMessage = 'Album agregado correctamente'
        this.resetFormFields()
        this.$emit('created', created)
      } catch (err) {
        this.errorMessage = 'Error al agregar el álbum'
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
      this.name = ''
      this.authorId = null
    }
  },
  mounted() {
    this.fetchAuthors()
  }
}
</script>
