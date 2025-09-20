<template>
  <div class="add-track-form card p-3 bg-dark text-white">
    <h5 class="mb-3">Add Track</h5>

    <form @submit.prevent="submitForm" novalidate>
      <div class="mb-3">
        <label for="trackTitle" class="form-label">Track Title</label>
        <input
          id="trackTitle"
          type="text"
          class="form-control"
          v-model="title"
          required
        />
      </div>

      <div class="mb-3">
        <label for="albumSelect" class="form-label">Album</label>
        <select
          id="albumSelect"
          class="form-select"
          v-model="albumId"
          required
        >
          <option :value="null">Select album</option>
          <option v-for="alb in albums" :key="alb.id" :value="alb.id">
            {{ alb.name }}
          </option>
        </select>
      </div>

      <div class="mb-3">
        <label for="artistsSelect" class="form-label">Artists</label>
        <select
          id="artistsSelect"
          class="form-select"
          v-model="artistIds"
          multiple
          required
        >
          <option v-for="art in artists" :key="art.id" :value="art.id">
            {{ art.name }}
          </option>
        </select>
      </div>

      <div class="mb-3">
        <label for="fileInput" class="form-label">Music File</label>
        <input
          id="fileInput"
          type="file"
          class="form-control"
          @change="handleFileChange"
          accept="audio/*"
          required
        />
      </div>

      <div class="d-flex justify-content-end gap-2">
        <button type="button" class="btn btn-secondary" @click="resetForm" :disabled="submitting">Reset</button>
        <button type="submit" class="btn btn-primary" :disabled="submitting">
          <span v-if="submitting">Uploading...</span>
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
import { mapActions } from 'vuex'
import { API_BASE_URL } from '@/utils/constants'

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
      successMessage: null
    }
  },
  methods: {
    ...mapActions('tracks', ['refreshSongs']),
    async fetchAlbums() {
      try {
        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/albums/`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        this.albums = Array.isArray(data) ? data : (data.results || [])
      } catch (err) {
        this.albums = []
        console.error('fetchAlbums error', err)
      }
    },
    async fetchArtists() {
      try {
        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/artists/`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        this.artists = Array.isArray(data) ? data : (data.results || [])
      } catch (err) {
        this.artists = []
        console.error('fetchArtists error', err)
      }
    },
    handleFileChange(event) {
      const f = event.target.files && event.target.files[0]
      this.file = f || null
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

      if (!this.title || !this.albumId || !this.artistIds.length || !this.file) {
        this.errorMessage = 'Please complete all required fields.'
        return
      }

      this.submitting = true

      try {
        const fileBase64 = await this.convertFileToBase64(this.file)
        const payload = {
          title: this.title,
          album: this.albumId,
          artist: this.artistIds,
          file_base64: fileBase64
        }

        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/songs/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        })

        if (!res.ok) {
          const errText = await res.text()
          throw new Error(errText || `HTTP ${res.status}`)
        }

        const created = await res.json()
        this.successMessage = 'Track added successfully'
        this.resetFormFields()
        try {
          await this.refreshSongs()
        } catch (err) {
          console.error('refreshSongs failed', err)
        }
        this.$emit('created', created)
      } catch (err) {
        this.errorMessage = 'Error adding track'
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
      const input = this.$el.querySelector('#fileInput')
      if (input) input.value = ''
    }
  },
  mounted() {
    this.fetchAlbums()
    this.fetchArtists()
  }
}
</script>
