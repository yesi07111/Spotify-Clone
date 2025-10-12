<template>
  <div class="add-artist-form card p-3 bg-dark text-white">
    <h5 class="mb-3">Add Artist</h5>

    <form @submit.prevent="submitForm" novalidate>
      <div class="mb-3">
        <label for="artistName" class="form-label">Artist Name</label>
        <input
          id="artistName"
          type="text"
          class="form-control"
          v-model="name"
          required
        />
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
  name: 'AddArtist',
  data() {
    return {
      name: '',
      submitting: false,
      errorMessage: null,
      successMessage: null
    }
  },
  methods: {
    async submitForm() {
      this.errorMessage = null
      this.successMessage = null

      if (!this.name) {
        this.errorMessage = 'Artist name is required.'
        return
      }

      this.submitting = true

      try {
        const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/artists/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: this.name })
        })

        if (!res.ok) {
          const errText = await res.text()
          throw new Error(errText || `HTTP ${res.status}`)
        }

        // const created = await res.json()
        const created = await UploadService.createArtist({ name: this.name })
        this.successMessage = 'Artist added successfully'
        this.resetFormFields()
        this.$emit('created', created)
      } catch (err) {
        this.errorMessage = 'Error adding artist'
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
    }
  }
}
</script>
