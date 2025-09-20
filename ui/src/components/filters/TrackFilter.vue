<template>
  <div
    v-if="visible"
    class="modal fade show"
    style="display: block"
    role="dialog"
    aria-modal="true"
    aria-labelledby="filterModalTitle"
    @click.self="closeModal"
  >
    <div class="modal-dialog" ref="modalContainer" tabindex="-1" @keydown.esc="closeModal">
      <div class="modal-content bg-dark text-white">
        <div class="modal-header">
          <h5 class="modal-title" id="filterModalTitle">Filter by Artist and Album</h5>
          <button
            type="button"
            class="btn-close btn-close-white"
            @click="closeModal"
            aria-label="Close"
          ></button>
        </div>

        <div class="modal-body">
          <form @submit.prevent="submitForm">
            <div class="mb-3">
              <label for="artistSelect" class="form-label">Select Artists</label>
              <select
                id="artistSelect"
                v-model="selectedArtists"
                class="form-control"
                multiple
                :disabled="loadingArtists"
                aria-describedby="artistHelp"
              >
                <option
                  v-for="artist in artists"
                  :key="artist.id"
                  :value="artist.id"
                >
                  {{ artist.name }}
                </option>
              </select>
              <div id="artistHelp" class="form-text" v-if="loadingArtists">Loading artists...</div>
              <div class="text-danger small" v-if="errorArtists">{{ errorArtists }}</div>
            </div>

            <div class="mb-3">
              <label for="albumSelect" class="form-label">Select Album</label>
              <select
                id="albumSelect"
                v-model="selectedAlbum"
                class="form-control"
                :disabled="loadingAlbums"
              >
                <option :value="null">All albums</option>
                <option
                  v-for="album in albums"
                  :key="album.id"
                  :value="album.id"
                >
                  {{ album.name }}
                </option>
              </select>
              <div class="form-text" v-if="loadingAlbums">Loading albums...</div>
              <div class="text-danger small" v-if="errorAlbums">{{ errorAlbums }}</div>
            </div>

            <div class="modal-footer">
              <button type="submit" class="btn btn-primary" :disabled="isSubmitting">
                Apply Filters
              </button>
              <button type="button" class="btn btn-secondary" @click="closeModal">Cancel</button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { mapActions } from 'vuex'

const API_BASE = process.env.VUE_APP_API_BASE_URL || 'http://localhost:8000/api'

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
    ...mapActions('tracks', ['filter']),
    async openModal() {
      this.visible = true
      this.selectedArtists = []
      this.selectedAlbum = null
      // parallel load
      await Promise.all([this.loadArtists(), this.loadAlbums()])
      this.$nextTick(() => {
        if (this.$refs.modalContainer) this.$refs.modalContainer.focus()
      })
    },
    closeModal() {
      this.visible = false
    },
    async loadArtists() {
      this.loadingArtists = true
      this.errorArtists = null
      try {
        const res = await fetch(`${API_BASE}/artists/`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        this.artists = Array.isArray(data) ? data : []
      } catch (err) {
        this.errorArtists = 'Error fetching artists'
        console.error('TrackFilterModal: fetch artists error', err)
      } finally {
        this.loadingArtists = false
      }
    },
    async loadAlbums() {
      this.loadingAlbums = true
      this.errorAlbums = null
      try {
        const res = await fetch(`${API_BASE}/albums/`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        this.albums = Array.isArray(data) ? data : []
      } catch (err) {
        this.errorAlbums = 'Error fetching albums'
        console.error('TrackFilterModal: fetch albums error', err)
      } finally {
        this.loadingAlbums = false
      }
    },
    async submitForm() {
      this.isSubmitting = true
      try {
        await this.filter({
          album: this.selectedAlbum,
          artist: Array.from(this.selectedArtists),
          name: null
        })
      } catch (err) {
        console.error('TrackFilterModal: apply filter error', err)
      } finally {
        this.isSubmitting = false
        this.closeModal()
      }
    }
  }
}
</script>

<style scoped>
.modal-content {
  border-radius: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
}
</style>
