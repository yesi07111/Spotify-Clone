<template>
  <div class="tracks-list-container">
    <div class="search-filter-container">
      <div class="row">
        <div class="col-md-8">
          <div class="input-group">
            <input
              type="text"
              class="form-control"
              placeholder="Buscar canciones..."
              @input="getSearchText"
              @keyup.enter="search"
            />
            <button
              class="btn btn-outline-secondary"
              type="button"
              @click="search"
            >
              <i class="fas fa-search"></i>
            </button>
          </div>
        </div>
        <div class="col-md-4 text-end">
          <button class="btn btn-outline-light" @click="openFilterModal">
            <i class="fas fa-filter me-2"></i>Filtrar
          </button>
        </div>
      </div>
    </div>

    <SearchBar />

    <div class="tracks-list">
      <TrackItem 
        v-for="track in filteredTracks" 
        :key="track.id" 
        :track="track"
        @track-selected="handleTrackSelection"
      />
    </div>

    <TrackFilterModal ref="filterModal" />
  </div>
</template>

<script>
import { mapGetters, mapActions } from 'vuex'
import TrackItem from './TrackItem.vue'
import TrackFilterModal from '@/components/filters/TrackFilter.vue'

export default {
  name: 'TrackList',
  components: {
    TrackItem,
    TrackFilterModal
  },
  data() {
    return {
      searchText: ''
    }
  },
  computed: {
    ...mapGetters('tracks', ['filteredTracks'])
  },
  methods: {
    ...mapActions('player', ['selectTrack']),
    ...mapActions('tracks', ['fetchTracks', 'filter']),
    handleTrackSelection(track) {
      this.selectTrack(track)
    },
    search() {
      this.filter({ album: null, artist: null, name: this.searchText })
    },
    getSearchText(event) {
      this.searchText = event.target.value
    },
    openFilterModal() {
      if (this.$refs.filterModal && typeof this.$refs.filterModal.openModal === 'function') {
        this.$refs.filterModal.openModal()
      }
    }
  },
  mounted() {
    if (this.fetchTracks) this.fetchTracks()
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>
