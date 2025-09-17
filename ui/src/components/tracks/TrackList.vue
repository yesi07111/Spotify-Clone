<template>
  <div class="tracks-list-container">
    <SearchBar />
    <div class="tracks-list">
      <TrackItem 
        v-for="track in filteredTracks" 
        :key="track.id" 
        :track="track"
        @track-selected="handleTrackSelection"
      />
    </div>
  </div>
</template>

<script>
import { mapGetters, mapActions } from 'vuex'
import SearchBar from './SearchBar.vue'
import TrackItem from './TrackItem.vue'

export default {
  name: 'TrackList',
  components: {
    SearchBar,
    TrackItem
  },
  computed: {
    ...mapGetters('tracks', ['filteredTracks'])
  },
  methods: {
    ...mapActions('player', ['selectTrack']),
    handleTrackSelection(track) {
      this.selectTrack(track)
    }
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>