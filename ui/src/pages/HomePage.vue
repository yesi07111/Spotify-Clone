<template>
  <div class="main-container">
    <TrackList />
    <CurrentTrack />
  </div>
</template>

<script>
import TrackList from '@/components/tracks/TrackList.vue'
import CurrentTrack from '@/components/player/CurrentTrack.vue'

export default {
  name: 'HomePage',
  components: {
    TrackList,
    CurrentTrack
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>