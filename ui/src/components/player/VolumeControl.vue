<!-- VolumenControl.vue -->
<template>
  <div class="volume-control-container">
    <i class="fas fa-volume-up volume-icon"></i>
    <input
      type="range"
      class="volume-slider"
      min="0"
      max="100"
      :value="volumeLevel"
      @input="updateVolumeLevel"
      aria-label="Control de volumen"
    >
  </div>
</template>

<script>
import { mapState, mapActions } from 'vuex'

export default {
  name: 'VolumeControl',
  computed: {
    ...mapState('player', ['volumeLevel'])
  },
  methods: {
    ...mapActions('player', ['updateVolumeLevel'])
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>