<template>
  <div class="search-container">
    <div class="input-group">
      <span class="input-group-text">
        <i class="fas fa-search"></i>
      </span>
      <input
        type="text"
        class="form-control search-input"
        placeholder="Buscar canciones, artistas o álbumes..."
        :value="searchQuery"
        @input="updateSearchQuery"
        aria-label="Buscar en la biblioteca musical"
      >
      <button 
        class="btn btn-outline-secondary" 
        type="button"
        @click="toggleFilters"
        aria-label="Mostrar filtros"
      >
        <i class="fas fa-filter me-2"></i>Filtrar
      </button>
    </div>
  </div>
</template>

<script>
import { mapState, mapActions } from 'vuex'

export default {
  name: 'SearchBar',
  computed: {
    ...mapState('tracks', ['searchQuery'])
  },
  methods: {
    ...mapActions('tracks', ['updateSearchQuery', 'toggleFilters'])
  }
}
</script>

<style scoped src="@/assets/styles/components.css"></style>