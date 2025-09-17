<template>
  <nav class="navigation-bar">
    <div class="navigation-brand">
      <i class="fab fa-spotify navigation-brand-icon"></i>
      <span>Spotify Clone</span>
    </div>
    <div class="navigation-actions">
      <button class="btn btn-outline-secondary me-2">Iniciar Sesión</button>
      <button class="btn btn-primary">Cerrar Sesión</button>
    </div>
  </nav>
</template>

<script>
export default {
  name: 'NavigationBar'
}
</script>

<style scoped src="@/assets/styles/components.css"></style>