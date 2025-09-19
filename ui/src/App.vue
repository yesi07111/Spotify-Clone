<template>
  <div id="app">
    <NavigationBar />
    <router-view></router-view>
  </div>
</template>

<script>
import NavigationBar from '@/components/layout/NavigationBar.vue'

export default {
  name: 'App',
  components: {
    NavigationBar
  }
}
</script>

<style>
@import '@/assets/styles/main.css';
</style>