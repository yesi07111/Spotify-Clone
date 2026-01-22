// src/main.js
import { createApp } from 'vue'
import App from './App.vue'
import router from './routing'
import store from './store'

import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap'
import '@fortawesome/fontawesome-free/css/all.min.css'
import '@/assets/styles/main.css'
import '@/assets/styles/components.css'
import '@/assets/styles/variables.css'
import AuthService from '@/services/AuthService'
import { UIState } from '@/store/ui'

// Limpiar tokens huérfanos al iniciar
const token = AuthService.getToken()
if (token && !AuthService.isAuthenticated()) {
  console.log('🧹 Tokens inválidos detectados, limpiando storage...')
  localStorage.removeItem('spotify_auth_token')
  localStorage.removeItem('spotify_user_data')
  UIState.isAuthenticated = false
  UIState.currentUser = null
}

const app = createApp(App)

app.use(router)
app.use(store)
app.mount('#app')