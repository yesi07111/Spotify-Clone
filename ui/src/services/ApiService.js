// services/ApiService.js
import axios from 'axios'
import AuthService from './AuthService';

const API_BASE_URL = process.env.VUE_APP_API_URL || 'http://127.0.0.1:8000/api'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true
})

// ✅ INTERCEPTOR para AGREGAR TOKEN (esto SÍ funciona)
apiClient.interceptors.request.use(
  (config) => {
    console.log('[API DEBUG] Interceptor ejecutado para URL:', config.url)
    
    const token = AuthService.getToken()
    console.log('[API DEBUG] Token obtenido:', token ? 'SÍ' : 'NO')
    
    if (token) {
      let accessToken = token
      
      if (typeof token === 'object' && token.access) {
        accessToken = token.access
      }
      
      console.log('[API DEBUG] Agregando Authorization header con token:', accessToken.substring(0, 20) + '...')
      config.headers.Authorization = `Bearer ${accessToken}`
      console.log('[API DEBUG] Headers después:', config.headers)
    }
    
    return config
  },
  (error) => {
    console.error('[API DEBUG] Error en request interceptor:', error)
    return Promise.reject(error)
  }
)

// Interceptor para refrescar token
apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    console.log('[API DEBUG] Response error:', error.response?.status)
    
    const originalRequest = error.config
    
    if (error.response?.status === 401 && !originalRequest._retry) {
      console.log('[API DEBUG] Token expirado, intentando refresh...')
      originalRequest._retry = true
      
      try {
        // Refrescar token
        const refreshed = await AuthService.refreshToken()
        if (refreshed) {
          // Reintentar
          return apiClient(originalRequest)
        }
      } catch (refreshError) {
        console.error('[API DEBUG] Refresh failed:', refreshError)
        AuthService.logout()
      }
    }
    
    return Promise.reject(error)
  }
)

export default {
  // Método genérico
  async executeApiRequest(url, params = {}) {
    return apiClient.get(url, { params });
  },

  // Tracks
  getTracks(params = {}) {
    return apiClient.get('/tracks/', { params })
  },
  
  getTrack(id) {
    return apiClient.get(`/tracks/${id}/`)
  },
  
  createTrack(trackData) {
    return apiClient.post('/tracks/', trackData)
  },
  
  updateTrack(id, trackData) {
    return apiClient.patch(`/tracks/${id}/`, trackData)
  },
  
  deleteTrack(id) {
    return apiClient.delete(`/tracks/${id}/`)
  },
  
  // Artists
  getArtists(params = {}) {
    return apiClient.get('/artists/', { params })
  },
  
  createArtist(artistData) {
    return apiClient.post('/artists/', artistData)
  },
  
  // Albums
  getAlbums(params = {}) {
    return apiClient.get('/albums/', { params })
  },
  
  createAlbum(albumData) {
    return apiClient.post('/albums/', albumData)
  },
  
  // Streaming
  streamAudio(params) {
    return apiClient.get('/streamer/', { params })
  }
}