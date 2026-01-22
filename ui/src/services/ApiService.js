// services/ApiService.js
import axios from 'axios'
import AuthService from './AuthService';

// Configuración de reintentos
const IMMEDIATE_RETRIES = 5
const DELAYED_RETRIES = 5
const RETRY_DELAY_MS = 500 // Delay entre reintentos (configurable)

const API_BASE_URL = process.env.VUE_APP_API_URL || 'http://127.0.0.1:8000/api'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true
})

// Función de reintentos mejorada
async function retryRequest(requestFn, maxImmediateRetries = IMMEDIATE_RETRIES, maxDelayedRetries = DELAYED_RETRIES, delay = RETRY_DELAY_MS) {
  let lastError
  
  // Fase 1: Reintentos inmediatos
  for (let attempt = 1; attempt <= maxImmediateRetries; attempt++) {
    try {
      console.log(`[RETRY] Intento inmediato ${attempt}/${maxImmediateRetries}`)
      return await requestFn()
    } catch (error) {
      lastError = error
      
      // Si es error de autenticación, no reintentar
      if (error.response?.status === 401 || error.response?.status === 403) {
        console.log('[RETRY] Error de autenticación, no se reintenta')
        throw error
      }
      
      console.log(`[RETRY] Intento inmediato ${attempt} falló:`, error.message)
    }
  }
  
  // Fase 2: Reintentos con delay
  for (let attempt = 1; attempt <= maxDelayedRetries; attempt++) {
    try {
      console.log(`[RETRY] Esperando ${delay}ms antes del intento ${attempt}/${maxDelayedRetries}`)
      await new Promise(resolve => setTimeout(resolve, delay))
      
      console.log(`[RETRY] Intento con delay ${attempt}/${maxDelayedRetries}`)
      return await requestFn()
    } catch (error) {
      lastError = error
      
      // Si es error de autenticación, no reintentar
      if (error.response?.status === 401 || error.response?.status === 403) {
        console.log('[RETRY] Error de autenticación en fase con delay, no se reintenta')
        throw error
      }
      
      console.log(`[RETRY] Intento con delay ${attempt} falló:`, error.message)
    }
  }
  
  console.error('[RETRY] Todos los reintentos fallaron')
  throw lastError
}



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
        const newAccess = await AuthService.refreshToken()
        originalRequest.headers.Authorization = `Bearer ${newAccess}`
        return apiClient(originalRequest)

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
    return retryRequest(() => apiClient.get(url, { params }))
  },

  // Tracks
  getTracks(params = {}) {
    return retryRequest(() => apiClient.get('/tracks/', { params }))
  },
  
  getTrack(id) {
    return retryRequest(() => apiClient.get(`/tracks/${id}/`))
  },
  
  createTrack(trackData) {
    return retryRequest(() => apiClient.post('/tracks/', trackData))
  },
  
  updateTrack(id, trackData) {
    return retryRequest(() => apiClient.patch(`/tracks/${id}/`, trackData))
  },
  
  deleteTrack(id) {
    return retryRequest(() => apiClient.delete(`/tracks/${id}/`))
  },

  // Artists
  getArtists(params = {}) {
    return retryRequest(() => apiClient.get('/artists/', { params }))
  },
  
  createArtist(artistData) {
    return retryRequest(() => apiClient.post('/artists/', artistData))
  },

  deleteArtist(id) {
    return retryRequest(() =>
      apiClient.delete(`/artists/${id}/`)
    )
  },

  // Albums
  getAlbums(params = {}) {
    return retryRequest(() => apiClient.get('/albums/', { params }))
  },
  
  createAlbum(albumData) {
    return retryRequest(() => apiClient.post('/albums/', albumData))
  },

   deleteAlbum(id) {
    return retryRequest(() =>
      apiClient.delete(`/albums/${id}/`)
    )
  },

  // Streaming
  streamAudio(params) {
    return retryRequest(() => apiClient.get('/streamer/', { params }))
  }
}