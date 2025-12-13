// services/ApiService.js
import axios from 'axios'

const API_BASE_URL = process.env.VUE_APP_API_URL || 'http://127.0.0.1:8000/api'

// Función para hacer reintentos
const retryRequest = async (requestFn, maxRetries = 5, delay = 500) => {
  let lastError;
  
  // Primero 5 intentos inmediatos
  for (let i = 0; i < 5; i++) {
    try {
      return await requestFn();
    } catch (error) {
      lastError = error;
      if (i === 4) break; // Si fallan los 5 inmediatos, pasamos a los que tienen delay
    }
  }
  
  // Luego 5 intentos con delay
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await requestFn();
    } catch (error) {
      lastError = error;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError;
}

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Wrappers para todas las peticiones con reintentos
const apiClientWithRetry = {
  get: (url, config = {}) => retryRequest(() => apiClient.get(url, config)),
  post: (url, data, config = {}) => retryRequest(() => apiClient.post(url, data, config)),
  patch: (url, data, config = {}) => retryRequest(() => apiClient.patch(url, data, config)),
  delete: (url, config = {}) => retryRequest(() => apiClient.delete(url, config)),
}

export default {
  // Método genérico
  async executeApiRequest(url, params = {}) {
    return apiClientWithRetry.get(url, { params });
  },

  // Tracks
  getTracks(params = {}) {
    return apiClientWithRetry.get('/tracks/', { params })
  },
  
  getTrack(id) {
    return apiClientWithRetry.get(`/tracks/${id}/`)
  },
  
  createTrack(trackData) {
    return apiClientWithRetry.post('/tracks/', trackData)
  },
  
  updateTrack(id, trackData) {
    return apiClientWithRetry.patch(`/tracks/${id}/`, trackData)
  },
  
  deleteTrack(id) {
    return apiClientWithRetry.delete(`/tracks/${id}/`)
  },
  
  // Artists
  getArtists(params = {}) {
    return apiClientWithRetry.get('/artists/', { params })
  },
  
  createArtist(artistData) {
    return apiClientWithRetry.post('/artists/', artistData)
  },
  
  // Albums
  getAlbums(params = {}) {
    return apiClientWithRetry.get('/albums/', { params })
  },
  
  createAlbum(albumData) {
    return apiClientWithRetry.post('/albums/', albumData)
  },
  
  // Streaming
  streamAudio(params) {
    return apiClientWithRetry.get('/streamer/', { params })
  }
}