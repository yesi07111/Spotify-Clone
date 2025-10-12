import axios from 'axios'

const API_BASE_URL = process.env.VUE_APP_API_URL || 'http://127.0.0.1:8000/api'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Interceptor para manejar errores
apiClient.interceptors.response.use(
  response => response,
  error => {
    console.error('API Error:', error.response?.data || error.message)
    return Promise.reject(error)
  }
)

export default {
   async executeApiRequest(url, params = {}) {
    try {
      const queryParams = new URLSearchParams()
      
      Object.entries(params).forEach(([key, value]) => {
        queryParams.append(key, typeof value === 'boolean' ? value.toString() : value)
      })

      const apiUrl = `${url}?${queryParams.toString()}`
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      })

      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`)
      }

      return await response.json()
    } catch (error) {
      console.error('API request failed:', error)
      throw error
    }
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