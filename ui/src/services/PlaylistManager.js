import ApiService from '@/services/ApiService'

export default class PlaylistManager {
  constructor() {
    this.tracks = []
    this.currentIndex = 0
    this.filters = { artist: null, album: null, name: null }
    this._loading = false
    this._lastLoadPromise = null
    this.loadSongs()
  }

  async loadSongs(params = {}) {
    if (this._loading && this._lastLoadPromise) return this._lastLoadPromise
    
    this._loading = true
    const merged = { ...this.filters, ...params }
    
    const p = ApiService.getTracks(merged)
      .then(response => {
        // Axios devuelve la respuesta en .data
        const data = response.data
        this.tracks = Array.isArray(data) ? data : (data.results || [])
        if (!this.tracks) this.tracks = []
        if (this.currentIndex >= this.tracks.length) this.currentIndex = 0
        return this.tracks
      })
      .catch(err => {
        console.error('PlaylistManager loadSongs error', err)
        return this.tracks
      })
      .finally(() => {
        this._loading = false
      })
    
    this._lastLoadPromise = p
    return p
  }

  async refresh() {
    return await this.loadSongs()
  }

  async filter({ artist = null, album = null, name = null } = {}) {
    if (artist !== null) this.filters.artist = artist
    if (album !== null) this.filters.album = album
    if (name !== null) this.filters.name = name
    return await this.refresh()
  }

  setCurrentSong(songId) {
    const idx = this.tracks.findIndex(s => s && (s.id === songId || String(s.id) === String(songId)))
    this.currentIndex = idx >= 0 ? idx : Math.max(0, Math.min(this.currentIndex, this.tracks.length - 1))
  }

  getCurrentSong() {
    if (!this.tracks || this.tracks.length === 0) return null
    if (this.currentIndex < 0) this.currentIndex = 0
    if (this.currentIndex >= this.tracks.length) this.currentIndex = this.tracks.length - 1
    return this.tracks[this.currentIndex] || null
  }

  next() {
    if (!this.tracks || this.tracks.length === 0) return
    this.currentIndex = (this.currentIndex + 1) % this.tracks.length
  }

  prev() {
    if (!this.tracks || this.tracks.length === 0) return
    this.currentIndex = (this.currentIndex - 1 + this.tracks.length) % this.tracks.length
  }

  shuffleKeepingCurrent(currentSongId = null) {
    if (!this.tracks || this.tracks.length <= 1) return
    
    const tracks = [...this.tracks]
    let currentTrack = null
    let otherTracks = tracks
    
    if (currentSongId) {
      const currentIndex = tracks.findIndex(t => t.id === currentSongId)
      if (currentIndex >= 0) {
        currentTrack = tracks[currentIndex]
        otherTracks = tracks.filter((_, i) => i !== currentIndex)
      }
    }
    
    for (let i = otherTracks.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1))
      const tmp = otherTracks[i]
      otherTracks[i] = otherTracks[j]
      otherTracks[j] = tmp
    }
    
    this.tracks = currentTrack ? [currentTrack, ...otherTracks] : otherTracks
    this.currentIndex = 0
  }

  async addSong(songData = {}) {
    try {
      const response = await ApiService.createTrack(songData)
      const created = response.data
      this.tracks.push(created)
      return created
    } catch (err) {
      console.error('PlaylistManager addSong error', err)
      return null
    }
  }

  async updateSong(id, songData) {
    try {
      const response = await ApiService.updateTrack(id, songData)
      const updated = response.data
      
      // Actualizar en la lista local
      const index = this.tracks.findIndex(track => track.id === id)
      if (index !== -1) {
        this.tracks[index] = { ...this.tracks[index], ...updated }
      }
      
      return updated
    } catch (err) {
      console.error('PlaylistManager updateSong error', err)
      return null
    }
  }

  async deleteSong(id) {
    try {
      await ApiService.deleteTrack(id)
      
      // Eliminar de la lista local
      this.tracks = this.tracks.filter(track => track.id !== id)
      if (this.currentIndex >= this.tracks.length) {
        this.currentIndex = Math.max(0, this.tracks.length - 1)
      }
      
      return true
    } catch (err) {
      console.error('PlaylistManager deleteSong error', err)
      return false
    }
  }

  getSongs() {
    return this.tracks
  }

  setSongs(list = []) {
    this.tracks = Array.isArray(list) ? list : []
    if (this.currentIndex >= this.tracks.length) this.currentIndex = 0
  }

  clearFilters() {
    this.filters = { artist: null, album: null, name: null }
  }
}