import { API_BASE_URL } from '@/utils/constants'

function buildUrl(base, params = {}) {
  const url = new URL(base)
  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return
    url.searchParams.append(k, v)
  })
  return url.toString()
}

export default class PlaylistManager {
  constructor(apiBase = API_BASE_URL || 'http://localhost:8000/api') {
    const base = typeof apiBase === 'string' ? apiBase.replace(/\/+$/, '') : 'http://localhost:8000/api'
    this.apiUrl = `${base}/tracks/`
    this.tracks = []
    this.currentIndex = 0
    this.filters = { artist: null, album: null, name: null }
    this._loading = false
    this._lastLoadPromise = null
    this.loadSongs()
  }

  async _fetchJson(url, options = {}) {
    const res = await fetch(url, options)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return await res.json()
  }

  async loadSongs(params = {}) {
    if (this._loading && this._lastLoadPromise) return this._lastLoadPromise
    this._loading = true
    const merged = { ...this.filters, ...params }
    const url = buildUrl(this.apiUrl, merged)
    const p = this._fetchJson(url)
      .then(data => {
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
    
    // Si hay una canción actual, separarla
    if (currentSongId) {
      const currentIndex = tracks.findIndex(t => t.id === currentSongId)
      if (currentIndex >= 0) {
        currentTrack = tracks[currentIndex]
        otherTracks = tracks.filter((_, i) => i !== currentIndex)
      }
    }
    
    // Mezclar el resto usando Fisher-Yates
    for (let i = otherTracks.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1))
      const tmp = otherTracks[i]
      otherTracks[i] = otherTracks[j]
      otherTracks[j] = tmp
    }
    
    // Reconstruir con canción actual al inicio
    this.tracks = currentTrack ? [currentTrack, ...otherTracks] : otherTracks
    this.currentIndex = 0
  }

  async addSong(songData = {}) {
    try {
      const res = await fetch(this.apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(songData)
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const created = await res.json()
      this.tracks.push(created)
      return created
    } catch (err) {
      console.error('PlaylistManager addSong error', err)
      return null
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