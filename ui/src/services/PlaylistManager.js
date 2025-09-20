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
    this.apiUrl = `${base}/songs/`
    this.songs = []
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
        this.songs = Array.isArray(data) ? data : (data.results || [])
        if (!this.songs) this.songs = []
        if (this.currentIndex >= this.songs.length) this.currentIndex = 0
        return this.songs
      })
      .catch(err => {
        console.error('PlaylistManager loadSongs error', err)
        return this.songs
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
    const idx = this.songs.findIndex(s => s && (s.id === songId || String(s.id) === String(songId)))
    this.currentIndex = idx >= 0 ? idx : Math.max(0, Math.min(this.currentIndex, this.songs.length - 1))
  }

  getCurrentSong() {
    if (!this.songs || this.songs.length === 0) return null
    if (this.currentIndex < 0) this.currentIndex = 0
    if (this.currentIndex >= this.songs.length) this.currentIndex = this.songs.length - 1
    return this.songs[this.currentIndex] || null
  }

  next() {
    if (!this.songs || this.songs.length === 0) return
    this.currentIndex = (this.currentIndex + 1) % this.songs.length
  }

  prev() {
    if (!this.songs || this.songs.length === 0) return
    this.currentIndex = (this.currentIndex - 1 + this.songs.length) % this.songs.length
  }

  shuffle() {
    for (let i = this.songs.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1))
      const tmp = this.songs[i]
      this.songs[i] = this.songs[j]
      this.songs[j] = tmp
    }
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
      this.songs.push(created)
      return created
    } catch (err) {
      console.error('PlaylistManager addSong error', err)
      return null
    }
  }

  getSongs() {
    return this.songs
  }

  setSongs(list = []) {
    this.songs = Array.isArray(list) ? list : []
    if (this.currentIndex >= this.songs.length) this.currentIndex = 0
  }

  clearFilters() {
    this.filters = { artist: null, album: null, name: null }
  }
}
