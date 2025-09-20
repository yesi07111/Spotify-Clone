import PlaylistManager from '@/services/PlaylistManager' 

const state = {
  tracks: [], 
  searchQuery: '',
  filters: {
    genre: '',
    decade: '',
    rating: 0
  },
  isFiltersVisible: false
}

const mutations = {
  SET_SEARCH_QUERY(state, query) {
    state.searchQuery = query
  },
  SET_FILTERS(state, filters) {
    state.filters = { ...state.filters, ...filters }
  },
  TOGGLE_FILTERS_VISIBILITY(state) {
    state.isFiltersVisible = !state.isFiltersVisible
  },
  SET_TRACKS(state, tracks) {
    state.tracks = tracks
  }
}

const actions = {
  updateSearchQuery({ commit }, event) {
    commit('SET_SEARCH_QUERY', event && event.target ? event.target.value : event || '')
  },
  updateFilters({ commit }, filters) {
    commit('SET_FILTERS', filters)
  },
  toggleFilters({ commit }) {
    commit('TOGGLE_FILTERS_VISIBILITY')
  },

  async fetchSongs({ commit, state }) {
    try {
      let pm = null
      try {
        pm = new PlaylistManager()
      } catch (err) {
        pm = null
      }
      let songs = []
      if (pm && typeof pm.loadSongs === 'function') {
        songs = await pm.loadSongs()
      } else if (pm && typeof pm.load === 'function') {
        songs = await pm.load()
      } else {
        songs = state.tracks 
      }
      commit('SET_TRACKS', songs)
      return songs
    } catch (err) {
      console.error('fetchSongs error', err)
      return []
    }
  },

  async refreshSongs({ commit }) {
    try {
      const pm = new PlaylistManager()
      let songs = []
      if (typeof pm.refresh === 'function') {
        songs = await pm.refresh()
      } else if (typeof pm.reload === 'function') {
        songs = await pm.reload()
      }
      commit('SET_TRACKS', songs)
      return songs
    } catch (err) {
      console.error('refreshSongs error', err)
      return []
    }
  },

  async filter({ commit, state }, { artist = null, album = null, name = null }) {
    commit('SET_FILTERS', { artist, album })
    try {
      const pm = new PlaylistManager()
      if (typeof pm.filter === 'function') {
        const filtered = await pm.filter({ artist, album, name })
        commit('SET_TRACKS', filtered)
        return filtered
      } else {
        // fallback local filter
        const filtered = (state.tracks || []).filter(s => {
          let ok = true
          if (artist) ok = ok && (s.artist_names ? s.artist_names.includes(artist) : s.artist === artist)
          if (album) ok = ok && (s.album_name ? s.album_name === album : s.album === album)
          if (name) ok = ok && (s.title ? s.title.toLowerCase().includes(name.toLowerCase()) : false)
          return ok
        })
        commit('SET_TRACKS', filtered)
        return filtered
      }
    } catch (err) {
      console.error('tracks.filter error', err)
      return []
    }
  }
}

const getters = {
  filteredTracks: (state) => {
    let filtered = state.tracks || []

    if (state.searchQuery) {
      const query = state.searchQuery.toLowerCase()
      filtered = filtered.filter(track =>
        (track.title && track.title.toLowerCase().includes(query)) ||
        (track.artist && track.artist.toLowerCase().includes(query)) ||
        (track.album && track.album.toLowerCase().includes(query))
      )
    }

    return filtered
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}
