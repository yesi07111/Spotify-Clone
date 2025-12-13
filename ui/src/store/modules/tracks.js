//store/modules/tracks.js
import { API_BASE_URL } from '@/utils/constants'

const state = {
  tracks: [],
  allTracks: [],
  searchQuery: '',
  activeFilters: {
    artists: [],
    album: null
  },
  isFiltersVisible: false
}

const mutations = {
  SET_SEARCH_QUERY(state, query) {
    state.searchQuery = query
  },
  SET_ACTIVE_FILTERS(state, filters) {
    state.activeFilters = { ...state.activeFilters, ...filters }
  },
  TOGGLE_FILTERS_VISIBILITY(state) {
    state.isFiltersVisible = !state.isFiltersVisible
  },
  SET_TRACKS(state, tracks) {
    state.tracks = tracks
  },
  SET_ALL_TRACKS(state, tracks) {
    state.allTracks = tracks
  },
  CLEAR_FILTERS(state) {
    state.activeFilters = { artists: [], album: null }
    state.searchQuery = ''
  }
}

const actions = {
  updateSearchQuery({ commit, dispatch }, query) {
    const searchText = typeof query === 'string' ? query :
      (query && query.target ? query.target.value : '')
    commit('SET_SEARCH_QUERY', searchText)
    dispatch('applyFilters')
  },

  toggleFilters({ commit }) {
    commit('TOGGLE_FILTERS_VISIBILITY')
  },

  async fetchSongs({ commit, dispatch }) {
    try {
      const response = await fetch(`${API_BASE_URL}/tracks/`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const songs = await response.json()

      commit('SET_ALL_TRACKS', songs)
      commit('SET_TRACKS', songs)

      dispatch('player/updatePlaylistFromTracks', null, { root: true })

      return songs
    } catch (err) {
      console.error('fetchSongs error', err)
      return []
    }
  },

  setTracks({ commit }, tracks) {
    commit('SET_TRACKS', tracks)
  },

  async refreshSongs({ dispatch }) {
    return await dispatch('fetchSongs')
  },

  async setFilters({ commit, dispatch }, { artists = [], album = null }) {
    commit('SET_ACTIVE_FILTERS', { artists, album })
    await dispatch('applyFilters')
  },

  async applyFilters({ state, commit, dispatch }) {
    try {
      // Construir query params para backend
      const params = new URLSearchParams()

      // Filtros de modal (backend)
      if (state.activeFilters.artists && state.activeFilters.artists.length > 0) {
        state.activeFilters.artists.forEach(artistId => {
          params.append('artist[]', artistId)
        })
      }

      if (state.activeFilters.album) {
        params.append('album', state.activeFilters.album)
      }

      // Filtro de búsqueda (backend también)
      if (state.searchQuery && state.searchQuery.trim()) {
        params.append('title', state.searchQuery.trim())
      }

      // Hacer request al backend con filtros
      const url = `${API_BASE_URL}/tracks/${params.toString() ? '?' + params.toString() : ''}`
      console.log('🔍 Fetching filtered tracks:', url)

      const response = await fetch(url)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const filtered = await response.json()

      commit('SET_TRACKS', filtered)
      dispatch('player/updatePlaylistFromTracks', null, { root: true })

    } catch (error) {
      console.error('Error applying filters:', error)
      // Fallback a todos los tracks si hay error
      commit('SET_TRACKS', state.allTracks)
    }
  },

  clearAllFilters({ commit, dispatch }) {
    commit('CLEAR_FILTERS')
    dispatch('applyFilters')
  }
}

const getters = {
  filteredTracks: state => state.tracks,
  hasActiveFilters: state => {
    return (state.activeFilters.artists && state.activeFilters.artists.length > 0) ||
      state.activeFilters.album !== null ||
      (state.searchQuery && state.searchQuery.trim().length > 0)
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}