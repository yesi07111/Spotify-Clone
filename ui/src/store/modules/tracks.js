// store/modules/tracks.js
import ApiService from '@/services/ApiService'

const state = {
  tracks: [],
  allTracks: [],
  searchQuery: '',
  activeFilters: {
    artists: [],
    album: null
  },
  isFiltersVisible: false,
  isLoading: false
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
  },
  SET_LOADING(state, isLoading) {
    state.isLoading = isLoading
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
      commit('SET_LOADING', true)
      console.log('[TRACKS STORE] Fetching all songs...')
      
      const response = await ApiService.getTracks()
      const songs = response.data
      
      console.log(`[TRACKS STORE] Fetched ${songs?.length || 0} songs`)
      
      commit('SET_ALL_TRACKS', songs || [])
      commit('SET_TRACKS', songs || [])
      
      // Actualizar playlist del player
      dispatch('player/updatePlaylistFromTracks', null, { root: true })
      
      return songs || []
    } catch (err) {
      console.error('[TRACKS STORE] fetchSongs error:', err)
      console.error('Error details:', err.response?.data)
      return []
    } finally {
      commit('SET_LOADING', false)
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
      commit('SET_LOADING', true)
      
      // Construir params para backend
      const params = {}
      
      // Filtros de artistas
      if (state.activeFilters.artists && state.activeFilters.artists.length > 0) {
        // Si el backend espera array: artist__in=id1,id2,id3
        params.artist__in = state.activeFilters.artists.join(',')
      }
      
      // Filtro de álbum
      if (state.activeFilters.album) {
        params.album = state.activeFilters.album
      }
      
      // Filtro de búsqueda
      if (state.searchQuery && state.searchQuery.trim()) {
        // Depende de tu backend - podría ser 'search', 'q', 'title__icontains'
        params.search = state.searchQuery.trim()
        // O si tu backend Django usa icontains:
        // params.title__icontains = state.searchQuery.trim()
      }
      
      console.log('[TRACKS STORE] Applying filters with params:', params)
      
      const response = await ApiService.getTracks(params)
      const filtered = response.data || []
      
      console.log(`[TRACKS STORE] Got ${filtered.length} filtered tracks`)
      
      commit('SET_TRACKS', filtered)
      
      // Actualizar playlist del player
      dispatch('player/updatePlaylistFromTracks', null, { root: true })
      
    } catch (error) {
      console.error('[TRACKS STORE] Error applying filters:', error)
      console.error('Error details:', error.response?.data)
      
      // Fallback a todos los tracks si hay error
      commit('SET_TRACKS', state.allTracks)
    } finally {
      commit('SET_LOADING', false)
    }
  },

  clearAllFilters({ commit, dispatch }) {
    commit('CLEAR_FILTERS')
    dispatch('applyFilters')
  },
  
  // Acciones adicionales para CRUD de tracks
  async createTrack({ dispatch }, trackData) {
    try {
      console.log('[TRACKS STORE] Creating track:', trackData)
      const response = await ApiService.createTrack(trackData)
      const newTrack = response.data
      
      // Refrescar lista
      await dispatch('fetchSongs')
      
      return newTrack
    } catch (error) {
      console.error('[TRACKS STORE] Error creating track:', error)
      throw error
    }
  },
  
  async updateTrack({ dispatch }, { id, trackData }) {
    try {
      console.log(`[TRACKS STORE] Updating track ${id}:`, trackData)
      const response = await ApiService.updateTrack(id, trackData)
      const updatedTrack = response.data
      
      // Refrescar lista
      await dispatch('fetchSongs')
      
      return updatedTrack
    } catch (error) {
      console.error(`[TRACKS STORE] Error updating track ${id}:`, error)
      throw error
    }
  },
  
  async deleteTrack({ dispatch }, id) {
    try {
      console.log(`[TRACKS STORE] Deleting track ${id}`)
      await ApiService.deleteTrack(id)
      
      // Refrescar lista
      await dispatch('fetchSongs')
      
      return true
    } catch (error) {
      console.error(`[TRACKS STORE] Error deleting track ${id}:`, error)
      throw error
    }
  }
}

const getters = {
  filteredTracks: state => state.tracks,
  hasActiveFilters: state => {
    return (state.activeFilters.artists && state.activeFilters.artists.length > 0) ||
      state.activeFilters.album !== null ||
      (state.searchQuery && state.searchQuery.trim().length > 0)
  },
  isLoading: state => state.isLoading,
  getTrackById: state => (id) => {
    return state.allTracks.find(track => track.id === id)
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}