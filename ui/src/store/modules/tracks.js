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
  },
  CLEAR_ALL_TRACKS(state) {
    state.tracks = []
    state.allTracks = []
    state.searchQuery = ''
    state.activeFilters = { artists: [], album: null }
  },
}

const actions = {
  updateSearchQuery({ commit, dispatch }, query) {
    const searchText = typeof query === 'string' ? query :
      (query && query.target ? query.target.value : '')
    commit('SET_SEARCH_QUERY', searchText)
    dispatch('applyFilters')
  },
  clearTracksOnLogout({ commit }) {
    commit('CLEAR_ALL_TRACKS')
    // También limpiar el player
    this.dispatch('player/stopTrack', null, { root: true })
  },

  toggleFilters({ commit }) {
    commit('TOGGLE_FILTERS_VISIBILITY')
  },

  async fetchSongs({ commit, dispatch }) {
  try {
    commit('SET_LOADING', true)
    console.log('[TRACKS STORE] Fetching songs from API...')
    
    // ApiService ya tiene reintentos integrados
    const response = await ApiService.getTracks()
    const songs = response.data || []
    
    console.log(`[TRACKS STORE] Fetched ${songs.length} songs successfully`)
    
    commit('SET_ALL_TRACKS', songs)
    commit('SET_TRACKS', songs)
    
    // Actualizar playlist del player
    dispatch('player/updatePlaylistFromTracks', null, { root: true })
    
    return songs
  } catch (err) {
    console.error('[TRACKS STORE] Error fetching songs:', err)
    commit('SET_ALL_TRACKS', [])
    commit('SET_TRACKS', [])
    throw err // Lanzar error para que el componente lo maneje
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

      const hasFilters =
        (state.activeFilters.artists && state.activeFilters.artists.length > 0) ||
        state.activeFilters.album !== null

      const hasSearch =
        state.searchQuery && state.searchQuery.trim().length > 0

      console.log('[TRACKS STORE] Aplicando filtros:', {
        hasFilters,
        hasSearch,
        artists: state.activeFilters.artists,
        album: state.activeFilters.album,
        searchQuery: state.searchQuery
      })

      /* ==========================================================
      * SIN FILTROS NI BÚSQUEDA
      * ========================================================== */
      if (!hasFilters && !hasSearch) {
        console.log('[TRACKS STORE] Sin filtros, mostrando todos')
        commit('SET_TRACKS', state.allTracks)
        dispatch('player/updatePlaylistFromTracks', null, { root: true })
        return
      }

      /* ==========================================================
      * SOLO FILTROS (BACKEND)
      * ========================================================== */
      if (hasFilters && !hasSearch) {
        const params = {}

        if (state.activeFilters.artists?.length > 0) {
        state.activeFilters.artists.forEach((id) => {
          params['artist[]'] = params['artist[]'] || []
          params['artist[]'].push(id)
        })
      }


        // No mandar "Álbum desconocido" al backend
        if (
          state.activeFilters.album &&
          state.activeFilters.album.toLowerCase() !== 'álbum desconocido'
        ) {
          params.album = state.activeFilters.album
        }

        console.log('[TRACKS STORE] Llamando API con params:', params)

        const response = await ApiService.getTracks(params)
        let filtered = response.data || []

        // Filtro frontend para "Álbum desconocido"
        if (
          state.activeFilters.album &&
          state.activeFilters.album.toLowerCase() === 'álbum desconocido'
        ) {
          filtered = filtered.filter(track => !track.album_name)
        }

        console.log(`[TRACKS STORE] Backend devolvió ${filtered.length} tracks`)
        commit('SET_TRACKS', filtered)
      }

      /* ==========================================================
      * SOLO BÚSQUEDA (LOCAL)
      * ========================================================== */
      else if (!hasFilters && hasSearch) {
        console.log('[TRACKS STORE] Aplicando búsqueda local')

        const query = state.searchQuery.toLowerCase().trim()

        const filtered = state.allTracks.filter(track => {
          const title = track.title?.toLowerCase() || ''
          const album = normalizeAlbum(track)
          const artists = normalizeArtists(track).toLowerCase()

          return (
            title.includes(query) ||
            album.includes(query) ||
            artists.includes(query)
          )
        })

        console.log(`[TRACKS STORE] Búsqueda local encontró ${filtered.length} tracks`)
        commit('SET_TRACKS', filtered)
      }

      /* ==========================================================
      * FILTROS BACKEND + BÚSQUEDA LOCAL
      * ========================================================== */
      else {
        console.log('[TRACKS STORE] Aplicando filtros backend + búsqueda local')

        const params = {}

        if (state.activeFilters.artists?.length > 0) {
          params.artist__in = state.activeFilters.artists.join(',')
        }

        if (
          state.activeFilters.album &&
          state.activeFilters.album.toLowerCase() !== 'álbum desconocido'
        ) {
          params.album = state.activeFilters.album
        }

        const response = await ApiService.getTracks(params)
        let backendFiltered = response.data || []

        // Aplicar filtro frontend para "Álbum desconocido"
        if (
          state.activeFilters.album &&
          state.activeFilters.album.toLowerCase() === 'álbum desconocido'
        ) {
          backendFiltered = backendFiltered.filter(track => !track.album_name)
        }

        const query = state.searchQuery.toLowerCase().trim()

        const finalFiltered = backendFiltered.filter(track => {
          const title = track.title?.toLowerCase() || ''
          const album = normalizeAlbum(track)
          const artists = normalizeArtists(track).toLowerCase()

          return (
            title.includes(query) ||
            album.includes(query) ||
            artists.includes(query)
          )
        })

        console.log(
          `[TRACKS STORE] Backend: ${backendFiltered.length}, después de búsqueda local: ${finalFiltered.length}`
        )

        commit('SET_TRACKS', finalFiltered)
      }

      dispatch('player/updatePlaylistFromTracks', null, { root: true })
    } catch (error) {
      console.error('[TRACKS STORE] Error aplicando filtros:', error)
      console.error('Error details:', error.response?.data)

      if (state.searchQuery && state.searchQuery.trim()) {
        const query = state.searchQuery.toLowerCase().trim()

        const filtered = state.allTracks.filter(track => {
          const title = track.title?.toLowerCase() || ''
          const album = normalizeAlbum(track)
          const artists = normalizeArtists(track).toLowerCase()

          return (
            title.includes(query) ||
            album.includes(query) ||
            artists.includes(query)
          )
        })

        commit('SET_TRACKS', filtered)
      } else {
        commit('SET_TRACKS', state.allTracks)
      }
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

//Helpers
const UNKNOWN_ALBUM = 'álbum desconocido'
const UNKNOWN_ARTIST = 'artista desconocido'

function normalizeAlbum(track) {
  return (track.album_name || UNKNOWN_ALBUM).toLowerCase()
}

function normalizeArtists(track) {
  if (!track.artist_names || track.artist_names.length === 0) {
    return UNKNOWN_ARTIST
  }

  // Si es array
  if (Array.isArray(track.artist_names)) {
    return track.artist_names.join(', ')
  }

  return track.artist_names
}


export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}