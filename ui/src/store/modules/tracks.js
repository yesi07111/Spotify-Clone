const state = {
    tracks: [
        {
            id: '1',
            title: 'Bohemian Rhapsody',
            artist: 'Queen',
            album: 'A Night at the Opera',
            duration: '5:55',
            // Nota: imageName se genera automáticamente desde el título, es random solo para bonito
        },
        {
            id: '2',
            title: 'Hotel California',
            artist: 'Eagles',
            album: 'Hotel California',
            duration: '6:30',
        },
        {
            id: '3',
            title: 'Sweet Child O\' Mine',
            artist: 'Guns N\' Roses',
            album: 'Appetite for Destruction',
            duration: '5:03',
        },
        {
            id: '4',
            title: 'Imagine',
            artist: 'John Lennon',
            album: 'Imagine',
            duration: '3:04',
        },
        {
            id: '5',
            title: 'Billie Jean',
            artist: 'Michael Jackson',
            album: 'Thriller',
            duration: '4:54',
        },
        {
            id: '6',
            title: 'Yesterday',
            artist: 'The Beatles',
            album: 'Help!',
            duration: '2:05',
        },
        {
            id: '7',
            title: 'Like a Rolling Stone',
            artist: 'Bob Dylan',
            album: 'Highway 61 Revisited',
            duration: '6:13',
        },
        {
            id: '8',
            title: 'Smells Like Teen Spirit',
            artist: 'Nirvana',
            album: 'Nevermind',
            duration: '5:01',
        }
    ],
    searchQuery: '',
    filters: {
        genre: '',
        decade: '',
        rating: 0
    },
    isFiltersVisible: false
};

const mutations = {
  SET_SEARCH_QUERY(state, query) {
    state.searchQuery = query
  },
  SET_FILTERS(state, filters) {
    state.filters = { ...state.filters, ...filters }
  },
  TOGGLE_FILTERS_VISIBILITY(state) {
    state.isFiltersVisible = !state.isFiltersVisible
  }
}

const actions = {
  updateSearchQuery({ commit }, event) {
    commit('SET_SEARCH_QUERY', event.target.value)
  },
  updateFilters({ commit }, filters) {
    commit('SET_FILTERS', filters)
  },
  toggleFilters({ commit }) {
    commit('TOGGLE_FILTERS_VISIBILITY')
  }
}

const getters = {
  filteredTracks: (state) => {
    let filtered = state.tracks
    
    if (state.searchQuery) {
      const query = state.searchQuery.toLowerCase()
      filtered = filtered.filter(track => 
        track.title.toLowerCase().includes(query) ||
        track.artist.toLowerCase().includes(query) ||
        track.album.toLowerCase().includes(query)
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