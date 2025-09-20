import AudioPlayerService from '@/services/AudioPlayerService' 
import PlaylistManager from '@/services/PlaylistManager'
import { formatTime } from '@/utils/audioHelper'

const STREAMER_URL = process.env.VUE_APP_STREAMER_URL || 'http://127.0.0.1:8000/api/streamer'

const state = {
  audioPlayer: null,
  playlistManager: null,
  currentSong: {
    name: '-----',
    artist: '-----',
    album: '-----'
  },
  isPlaying: false,
  formattedRemainingTime: '00:00',
  formattedTotalDuration: '00:00',
  progressBarProgress: 0,
  volume: 50,
  songs: [],
  repeat: false,
  filters: {
    artist: null,
    album: null,
  }
}

const mutations = {
  SET_AUDIO_PLAYER(state, player) {
    state.audioPlayer = player
  },
  SET_PLAYLIST_MANAGER(state, manager) {
    state.playlistManager = manager
  },
  SET_PLAYING(state, isPlaying) {
    state.isPlaying = isPlaying
  },
  SET_TIMES(state, { remaining, total, progress }) {
    state.formattedRemainingTime = remaining
    state.formattedTotalDuration = total
    state.progressBarProgress = progress
  },
  SET_VOLUME(state, volume) {
    state.volume = volume
  },
  SET_CURRENT_SONG(state, song) {
    state.currentSong = {
      name: song.title || song.name || '-----',
      album: song.album_name || song.album || '-----',
      artist: Array.isArray(song.artist_names) ? song.artist_names.join(', ') : (song.artist || '-----')
    }
  },
  SET_SONGS(state, songs) {
    state.songs = songs
  },
  SET_REPEAT(state, repeat) {
    state.repeat = repeat
  },
  SET_FILTERS(state, { album, artist }) {
    state.filters = {
      album: album,
      artist: artist,
    }
  }
}

const actions = {
  async initializeServices({ state, commit }) {
    try {
      if (!state.audioPlayer) {
        let audioInst = null
        try {
          audioInst = new AudioPlayerService(STREAMER_URL)
        } catch (err) {
            console.warn('No AudioPlayer constructor found, audio features will be limited: ', err)
        }
        commit('SET_AUDIO_PLAYER', audioInst)
      }

      if (!state.playlistManager) {
        try {
          const pm = new PlaylistManager()
          commit('SET_PLAYLIST_MANAGER', pm)
        } catch (err) {
          console.warn('PlaylistManager not available', err)
          commit('SET_PLAYLIST_MANAGER', null)
        }
      }

      if (state.audioPlayer && typeof state.audioPlayer.onPlaybackProgressUpdate === 'function') {
        state.audioPlayer.onPlaybackProgressUpdate((progressData) => {
          const elapsed = progressData.currentTime ?? null
          const duration = progressData.duration ?? (state.audioPlayer.duration || state.currentSong.duration || 0)
          if (elapsed != null && duration) {
            const remainingTime = duration - elapsed
            const formattedRemaining = formatTime(remainingTime)
            const formattedTotal = formatTime(duration)
            const progressPercent = Math.floor((elapsed / duration) * 100)
            commit('SET_TIMES', { remaining: formattedRemaining, total: formattedTotal, progress: progressPercent })
          }
        })
      }

      return { audioPlayer: state.audioPlayer, playlistManager: state.playlistManager }
    } catch (err) {
      console.error('initializeServices error', err)
      throw err
    }
  },

  async playAudio({ state, commit, dispatch }, songId) {
    if (!state.audioPlayer || !state.playlistManager) {
      await dispatch('initializeServices')
    }

    try {
      if (state.audioPlayer) {
        if (typeof state.audioPlayer.start === 'function') {
          await state.audioPlayer.start(songId, state.volume)
        } else if (typeof state.audioPlayer.initializeAudioPlayer === 'function') {
          await state.audioPlayer.initializeAudioPlayer(songId)
        } else if (typeof state.audioPlayer.playTrack === 'function') {
          await state.audioPlayer.playTrack(songId, state.volume)
        } else {
          console.warn('audioPlayer does not expose start/initializeAudioPlayer/playTrack')
        }
      }

      // set playlist current
      if (state.playlistManager && typeof state.playlistManager.setCurrentSong === 'function') {
        state.playlistManager.setCurrentSong(songId)
      } else if (state.playlistManager && typeof state.playlistManager.setCurrent === 'function') {
        state.playlistManager.setCurrent(songId)
      }

      commit('SET_PLAYING', true)

      let current = null
      if (state.playlistManager && typeof state.playlistManager.getCurrentSong === 'function') {
        current = state.playlistManager.getCurrentSong()
      } else if (state.playlistManager && state.playlistManager.songs) {
        current = state.playlistManager.songs.find(s => s.id === songId)
      }

      if (current) {
        commit('SET_CURRENT_SONG', current)
      }

      setTimeUpdater(state, commit)

      if (state.audioPlayer) {
        state.audioPlayer.onPlaybackEnd = async () => {
          try {
            if (!state.repeat && state.playlistManager && typeof state.playlistManager.next === 'function') {
              state.playlistManager.next()
            }
            const nextSong = state.playlistManager && typeof state.playlistManager.getCurrentSong === 'function'
              ? state.playlistManager.getCurrentSong()
              : null
            if (nextSong && nextSong.id) {
              // start next
              if (typeof state.audioPlayer.start === 'function') {
                await state.audioPlayer.start(nextSong.id, state.volume)
              } else if (typeof state.audioPlayer.initializeAudioPlayer === 'function') {
                await state.audioPlayer.initializeAudioPlayer(nextSong.id)
              }
              commit('SET_PLAYING', true)
              if (nextSong) commit('SET_CURRENT_SONG', nextSong)
              setTimeUpdater(state, commit)
            } else {
              commit('SET_PLAYING', false)
            }
          } catch (err) {
            console.error('onPlaybackEnd handler error', err)
          }
        }
      }
    } catch (err) {
      console.error('playAudio error', err)
      throw err
    }
  },

  setVolume({ state, commit }, event) {
    const volume = (event && event.target && event.target.value) !== undefined ? event.target.value : event
    commit('SET_VOLUME', volume)
    if (state.audioPlayer && typeof state.audioPlayer.setVolume === 'function') {
      state.audioPlayer.setVolume(volume / 100)
    } else if (state.audioPlayer && state.audioPlayer.sound && typeof state.audioPlayer.sound.volume === 'function') {
      state.audioPlayer.sound.volume(volume / 100)
    }
  },

  playAndPause({ state, commit }) {
    try {
      if (!state.audioPlayer) return
      if (typeof state.audioPlayer.playAndPause === 'function') {
        state.audioPlayer.playAndPause()
      } else if (state.audioPlayer.sound && typeof state.audioPlayer.sound.play === 'function') {
        if (state.audioPlayer.sound.playing && state.audioPlayer.sound.playing()) {
          state.audioPlayer.sound.pause()
        } else {
          state.audioPlayer.sound.play()
        }
      } else if (typeof state.audioPlayer.toggle === 'function') {
        state.audioPlayer.toggle()
      }

      const playing = (state.audioPlayer.sound && state.audioPlayer.sound.playing && state.audioPlayer.sound.playing()) || !!state.isPlaying
      commit('SET_PLAYING', playing)
      setTimeUpdater(state, commit)
    } catch (err) {
      console.error('playAndPause error', err)
    }
  },

  moveToTime({ state, commit }, event) {
    try {
      const value = event && event.target ? event.target.value : event
      const timePercent = value / 100
      const duration = (state.audioPlayer && state.audioPlayer.duration) || (state.currentSong && state.currentSong.duration) || null
      if (state.audioPlayer && typeof state.audioPlayer.moveToPosition === 'function') {
        state.audioPlayer.moveToPosition(null, timePercent)
      } else if (state.audioPlayer && typeof state.audioPlayer.seekToPosition === 'function') {
        const pos = duration ? timePercent * duration : null
        state.audioPlayer.seekToPosition(pos)
      } else if (state.audioPlayer && state.audioPlayer.sound && typeof state.audioPlayer.sound.seek === 'function') {
        const pos = duration ? timePercent * duration : null
        state.audioPlayer.sound.seek(pos)
      }
      commit('SET_PLAYING', true)
    } catch (err) {
      console.error('moveToTime error', err)
    }
  },

  prevSong({ state, commit }) {
    try {
      if (state.playlistManager && typeof state.playlistManager.prev === 'function') {
        state.playlistManager.prev()
        const song = state.playlistManager.getCurrentSong ? state.playlistManager.getCurrentSong() : null
        if (song && song.id) {
          // start
          if (state.audioPlayer && typeof state.audioPlayer.start === 'function') {
            state.audioPlayer.start(song.id, state.volume)
          }
          commit('SET_PLAYING', true)
          if (song) commit('SET_CURRENT_SONG', song)
          setTimeUpdater(state, commit)
        }
      }
    } catch (err) {
      console.error('prevSong error', err)
    }
  },

  nextSong({ state, commit }) {
    try {
      if (state.playlistManager && typeof state.playlistManager.next === 'function') {
        state.playlistManager.next()
        const song = state.playlistManager.getCurrentSong ? state.playlistManager.getCurrentSong() : null
        if (song && song.id) {
          if (state.audioPlayer && typeof state.audioPlayer.start === 'function') {
            state.audioPlayer.start(song.id, state.volume)
          }
          commit('SET_PLAYING', true)
          if (song) commit('SET_CURRENT_SONG', song)
          setTimeUpdater(state, commit)
        }
      }
    } catch (err) {
      console.error('nextSong error', err)
    }
  },

  shuffleList({ state, commit }) {
    try {
      if (state.playlistManager && typeof state.playlistManager.shuffle === 'function') {
        state.playlistManager.shuffle()
        if (Array.isArray(state.playlistManager.songs)) {
          commit('SET_SONGS', state.playlistManager.songs)
        }
      }
    } catch (err) {
      console.error('shuffleList error', err)
    }
  },

  setAndUnsetRepeat({ state, commit }) {
    commit('SET_REPEAT', !state.repeat)
  },

  async fetchSongs({ state, commit, dispatch }) {
    if (!state.playlistManager) {
      await dispatch('initializeServices')
    }
    try {
      let songs = []
      if (state.playlistManager && typeof state.playlistManager.loadSongs === 'function') {
        songs = await state.playlistManager.loadSongs()
      } else if (state.playlistManager && typeof state.playlistManager.load === 'function') {
        songs = await state.playlistManager.load()
      } else {
        songs = state.songs || []
      }
      commit('SET_SONGS', songs)
      return songs
    } catch (err) {
      console.error('fetchSongs error', err)
      return []
    }
  },

  async refreshSongs({ state, commit }) {
    try {
      let songs = []
      if (state.playlistManager && typeof state.playlistManager.refresh === 'function') {
        songs = await state.playlistManager.refresh()
      } else if (state.playlistManager && typeof state.playlistManager.reload === 'function') {
        songs = await state.playlistManager.reload()
      }
      commit('SET_SONGS', songs)
      return songs
    } catch (err) {
      console.error('refreshSongs error', err)
      return []
    }
  },

  async filter({ state, commit }, { album = null, artist = null, name = null }) {
    commit('SET_FILTERS', { album, artist })
    try {
      let filtered = state.songs
      if (state.playlistManager && typeof state.playlistManager.filter === 'function') {
        filtered = await state.playlistManager.filter({ artist, album, name })
      } else {
        filtered = (state.songs || []).filter(s => {
          let ok = true
          if (artist) ok = ok && (s.artist_names ? s.artist_names.includes(artist) : (s.artist === artist))
          if (album) ok = ok && (s.album_name ? s.album_name === album : (s.album === album))
          if (name) ok = ok && (s.title ? s.title.toLowerCase().includes(name.toLowerCase()) : (s.title && s.title.toLowerCase().includes(name.toLowerCase())))
          return ok
        })
      }
      commit('SET_SONGS', filtered)
      return filtered
    } catch (err) {
      console.error('filter error', err)
      return []
    }
  }
}

function setTimeUpdater(state, commit) {
  const interval = setInterval(() => {
    try {
      if (state.audioPlayer && state.audioPlayer.sound && typeof state.audioPlayer.sound.playing === 'function' && state.audioPlayer.sound.playing()) {
        const currentTime = typeof state.audioPlayer.sound.seek === 'function' ? state.audioPlayer.sound.seek() : (state.audioPlayer.currentTime || 0)
        updateTime(currentTime)
      } else {
        clearInterval(interval)
      }
    } catch (err) {
      clearInterval(interval)
      console.error('setTimeUpdater error', err)
    }
  }, 1000)

  function updateTime(currentTime) {
    const duration = state.audioPlayer && state.audioPlayer.duration ? state.audioPlayer.duration : (state.currentSong && state.currentSong.duration ? state.currentSong.duration : 0)
    const remainingTime = Math.max(duration - (currentTime || 0), 0)

    const formattedRemainingTime = formatTime(remainingTime)
    const formattedTotalDuration = formatTime(duration)
    const progressBarProgress = duration ? Math.floor(((currentTime || 0) / duration) * 100) : 0

    commit('SET_TIMES', { remaining: formattedRemainingTime, total: formattedTotalDuration, progress: progressBarProgress })
  }

  function formatTime(seconds) {
    seconds = Math.floor(seconds || 0)
    const totalMinutes = Math.floor(seconds / 60)
    const totalSeconds = Math.floor(seconds % 60)
    const formattedMinutes = String(totalMinutes).padStart(2, '0')
    const formattedSeconds = String(totalSeconds).padStart(2, '0')
    return `${formattedMinutes}:${formattedSeconds}`
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}
