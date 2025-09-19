import AudioPlayerService from '@/services/AudioPlayerService'

const state = {
    audioPlayer: null,
    currentTrack: {
        id: null,
        title: 'Selecciona una canción',
        artist: 'Artista',
        album: 'Álbum',
        imageUrl: 'https://via.placeholder.com/250',
        duration: 0
    },
    isPlaying: false,
    formattedElapsedTime: '0:00',
    formattedTotalDuration: '0:00',
    playbackProgress: 0,
    volumeLevel: 70,
    isShuffleActive: false,
    isRepeatActive: false
}

const mutations = {
    SET_AUDIO_PLAYER(state, playerInstance) {
        state.audioPlayer = playerInstance
    },
    SET_CURRENT_TRACK(state, trackData) {
        state.currentTrack = { ...state.currentTrack, ...trackData }
    },
    SET_PLAYBACK_STATUS(state, isPlaying) {
        state.isPlaying = isPlaying
    },
    SET_PLAYBACK_TIMES(state, { elapsed, total, progress }) {
        state.formattedElapsedTime = elapsed
        state.formattedTotalDuration = total
        state.playbackProgress = progress
    },
    SET_VOLUME_LEVEL(state, volumeLevel) {
        state.volumeLevel = volumeLevel
    },
    TOGGLE_SHUFFLE(state) {
        state.isShuffleActive = !state.isShuffleActive
    },
    TOGGLE_REPEAT(state) {
        state.isRepeatActive = !state.isRepeatActive
    }
}

const actions = {
    async initializeAudioPlayer({ commit, dispatch }) {
        try {
            const audioPlayer = new AudioPlayerService()
            commit('SET_AUDIO_PLAYER', audioPlayer)

            audioPlayer.onPlaybackProgressUpdate((progressData) => {
                dispatch('updatePlaybackProgress', progressData)
            })

            return audioPlayer
        } catch (error) {
            console.error('Failed to initialize audio player:', error)
            throw error
        }
    },

    // async selectTrack({ commit, state, dispatch }, trackId) {
    //     try {
    //         if (!state.audioPlayer) {
    //             await dispatch('initializeAudioPlayer')
    //         }

    //         const trackMetadata = await state.audioPlayer.initializeAudioPlayer(trackId)

    //         commit('SET_CURRENT_TRACK', {
    //             id: trackId,
    //             duration: trackMetadata.duration
    //         })

    //         commit('SET_PLAYBACK_TIMES', {
    //             elapsed: '0:00',
    //             total: formatTime(trackMetadata.duration),
    //             progress: 0
    //         })

    //         await dispatch('playAudio')
    //     } catch (error) {
    //         console.error('Failed to select track:', error)
    //     }
    // },

    async selectTrack({ commit, state, dispatch }, track) {
        try {
            if (!state.audioPlayer) {
                await dispatch('initializeAudioPlayer')
            }

            // Actualizar la canción actual
            commit('SET_CURRENT_TRACK', {
                id: track.id,
                title: track.title,
                artist: track.artist,
                album: track.album,
                duration: track.duration
            })

            // Iniciar la reproducción
            await state.audioPlayer.initializeAudioPlayer(track.id)
            commit('SET_PLAYBACK_STATUS', true)

            // Configurar el actualizador de tiempo
            setTimeUpdater(state, commit);
        } catch (error) {
            console.error('Failed to select track:', error)
        }
    },

    async playAudio({ commit, state }) {
        if (state.audioPlayer) {
            state.audioPlayer.playAudio()
            commit('SET_PLAYBACK_STATUS', true)
        }
    },

    pauseAudio({ commit, state }) {
        if (state.audioPlayer) {
            state.audioPlayer.pauseAudio()
            commit('SET_PLAYBACK_STATUS', false)
        }
    },

    togglePlayback({ state, dispatch }) {
        if (state.isPlaying) {
            dispatch('pauseAudio')
        } else {
            dispatch('playAudio')
        }
    },

    updatePlaybackProgress({ commit }, progressData) {
        commit('SET_PLAYBACK_TIMES', {
            elapsed: progressData.formattedTime,
            total: state.formattedTotalDuration,
            progress: progressData.progress
        })
    },

    seekToPosition({ state }, progressEvent) {
        if (state.audioPlayer) {
            const progressPercent = progressEvent.target.value / 100
            const seekPosition = progressPercent * state.currentTrack.duration
            state.audioPlayer.seekToPosition(seekPosition)
        }
    },

    updateVolumeLevel({ commit, state }, volumeEvent) {
        const volumeLevel = volumeEvent.target.value
        commit('SET_VOLUME_LEVEL', volumeLevel)

        if (state.audioPlayer) {
            state.audioPlayer.setVolume(volumeLevel)
        }
    },

    toggleShuffle({ commit }) {
        commit('TOGGLE_SHUFFLE')
    },

    toggleRepeat({ commit }) {
        commit('TOGGLE_REPEAT')
    },

    playNextTrack({ state, dispatch }) {
        // Por Implementar
        state = "state"
        dispatch = "dispatch"
        console.log('Playing next track: ', state, dispatch)
    },

    playPreviousTrack({ state, dispatch }) {
        // Por Implementar
        state = "state"
        dispatch = "dispatch"
        console.log('Playing previous track: ', state, dispatch)
    }
}

function setTimeUpdater(state, commit) {
  const interval = setInterval(() => {
    if (state.audioPlayer && state.audioPlayer.sound && state.audioPlayer.sound.playing()) {
      const currentTime = state.audioPlayer.sound.seek();
      updateTime(currentTime)
    } else {
      clearInterval(interval)
    }
  }, 1000)

  function updateTime(currentTime) {
    const remainingTime = state.audioPlayer.duration - currentTime;

    const formattedRemainingTime = formatTime(remainingTime);
    const formattedTotalDuration = formatTime(state.audioPlayer.duration);
    const progressBarProgress = Math.floor((currentTime / state.audioPlayer.duration) * 100);

    commit('SET_TIMES', { 
      remaining: formattedRemainingTime, 
      total: formattedTotalDuration, 
      progress: progressBarProgress 
    });
  }
  
  function formatTime(seconds) {
    const totalMinutes = Math.floor(seconds / 60);
    const totalSeconds = Math.floor(seconds % 60);
    const formattedMinutes = String(totalMinutes).padStart(2, '0');
    const formattedSeconds = String(totalSeconds).padStart(2, '0');

    return `${formattedMinutes}:${formattedSeconds}`;
  }
}

export default {
    namespaced: true,
    state,
    mutations,
    actions
}