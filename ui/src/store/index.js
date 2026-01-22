//store/index.js
import { createStore } from 'vuex'
import player from './modules/player'
import tracks from './modules/tracks'

export default createStore({
  modules: {
    player,
    tracks
  }
})