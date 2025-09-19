import { Howl } from 'howler'
import { API_BASE_URL, DEFAULT_AUDIO_ID } from '@/utils/constants'
import { formatTime, calculatePlaybackProgress } from '@/utils/audioHelper'

export default class AudioPlayerService {
  constructor() {
    this.audioInstance = null
    this.audioId = null
    this.chunksToLoad = 10
    this.isInitialized = false
    this.playbackUpdateInterval = null
  }

  async initializeAudioPlayer(trackId = DEFAULT_AUDIO_ID) {
    try {
      this.audioId = trackId
      const audioMetadata = await this.fetchAudioMetadata(trackId)

      this.configureAudioProperties(audioMetadata)
      await this.loadAudioChunks()

      this.isInitialized = true
      return audioMetadata
    } catch (error) {
      console.error('Error initializing audio player:', error)
      throw error
    }
  }

  async fetchAudioMetadata(trackId) {
    const requestParams = {
      chunk_index: 0,
      chunk_count: this.chunksToLoad,
      audio_id: trackId,
      include_header: true,
      include_metadata: true
    }

    const responseData = await this.executeApiRequest(requestParams)
    return responseData.metadata
  }

  configureAudioProperties(metadata) {
    this.totalChunks = metadata.total_chunks
    this.chunkSize = metadata.chunk_size
    this.channels = metadata.channels
    this.sampleRate = metadata.sample_rate
    this.bitsPerSample = metadata.bits_per_sample
    this.duration = metadata.duration
    this.cachedChunks = Array(this.totalChunks + 1).fill(false)
  }

  async loadAudioChunks() {
    for (let chunkIndex = 0; chunkIndex <= this.totalChunks; chunkIndex += this.chunksToLoad) {
      const chunksToLoad = Math.min(this.chunksToLoad, this.totalChunks - chunkIndex)
      await this.loadChunkRange(chunkIndex, chunksToLoad)
    }
  }

  async loadChunkRange(chunkIndex, chunkCount) {
    const requestParams = {
      chunk_index: chunkIndex,
      chunk_count: chunkCount,
      audio_id: this.audioId,
      include_header: false,
      include_metadata: false
    }

    const responseData = await this.executeApiRequest(requestParams)
    this.processAudioChunks(responseData.chunks, responseData.chunk_index, responseData.chunk_count)
  }

  processAudioChunks(chunks, chunkIndex, chunkCount) {
    for (let i = 0; i < chunkCount; i++) {
      const currentIndex = chunkIndex + i
      if (this.cachedChunks[currentIndex]) continue

      this.compileAudioChunk(chunks[i], currentIndex)
      this.cachedChunks[currentIndex] = true
    }
  }

  compileAudioChunk(chunkData, chunkIndex) {
    const chunkOffset = 44 + chunkIndex * this.chunkSize
    const chunkBytes = this.convertBase64ToBytes(chunkData)
    this.audioByteArray.set(chunkBytes, chunkOffset)
  }

  convertBase64ToBytes(base64String) {
    const binaryString = atob(base64String)
    const bytesArray = new Uint8Array(binaryString.length)

    for (let i = 0; i < binaryString.length; i++) {
      bytesArray[i] = binaryString.charCodeAt(i)
    }

    return bytesArray
  }

  async executeApiRequest(params) {
    try {
      const queryParams = new URLSearchParams()

      Object.entries(params).forEach(([key, value]) => {
        queryParams.append(key, typeof value === 'boolean' ? value.toString() : value)
      })

      const apiUrl = `${API_BASE_URL}?${queryParams.toString()}`
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      })

      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`)
      }

      return await response.json()
    } catch (error) {
      console.error('API request failed:', error)
      throw error
    }
  }

  playAudio() {
    if (!this.isInitialized) {
      console.error('Audio player not initialized')
      return
    }

    const audioBlob = new Blob([this.audioByteArray], { type: 'audio/wav' })
    const audioBlobUrl = URL.createObjectURL(audioBlob)

    this.audioInstance = new Howl({
      src: [audioBlobUrl],
      format: ['wav'],
      html5: true,
      onend: () => this.handlePlaybackEnd(),
      onloaderror: (id, error) => this.handleLoadError(error)
    })

    this.audioInstance.play()
    this.startPlaybackUpdates()
  }

  handlePlaybackEnd() {
    this.stopPlaybackUpdates()
    // Implementar lógica para avisar que la canción terminó
  }

  handleLoadError(error) {
    console.error('Audio loading error:', error)
    // Implementar manejo de errores al cargar el audio (mostrar un mensaje)
  }

  startPlaybackUpdates() {
    this.playbackUpdateInterval = setInterval(() => {
      if (this.audioInstance.playing()) {
        const currentTime = this.audioInstance.seek()
        const progress = calculatePlaybackProgress(currentTime, this.duration)
        const formattedTime = formatTime(currentTime)

        // Implementar actualización de la barra de progreso o algún indicador visual
        this.onPlaybackProgressUpdate({
          currentTime,
          progress,
          formattedTime
        })
      }
    }, 1000)
  }

  stopPlaybackUpdates() {
    if (this.playbackUpdateInterval) {
      clearInterval(this.playbackUpdateInterval)
      this.playbackUpdateInterval = null
    }
  }

  pauseAudio() {
    if (this.audioInstance) {
      this.audioInstance.pause()
      this.stopPlaybackUpdates()
    }
  }

  seekToPosition(positionInSeconds) {
    if (this.audioInstance) {
      this.audioInstance.seek(positionInSeconds)
    }
  }

  setVolume(volumeLevel) {
    if (this.audioInstance) {
      const normalizedVolume = Math.max(0, Math.min(1, volumeLevel / 100))
      this.audioInstance.volume(normalizedVolume)
    }
  }

  destroy() {
    this.stopPlaybackUpdates()

    if (this.audioInstance) {
      this.audioInstance.unload()
      this.audioInstance = null
    }

    this.isInitialized = false
  }

  onPlaybackProgressUpdate(callback) {
    this.playbackProgressCallback = callback
    // Implementar guardado del callback para avisar cada vez que avance la canción
  }
}