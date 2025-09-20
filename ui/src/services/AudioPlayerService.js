import { Howl } from 'howler'
import { API_BASE_URL, DEFAULT_AUDIO_ID } from '@/utils/constants'
import { formatTime, calculatePlaybackProgress } from '@/utils/audioHelper'

export default class AudioPlayerService {
  constructor(host = API_BASE_URL) {
    this.host = host
    this.audioInstance = null
    this.sound = null
    this.audioId = null
    this.chunksToLoad = 10
    this.isInitialized = false
    this.playbackUpdateInterval = null
    this.playbackProgressCallback = null
    this.onPlaybackEnd = () => {}
    this.isPlaying = false
    this.totalChunks = 0
    this.chunkSize = 0
    this.duration = 0
    this.fileSize = 0
    this.audioByteArray = null
  }

  async start(audioId = DEFAULT_AUDIO_ID, volume = null) {
    if (audioId !== this.audioId) {
      this.audioId = audioId
      const initData = await this.executeApiRequest({
        chunk_index: 0,
        chunk_count: this.chunksToLoad,
        audio_id: this.audioId,
        include_metadata: true,
        include_header: true
      })

      const metadata = initData.metadata || {}
      this.initAudio(metadata)

      for (let index = 0; index <= this.totalChunks; index += this.chunksToLoad) {
        await this.loadChunks(index, Math.min(this.chunksToLoad, this.totalChunks - index))
      }
    }

    this.isPlaying = true
    this.playAudioBuffer()

    this.setVolume(volume ?? 50)
  }

  async executeApiRequest(params) {
    try {
      const queryParams = new URLSearchParams()
      Object.entries(params).forEach(([key, value]) => {
        queryParams.append(key, typeof value === 'boolean' ? value.toString() : value)
      })

      const apiUrl = `${this.host}?${queryParams.toString()}`
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

  initAudio(metadata) {
    this.isPlaying = false
    this.totalChunks = metadata.total_chunks || 0
    this.chunkSize = metadata.chunk_size || 0
    this.duration = metadata.duration || 0
    this.fileSize = metadata.file_size || (this.totalChunks * this.chunkSize) || 0
    this.audioByteArray = new Uint8Array(this.fileSize).fill(0)
    this.sound = null
  }

  async loadChunks(chunkIndex, chunkCount = null) {
    const data = await this.executeApiRequest({
      chunk_index: chunkIndex,
      chunk_count: chunkCount ?? this.chunksToLoad,
      audio_id: this.audioId,
      include_header: false,
      include_metadata: false
    })

    this.addChunks(data.chunks, data.chunk_index, data.chunk_count)
  }

  addChunks(chunks, chunkIndex, chunkCount) {
    for (let i = 0; i < chunkCount; i++) {
      const index = chunkIndex + i
      this.compileChunk(chunks[i], index)
    }
  }

  base64ToByteArray(base64) {
    const binaryString = atob(base64)
    const byteArray = new Uint8Array(binaryString.length)
    for (let i = 0; i < binaryString.length; i++) {
      byteArray[i] = binaryString.charCodeAt(i)
    }
    return byteArray
  }

  compileChunk(chunk, index) {
    const offset = index * this.chunkSize
    const chunkAsBytes = this.base64ToByteArray(chunk)
    if (!this.audioByteArray) {
      this.audioByteArray = new Uint8Array((this.totalChunks + 1) * this.chunkSize).fill(0)
    }
    this.audioByteArray.set(chunkAsBytes, offset)
  }

  playAndPause() {
    if (!this.sound) return
    if (this.sound.playing && this.sound.playing()) {
      this.sound.pause()
    } else {
      this.sound.play()
    }
    this.isPlaying = this.sound.playing && this.sound.playing()
  }

  moveToPosition(positionInSeconds = null, positionInPercent = null) {
    if (positionInSeconds === null && positionInPercent !== null) {
      positionInSeconds = positionInPercent * this.duration
    }
    if (this.sound && typeof this.sound.seek === 'function') {
      this.sound.seek(positionInSeconds)
    }
  }

  setVolume(value = null) {
    const volume = Math.max(0, Math.min(1, (value || 0) / 100))
    if (this.sound && typeof this.sound.volume === 'function') {
      this.sound.volume(volume)
    }
  }

  playAudioBuffer() {
    const blob = new Blob([this.audioByteArray], { type: 'audio/mp3' })
    const blobURL = URL.createObjectURL(blob)

    if (this.sound) {
      this.sound.unload()
    }

    this.sound = new Howl({
      src: [blobURL],
      format: ['mp3', 'wav', 'aac'],
      html5: true,
      onloaderror: (id, error) => {
        console.error('Error loading audio:', error)
      },
      onend: () => {
        this.onPlaybackEnd && this.onPlaybackEnd()
        this.stopPlaybackUpdates()
      }
    })

    this.sound.play()
    this.isPlaying = true
    this.startPlaybackUpdates()
  }

  async loadAudioChunks() {
    for (let chunkIndex = 0; chunkIndex <= this.totalChunks; chunkIndex += this.chunksToLoad) {
      const chunksToLoad = Math.min(this.chunksToLoad, this.totalChunks - chunkIndex)
      await this.loadChunkRange(chunkIndex, chunksToLoad)
    }
  }

  async loadChunkRange(chunkIndex, chunkCount) {
    const responseData = await this.executeApiRequest({
      chunk_index: chunkIndex,
      chunk_count: chunkCount,
      audio_id: this.audioId,
      include_header: false,
      include_metadata: false
    })
    this.processAudioChunks(responseData.chunks, responseData.chunk_index, responseData.chunk_count)
  }

  processAudioChunks(chunks, chunkIndex, chunkCount) {
    for (let i = 0; i < chunkCount; i++) {
      const currentIndex = chunkIndex + i
      this.compileChunk(chunks[i], currentIndex)
    }
  }

  startPlaybackUpdates() {
    this.stopPlaybackUpdates()
    this.playbackUpdateInterval = setInterval(() => {
      if (this.sound && this.sound.playing && this.sound.playing()) {
        const currentTime = this.sound.seek()
        const progress = calculatePlaybackProgress(currentTime, this.duration)
        const formattedTime = formatTime(currentTime)
        if (this.playbackProgressCallback) {
          this.playbackProgressCallback({
            currentTime,
            progress,
            formattedTime
          })
        }
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
    if (this.sound) {
      this.sound.pause()
      this.stopPlaybackUpdates()
      this.isPlaying = false
    }
  }

  seekToPosition(positionInSeconds) {
    if (this.sound) {
      this.sound.seek(positionInSeconds)
    }
  }

  setVolumeLevel(volumeLevel) {
    if (this.sound) {
      const normalizedVolume = Math.max(0, Math.min(1, volumeLevel / 100))
      this.sound.volume(normalizedVolume)
    }
  }

  destroy() {
    this.stopPlaybackUpdates()
    if (this.sound) {
      this.sound.unload()
      this.sound = null
    }
    this.isInitialized = false
    this.audioId = null
    this.audioByteArray = null
  }

  onPlaybackProgressUpdate(callback) {
    this.playbackProgressCallback = callback
  }
}
