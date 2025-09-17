export const formatTime = (seconds) => {
  const minutes = Math.floor(seconds / 60)
  const remainingSeconds = Math.floor(seconds % 60)
  return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`
}

export const calculatePlaybackProgress = (currentTime, totalDuration) => {
  return totalDuration > 0 ? Math.floor((currentTime / totalDuration) * 100) : 0
}