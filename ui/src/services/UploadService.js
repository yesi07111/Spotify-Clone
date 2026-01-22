// services/UploadService.js
import ApiService from './ApiService'

export default class UploadService {
  static async uploadTrack(file, trackData = {}) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      
      reader.onload = async (e) => {
        try {
          const base64 = e.target.result.split(',')[1]
          
          const trackPayload = {
            ...trackData,
            file_base64: base64,
            title: trackData.title || file.name.replace(/\.[^/.]+$/, ""),
          }

          // Usar ApiService para crear el track
          const response = await ApiService.createTrack(trackPayload)
          resolve(response)
        } catch (error) {
          reject(error)
        }
      }
      
      reader.onerror = () => reject(new Error('Error reading file'))
      reader.readAsDataURL(file)
    })
  }

  static async getArtists() {
    const response = await ApiService.getArtists()
    return response.data
  }

  static async getAlbums() {
    const response = await ApiService.getAlbums()
    return response.data
  }

  static async createArtist(artistData) {
    const response = await ApiService.createArtist(artistData)
    return response.data.data
  }

  static async createAlbum(albumData) {
    const response = await ApiService.createAlbum(albumData)
    return response.data.data
  }

  static async updateTrack(id, trackData) {
    try {
      const response = await ApiService.updateTrack(id, trackData)
          return response.data.data

    }
    catch (error){
      console.log("Error actualizando: " + error)
    }
  }

    static async deleteArtist(artistId) {
    if (!artistId) {
      throw new Error('artistId es obligatorio')
    }

    const response = await ApiService.deleteArtist(artistId)
    return response.data
  }

  static async deleteAlbum(albumId) {
    if (!albumId) {
      throw new Error('albumId es obligatorio')
    }

    const response = await ApiService.deleteAlbum(albumId)
    return response.data
  }

}

