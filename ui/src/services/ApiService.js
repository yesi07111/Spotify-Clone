export class ApiService {
  static async executeApiRequest(url, params = {}) {
    try {
      const queryParams = new URLSearchParams()
      
      Object.entries(params).forEach(([key, value]) => {
        queryParams.append(key, typeof value === 'boolean' ? value.toString() : value)
      })

      const apiUrl = `${url}?${queryParams.toString()}`
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
}