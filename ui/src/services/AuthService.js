// services/AuthService.js
import axios from 'axios'
import CryptoJS from 'crypto-js'

const API_BASE_URL = process.env.VUE_APP_API_URL || 'https://127.0.0.1:8000/api'

// Configuración de cifrado (en producción debería ser una variable de entorno)
const ENCRYPTION_KEY = process.env.VUE_APP_ENCRYPTION_KEY || 'spotify-clone-distribuido-2024-seguridad'
const TOKEN_KEY = 'spotify_auth_token'
const USER_KEY = 'spotify_user_data'

// Función para cifrar datos
const encryptData = (data) => {
    try {
        const jsonData = JSON.stringify(data)
        const encrypted = CryptoJS.AES.encrypt(jsonData, ENCRYPTION_KEY).toString()
        return encrypted
    } catch (error) {
        console.error('Error cifrando datos:', error)
        return null
    }
}

// Función para descifrar datos
const decryptData = (encryptedData) => {
    try {
        if (!encryptedData) return null
        const bytes = CryptoJS.AES.decrypt(encryptedData, ENCRYPTION_KEY)
        const decrypted = bytes.toString(CryptoJS.enc.Utf8)
        return decrypted ? JSON.parse(decrypted) : null
    } catch (error) {
        console.error('Error descifrando datos:', error)
        return null
    }
}

// Almacenamiento seguro en Session Storage con cifrado
const secureStorage = {
    setItem(key, data) {
        const encrypted = encryptData(data)
        if (encrypted) {
            sessionStorage.setItem(key, encrypted)
        }
    },
    
    getItem(key) {
        const encrypted = sessionStorage.getItem(key)
        return decryptData(encrypted)
    },
    
    removeItem(key) {
        sessionStorage.removeItem(key)
    },
    
    clear() {
        sessionStorage.removeItem(TOKEN_KEY)
        sessionStorage.removeItem(USER_KEY)
    }
}

// Cliente Axios para autenticación
const authClient = axios.create({
    baseURL: API_BASE_URL,
    timeout: 30000,
    headers: {
        'Content-Type': 'application/json',
    },
    withCredentials: true, // Para cookies seguras
})

// Interceptor para agregar token a las peticiones
authClient.interceptors.request.use(
    (config) => {
        const token = secureStorage.getItem(TOKEN_KEY)
        if (token) {
            config.headers.Authorization = `Bearer ${token.access}`
        }
        return config
    },
    (error) => {
        return Promise.reject(error)
    }
)

// Interceptor para manejar tokens expirados
authClient.interceptors.response.use(
    (response) => response,
    async (error) => {
        const originalRequest = error.config
        
        if (error.response?.status === 401 && !originalRequest._retry) {
            originalRequest._retry = true
            
            try {
                const token = secureStorage.getItem(TOKEN_KEY)
                if (token?.refresh) {
                    const response = await authClient.post('/auth/token/refresh/', {
                        refresh: token.refresh
                    })
                    
                    const newToken = response.data
                    secureStorage.setItem(TOKEN_KEY, {
                        ...token,
                        access: newToken.access
                    })
                    
                    originalRequest.headers.Authorization = `Bearer ${newToken.access}`
                    return authClient(originalRequest)
                }
            } catch (refreshError) {
                // Si el refresh token expiró, cerrar sesión
                secureStorage.clear()
                window.location.href = '/login'
                return Promise.reject(refreshError)
            }
        }
        
        return Promise.reject(error)
    }
)

export default {
    // Registro de usuario
    async register(userData) {
        try {
            // Hash de la contraseña en el cliente antes de enviar
            const hashedPassword = CryptoJS.SHA256(userData.password).toString()
            
            const response = await authClient.post('/auth/register/', {
                username: userData.username,
                password: hashedPassword,
                email: userData.email || ''
            })
            
            if (response.data.access) {
                secureStorage.setItem(TOKEN_KEY, response.data)
                secureStorage.setItem(USER_KEY, response.data.user)
            }
            
            return response.data
        } catch (error) {
            console.error('Error en registro:', error)
            throw error
        }
    },
    
    // Login de usuario
    async login(credentials) {
        try {
            // Hash de la contraseña en el cliente antes de enviar
            const hashedPassword = CryptoJS.SHA256(credentials.password).toString()
            
            const response = await authClient.post('/auth/login/', {
                username: credentials.username,
                password: hashedPassword
            })
            
            if (response.data.access) {
                secureStorage.setItem(TOKEN_KEY, response.data)
                secureStorage.setItem(USER_KEY, response.data.user)
            }
            
            return response.data
        } catch (error) {
            console.error('Error en login:', error)
            throw error
        }
    },
    
    // Logout
    async logout() {
        try {
            await authClient.post('/auth/logout/')
        } finally {
            secureStorage.clear()
        }
    },
    
    // Obtener usuario actual
    getCurrentUser() {
        return secureStorage.getItem(USER_KEY)
    },
    
    // Obtener token
    getToken() {
        return secureStorage.getItem(TOKEN_KEY)
    },
    
    // Verificar si está autenticado
    isAuthenticated() {
        const token = secureStorage.getItem(TOKEN_KEY)
        return !!(token && token.access)
    },
    
    // Actualizar perfil
    async updateProfile(userData) {
        const response = await authClient.patch('/auth/profile/', userData)
        if (response.data) {
            secureStorage.setItem(USER_KEY, response.data)
        }
        return response.data
    },
    
    // Cambiar contraseña
    async changePassword(passwordData) {
        const hashedCurrent = CryptoJS.SHA256(passwordData.current_password).toString()
        const hashedNew = CryptoJS.SHA256(passwordData.new_password).toString()
        
        return await authClient.post('/auth/change-password/', {
            current_password: hashedCurrent,
            new_password: hashedNew
        })
    },
    
    // Eliminar cuenta
    async deleteAccount() {
        const response = await authClient.delete('/auth/delete-account/')
        secureStorage.clear()
        return response.data
    }
}