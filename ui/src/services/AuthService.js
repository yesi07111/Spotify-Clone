// services/AuthService.js
import axios from 'axios'
import CryptoJS from 'crypto-js'

const API_BASE_URL = process.env.VUE_APP_API_URL || 'https://127.0.0.1:8000/api'
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
            localStorage.setItem(key, encrypted)
        }
    },
    
    getItem(key) {
        const encrypted = localStorage.getItem(key)
        return decryptData(encrypted)
    },
    
    removeItem(key) {
        localStorage.removeItem(key)
    },
    
    clear() {
        localStorage.removeItem(TOKEN_KEY)
        localStorage.removeItem(USER_KEY)
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

// En services/AuthService.js - Añade debugging
authClient.interceptors.request.use(
    (config) => {
        const token = secureStorage.getItem(TOKEN_KEY)
        console.log('[AUTH DEBUG] Token encontrado:', !!token)
        console.log('[AUTH DEBUG] Token completo:', token)
        
        if (token && token.access) {
            console.log('[AUTH DEBUG] Agregando Authorization header')
            config.headers.Authorization ||= `Bearer ${token.access}`
        } else {
            console.log('[AUTH DEBUG] NO hay token disponible')
        }
        
        console.log('[AUTH DEBUG] Headers finales:', config.headers)
        return config
    },
    (error) => {
        console.error('[AUTH DEBUG] Error en interceptor request:', error)
        return Promise.reject(error)
    }
)


// En services/AuthService.js - Modifica el interceptor de respuestas
authClient.interceptors.response.use(
    (response) => response,
    async (error) => {
        const originalRequest = error.config
        
        // Silenciar errores 401 para rutas específicas
        const silent401Paths = ['/auth/logout/', '/auth/token/verify/']
        const isSilentPath = silent401Paths.some(path => 
            originalRequest.url?.includes(path)
        )
        
        if (isSilentPath && error.response?.status === 401) {
            console.log('[AUTH] Petición no autenticada (esperado)')
            return Promise.resolve({ data: { silent: true } })
        }
        
        // Manejo de refresh token (solo si no es una ruta silenciosa)
        if (error.response?.status === 401 && !originalRequest._retry && !isSilentPath) {
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
                console.log('[AUTH] Refresh token expirado - cerrando sesión silenciosamente')
            }
        }
        
        // Para otros errores, podemos decidir si mostrar o no
        if (error.response?.status >= 500) {
            console.error('[API] Error del servidor:', error.message)
        }
        
        return Promise.reject(error)
    }
)

export default {
    // Registro de usuario
    async register(userData) {
        try {
            // Hash de la contraseña en el cliente antes de enviar
            // const hashedPassword = CryptoJS.SHA256(userData.password).toString()
            
            const response = await authClient.post('/auth/register/', {
                username: userData.username,
                password: userData.password,
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
            console.log('[LOGIN DEBUG] Credenciales recibidas:', {
                username: credentials.username,
                passwordLength: credentials.password.length
            })
            
            const response = await authClient.post('/auth/login/', {
                username: credentials.username,
                password: credentials.password
            })
            
            console.log('[LOGIN DEBUG] Respuesta del servidor:', response.data)
            
            if (response.data.access) {
                // Guardar token COMPLETO (access y refresh)
                const tokenData = {
                    access: response.data.access,
                    refresh: response.data.refresh,
                    user: response.data.user
                }
                
                console.log('[LOGIN DEBUG] Guardando token:', tokenData)
                secureStorage.setItem(TOKEN_KEY, tokenData)
                secureStorage.setItem(USER_KEY, response.data.user)
                
                // Verificar que se guardó
                const savedToken = secureStorage.getItem(TOKEN_KEY)
                console.log('[LOGIN DEBUG] Token guardado verificado:', savedToken)
            }
            
            return response.data
        } catch (error) {
            console.error('[LOGIN DEBUG] Error completo:', error.response || error)
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

    // Refrescar token (USADO POR ApiService)
    async refreshToken() {
    const token = secureStorage.getItem(TOKEN_KEY)

    if (!token || !token.refresh) {
        throw new Error('No refresh token disponible')
    }

    // ⚠️ IMPORTANTE: request SIN Authorization header
    const refreshClient = axios.create({
        baseURL: API_BASE_URL,
        headers: {
            'Content-Type': 'application/json'
        },
        withCredentials: true
    })

    const response = await refreshClient.post('/auth/token/refresh/', {
        refresh: token.refresh
    })

    const newToken = {
        ...token,
        access: response.data.access
    }

    secureStorage.setItem(TOKEN_KEY, newToken)
    return response.data.access
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

        return await authClient.post('/auth/change-password/', {
        current_password: passwordData.current_password,
        new_password: passwordData.new_password
    })
    },
    

    // Eliminar cuenta
    async deleteAccount() {
        const response = await authClient.delete('/auth/delete-account/')
        secureStorage.clear()
        return response.data
    }
}

