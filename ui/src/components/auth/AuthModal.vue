<!-- src/components/auth/AuthModal.vue -->
<template>
  <div class="auth-modal-overlay" v-if="isVisible" @click="closeModal">
    <div class="auth-modal-container" @click.stop>
      <div class="auth-modal-header">
        <button class="auth-modal-close" @click="closeModal">
          <i class="fas fa-times"></i>
        </button>
        <h2>{{ isLoginMode ? 'Iniciar Sesión' : 'Registrarse' }}</h2>
      </div>
      
      <div class="auth-modal-body">
        <form @submit.prevent="handleSubmit" class="auth-form">
          <div v-if="errorMessage" class="alert alert-danger">
            {{ errorMessage }}
          </div>
          
          <div class="form-group">
            <label for="username">
              <i class="fas fa-user"></i> Nombre de usuario
            </label>
            <input
              type="text"
              id="username"
              v-model="formData.username"
              required
              :disabled="isLoading"
              placeholder="Ingresa tu usuario"
              class="form-control"
            />
          </div>
          
          <div class="form-group">
            <label for="password">
              <i class="fas fa-lock"></i> Contraseña
            </label>
            <input
              :type="showPassword ? 'text' : 'password'"
              id="password"
              v-model="formData.password"
              required
              :disabled="isLoading"
              placeholder="Ingresa tu contraseña"
              class="form-control"
            />
            <button
              type="button"
              class="password-toggle"
              @click="togglePasswordVisibility"
            >
              <i :class="showPassword ? 'fas fa-eye-slash' : 'fas fa-eye'"></i>
            </button>
          </div>
          
          <div v-if="!isLoginMode" class="form-group">
            <label for="confirmPassword">
              <i class="fas fa-lock"></i> Confirmar contraseña
            </label>
            <input
              :type="showConfirmPassword ? 'text' : 'password'"
              id="confirmPassword"
              v-model="formData.confirmPassword"
              required
              :disabled="isLoading"
              placeholder="Confirma tu contraseña"
              class="form-control"
            />
            <button
              type="button"
              class="password-toggle"
              @click="toggleConfirmPasswordVisibility"
            >
              <i :class="showConfirmPassword ? 'fas fa-eye-slash' : 'fas fa-eye'"></i>
            </button>
          </div>
          
          <div v-if="!isLoginMode" class="form-group">
            <label for="email">
              <i class="fas fa-envelope"></i> Email (opcional)
            </label>
            <input
              type="email"
              id="email"
              v-model="formData.email"
              :disabled="isLoading"
              placeholder="tu@email.com"
              class="form-control"
            />
          </div>
          
          <button
            type="submit"
            class="btn-auth-submit"
            :disabled="isLoading"
          >
            <span v-if="isLoading" class="spinner-border spinner-border-sm"></span>
            {{ isLoginMode ? 'Iniciar Sesión' : 'Crear Cuenta' }}
          </button>
          
          <div class="auth-switch">
            <span>
              {{ isLoginMode ? '¿No tienes cuenta?' : '¿Ya tienes cuenta?' }}
              <button
                type="button"
                class="btn-auth-switch"
                @click="toggleMode"
                :disabled="isLoading"
              >
                {{ isLoginMode ? 'Regístrate' : 'Inicia sesión' }}
              </button>
            </span>
          </div>
        </form>
      </div>
      
      <div class="auth-modal-footer">
        <p class="security-info">
          <i class="fas fa-shield-alt"></i>
          Tus credenciales están cifradas y protegidas
        </p>
      </div>
    </div>
  </div>
</template>

<script>
import AuthService from '@/services/AuthService'

export default {
  name: 'AuthModal',
  props: {
    isVisible: {
      type: Boolean,
      required: true
    }
  },
  data() {
    return {
      isLoginMode: true,
      isLoading: false,
      showPassword: false,
      showConfirmPassword: false,
      errorMessage: '',
      formData: {
        username: '',
        password: '',
        confirmPassword: '',
        email: ''
      }
    }
  },
  methods: {
    closeModal() {
      this.resetForm()
      this.$emit('close')
    },
    
    toggleMode() {
      this.isLoginMode = !this.isLoginMode
      this.errorMessage = ''
      this.resetForm()
    },
    
    togglePasswordVisibility() {
      this.showPassword = !this.showPassword
    },
    
    toggleConfirmPasswordVisibility() {
      this.showConfirmPassword = !this.showConfirmPassword
    },
    
    resetForm() {
      this.formData = {
        username: '',
        password: '',
        confirmPassword: '',
        email: ''
      }
      this.showPassword = false
      this.showConfirmPassword = false
      this.errorMessage = ''
    },
    
    async handleSubmit() {
      // Validaciones básicas
      if (!this.formData.username || !this.formData.password) {
        this.errorMessage = 'Por favor, completa todos los campos obligatorios'
        return
      }
      
      if (!this.isLoginMode && this.formData.password !== this.formData.confirmPassword) {
        this.errorMessage = 'Las contraseñas no coinciden'
        return
      }
      
      if (this.formData.password.length < 8) {
        this.errorMessage = 'La contraseña debe tener al menos 8 caracteres'
        return
      }
      
      this.isLoading = true
      this.errorMessage = ''
      
      try {
        if (this.isLoginMode) {
          await AuthService.login({
            username: this.formData.username,
            password: this.formData.password
          })
          this.$emit('success', '¡Inicio de sesión exitoso!')
        } else {
          await AuthService.register({
            username: this.formData.username,
            password: this.formData.password,
            email: this.formData.email
          })
          this.$emit('success', '¡Cuenta creada exitosamente!')
        }
        
        this.closeModal()
        
        // Recargar la página para actualizar estado de autenticación
        setTimeout(() => {
          window.location.reload()
        }, 500)
        
      } catch (error) {
        this.errorMessage = this.getErrorMessage(error)
      } finally {
        this.isLoading = false
      }
    },
    
    getErrorMessage(error) {
      if (error.response) {
        const { status, data } = error.response
        
        switch (status) {
          case 400:
            return data.detail || 'Datos inválidos'
          case 401:
            return 'Credenciales incorrectas'
          case 409:
            return 'El nombre de usuario ya existe'
          default:
            return 'Error del servidor'
        }
      }
      return 'Error de conexión'
    }
  },
  
  watch: {
    isVisible(newVal) {
      if (newVal) {
        this.resetForm()
        document.body.style.overflow = 'hidden'
      } else {
        document.body.style.overflow = ''
      }
    }
  }
}
</script>

<style scoped>
.auth-modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(0, 0, 0, 0.8);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  backdrop-filter: blur(4px);
}

.auth-modal-container {
  background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
  border-radius: 16px;
  width: 90%;
  max-width: 400px;
  border: 1px solid #444;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
  overflow: hidden;
}

.auth-modal-header {
  padding: 20px;
  background: #121212;
  border-bottom: 1px solid #333;
  position: relative;
}

.auth-modal-header h2 {
  margin: 0;
  color: #fff;
  font-size: 1.5rem;
  text-align: center;
}

.auth-modal-close {
  position: absolute;
  right: 15px;
  top: 15px;
  background: none;
  border: none;
  color: #999;
  font-size: 1.2rem;
  cursor: pointer;
  transition: color 0.2s;
}

.auth-modal-close:hover {
  color: #fff;
}

.auth-modal-body {
  padding: 30px;
}

.auth-form .form-group {
  margin-bottom: 20px;
  position: relative;
}

.auth-form label {
  display: block;
  margin-bottom: 8px;
  color: #ccc;
  font-size: 0.9rem;
  font-weight: 500;
}

.auth-form label i {
  margin-right: 8px;
  color: #1db954;
}

.form-control {
  width: 100%;
  padding: 12px 40px 12px 15px;
  background: #282828;
  border: 1px solid #404040;
  border-radius: 8px;
  color: #fff;
  font-size: 0.95rem;
  transition: all 0.2s;
}

.form-control:focus {
  outline: none;
  border-color: #1db954;
  box-shadow: 0 0 0 2px rgba(29, 185, 84, 0.2);
}

.form-control:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.password-toggle {
  position: absolute;
  right: 12px;
  top: 38px;
  background: none;
  border: none;
  color: #999;
  cursor: pointer;
  padding: 0;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.password-toggle:hover {
  color: #fff;
}

.btn-auth-submit {
  width: 100%;
  padding: 14px;
  background: linear-gradient(135deg, #1db954 0%, #1ed760 100%);
  color: white;
  border: none;
  border-radius: 50px;
  font-size: 1rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  margin-top: 10px;
}

.btn-auth-submit:hover:not(:disabled) {
  transform: scale(1.02);
  box-shadow: 0 4px 20px rgba(29, 185, 84, 0.3);
}

.btn-auth-submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.auth-switch {
  text-align: center;
  margin-top: 20px;
  color: #999;
  font-size: 0.9rem;
}

.btn-auth-switch {
  background: none;
  border: none;
  color: #1db954;
  cursor: pointer;
  padding: 0;
  margin-left: 5px;
  font-weight: 600;
  transition: color 0.2s;
}

.btn-auth-switch:hover:not(:disabled) {
  color: #1ed760;
  text-decoration: underline;
}

.btn-auth-switch:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.auth-modal-footer {
  padding: 15px 30px;
  background: #121212;
  border-top: 1px solid #333;
  text-align: center;
}

.security-info {
  color: #666;
  font-size: 0.8rem;
  margin: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.security-info i {
  color: #1db954;
}

.alert-danger {
  background-color: rgba(220, 53, 69, 0.1);
  border: 1px solid rgba(220, 53, 69, 0.3);
  color: #dc3545;
  padding: 10px 15px;
  border-radius: 8px;
  margin-bottom: 20px;
  font-size: 0.9rem;
}

.spinner-border {
  margin-right: 8px;
}
</style>