<!-- src/components/auth/ChangePasswordModal.vue -->
<template>
  <div class="modal-overlay" @click.self="close">
    <div class="modal-container">
      <div class="modal-header">
        <h3>Cambiar Contraseña</h3>
        <button class="modal-close" @click="close">
          <i class="fas fa-times"></i>
        </button>
      </div>
      
      <div class="modal-body">
        <form @submit.prevent="handleSubmit">
          <div class="form-group">
            <label>Contraseña actual</label>
            <input
              :type="showCurrentPassword ? 'text' : 'password'"
              v-model="form.currentPassword"
              required
              placeholder="Ingresa tu contraseña actual"
            />
            <button type="button" @click="toggleCurrentPasswordVisibility">
              <i :class="showCurrentPassword ? 'fas fa-eye-slash' : 'fas fa-eye'"></i>
            </button>
          </div>
          
          <div class="form-group">
            <label>Nueva contraseña</label>
            <input
              :type="showNewPassword ? 'text' : 'password'"
              v-model="form.newPassword"
              required
              placeholder="Ingresa tu nueva contraseña"
            />
            <button type="button" @click="toggleNewPasswordVisibility">
              <i :class="showNewPassword ? 'fas fa-eye-slash' : 'fas fa-eye'"></i>
            </button>
          </div>
          
          <div class="form-group">
            <label>Confirmar nueva contraseña</label>
            <input
              :type="showConfirmPassword ? 'text' : 'password'"
              v-model="form.confirmPassword"
              required
              placeholder="Confirma tu nueva contraseña"
            />
            <button type="button" @click="toggleConfirmPasswordVisibility">
              <i :class="showConfirmPassword ? 'fas fa-eye-slash' : 'fas fa-eye'"></i>
            </button>
          </div>
          
          <div v-if="errorMessage" class="error-message">
            {{ errorMessage }}
          </div>
          
          <div v-if="successMessage" class="success-message">
            {{ successMessage }}
          </div>
          
          <div class="modal-actions">
            <button type="button" class="btn-cancel" @click="close">
              Cancelar
            </button>
            <button type="submit" class="btn-submit" :disabled="isLoading">
              <span v-if="isLoading" class="spinner"></span>
              Cambiar Contraseña
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<script>
import AuthService from '@/services/AuthService'

export default {
  name: 'ChangePasswordModal',
  data() {
    return {
      form: {
        currentPassword: '',
        newPassword: '',
        confirmPassword: ''
      },
      showCurrentPassword: false,
      showNewPassword: false,
      showConfirmPassword: false,
      isLoading: false,
      errorMessage: '',
      successMessage: ''
    }
  },
  methods: {
    close() {
      this.$emit('close')
    },
    
    toggleCurrentPasswordVisibility() {
      this.showCurrentPassword = !this.showCurrentPassword
    },
    
    toggleNewPasswordVisibility() {
      this.showNewPassword = !this.showNewPassword
    },
    
    toggleConfirmPasswordVisibility() {
      this.showConfirmPassword = !this.showConfirmPassword
    },
    
    async handleSubmit() {
  // Validaciones
  if (this.form.newPassword !== this.form.confirmPassword) {
    this.errorMessage = 'Las contraseñas nuevas no coinciden'
    return
  }
  
  if (this.form.newPassword.length < 8) {
    this.errorMessage = 'La nueva contraseña debe tener al menos 8 caracteres'
    return
  }
  
  if (this.form.currentPassword === this.form.newPassword) {
    this.errorMessage = 'La nueva contraseña debe ser diferente a la actual'
    return
  }
  
  this.isLoading = true
  this.errorMessage = ''
  this.successMessage = ''
  
  try {
    console.log('🔐 [CHANGE PASSWORD] Enviando request...')
    console.log('🔐 [CHANGE PASSWORD] Current password length:', this.form.currentPassword.length)
    console.log('🔐 [CHANGE PASSWORD] New password length:', this.form.newPassword.length)
    
    const response = await AuthService.changePassword({
      current_password: this.form.currentPassword,  // texto plano
      new_password: this.form.newPassword           // texto plano
    })
    
    console.log('✅ [CHANGE PASSWORD] Respuesta exitosa:', response)
    
    this.successMessage = '¡Contraseña cambiada exitosamente!'
    setTimeout(() => {
      this.$emit('success', 'Contraseña actualizada')
      this.$emit('close')
    }, 2000)
    
  } catch (error) {
    console.error('❌ [CHANGE PASSWORD] Error completo:', error)
    console.error('❌ [CHANGE PASSWORD] Response data:', error.response?.data)
    console.error('❌ [CHANGE PASSWORD] Response status:', error.response?.status)
    
    this.errorMessage = error.response?.data?.current_password?.[0] || 
                        error.response?.data?.new_password?.[0] ||
                        error.response?.data?.detail || 
                        error.response?.data?.error ||
                        'Error al cambiar la contraseña'
  } finally {
    this.isLoading = false
  }
}
  }
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.7);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 2000;
  backdrop-filter: blur(4px);
}

.modal-container {
  background: #282828;
  border-radius: 12px;
  width: 90%;
  max-width: 400px;
  border: 1px solid #404040;
  overflow: hidden;
  animation: slideIn 0.3s ease;
}

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateY(-20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px;
  background: #1f1f1f;
  border-bottom: 1px solid #404040;
}

.modal-header h3 {
  margin: 0;
  color: white;
  font-size: 1.3rem;
}

.modal-close {
  background: none;
  border: none;
  color: #999;
  font-size: 1.2rem;
  cursor: pointer;
  padding: 5px;
  border-radius: 4px;
  transition: all 0.2s;
}

.modal-close:hover {
  color: white;
  background: rgba(255, 255, 255, 0.1);
}

.modal-body {
  padding: 30px;
}

.form-group {
  margin-bottom: 20px;
  position: relative;
}

.form-group label {
  display: block;
  margin-bottom: 8px;
  color: #ccc;
  font-size: 0.9rem;
  font-weight: 500;
}

.form-group input {
  width: 100%;
  padding: 12px 40px 12px 15px;
  background: #1a1a1a;
  border: 1px solid #404040;
  border-radius: 8px;
  color: white;
  font-size: 0.95rem;
  transition: all 0.2s;
}

.form-group input:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px rgba(29, 185, 84, 0.2);
}

.form-group button {
  position: absolute;
  right: 12px;
  top: 38px;
  background: none;
  border: none;
  color: #999;
  cursor: pointer;
  padding: 0;
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.form-group button:hover {
  color: white;
}

.error-message {
  background: rgba(220, 53, 69, 0.1);
  color: #dc3545;
  padding: 10px 15px;
  border-radius: 8px;
  margin: 15px 0;
  font-size: 0.9rem;
  border: 1px solid rgba(220, 53, 69, 0.3);
}

.success-message {
  background: rgba(40, 167, 69, 0.1);
  color: #28a745;
  padding: 10px 15px;
  border-radius: 8px;
  margin: 15px 0;
  font-size: 0.9rem;
  border: 1px solid rgba(40, 167, 69, 0.3);
}

.modal-actions {
  display: flex;
  gap: 15px;
  margin-top: 25px;
}

.btn-cancel, .btn-submit {
  flex: 1;
  padding: 12px;
  border-radius: 8px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  border: none;
  font-size: 0.95rem;
}

.btn-cancel {
  background: #404040;
  color: #ccc;
}

.btn-cancel:hover {
  background: #505050;
  color: white;
}

.btn-submit {
  background: linear-gradient(135deg, var(--color-primary) 0%, #1ed760 100%);
  color: white;
  position: relative;
}

.btn-submit:hover:not(:disabled) {
  transform: scale(1.02);
  box-shadow: 0 4px 15px rgba(29, 185, 84, 0.3);
}

.btn-submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-radius: 50%;
  border-top-color: white;
  animation: spin 1s ease-in-out infinite;
  margin-right: 8px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}
</style>