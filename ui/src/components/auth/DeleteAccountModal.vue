<!-- src/components/auth/DeleteAccountModal.vue -->
<template>
  <div v-if="isVisible" class="modal-overlay" @click.self="close">
    <div class="modal-content">
      <div class="modal-header">
        <h2>Eliminar Cuenta</h2>
        <button class="close-button" @click="close">×</button>
      </div>
      
      <div class="modal-body">
        <p>¿Estás seguro de que deseas eliminar tu cuenta? Esta acción no se puede deshacer.</p>
        
        <form @submit.prevent="handleDeleteAccount" class="delete-form">
          <div class="form-group">
            <label for="password">Confirma tu contraseña para eliminar la cuenta:</label>
            <input
              type="password"
              id="password"
              v-model="password"
              placeholder="Ingresa tu contraseña"
              required
              class="form-input"
            />
          </div>
          
          <div class="form-actions">
            <button type="button" class="btn-cancel" @click="close">
              Cancelar
            </button>
            <button type="submit" class="btn-delete" :disabled="isLoading">
              <span v-if="isLoading">Eliminando...</span>
              <span v-else>Eliminar Cuenta</span>
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
  name: 'DeleteAccountModal',
  props: {
    isVisible: {
      type: Boolean,
      default: false
    }
  },
  data() {
    return {
      password: '',
      isLoading: false,
      error: null
    }
  },
  watch: {
    isVisible(newVal) {
      if (!newVal) {
        this.resetForm()
      }
    }
  },
  methods: {
    close() {
      this.$emit('close')
    },
    
    resetForm() {
      this.password = ''
      this.isLoading = false
      this.error = null
    },
    
    async handleDeleteAccount() {
      if (!this.password) {
        alert('Por favor ingresa tu contraseña')
        return
      }
      
      this.isLoading = true
      this.error = null
      
      try {
        // Aquí deberías llamar a tu API para eliminar la cuenta
        const response = await AuthService.deleteAccount({
          password: this.password
        })
        
        if (response.success) {
          this.$emit('success', 'Cuenta eliminada exitosamente')
          this.resetForm()
        } else {
          this.error = response.message || 'Error al eliminar la cuenta'
          alert(this.error)
        }
      } catch (error) {
        console.error('Error deleting account:', error)
        this.error = error.message || 'Ocurrió un error al eliminar la cuenta'
        alert(this.error)
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
  background-color: rgba(0, 0, 0, 0.8);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.modal-content {
  background: #181818;
  border-radius: 12px;
  width: 90%;
  max-width: 450px;
  color: white;
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
  overflow: hidden;
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px 30px;
  border-bottom: 1px solid #333;
}

.modal-header h2 {
  margin: 0;
  color: #fff;
  font-size: 1.5rem;
}

.close-button {
  background: none;
  border: none;
  color: #b3b3b3;
  font-size: 2rem;
  cursor: pointer;
  padding: 0;
  line-height: 1;
}

.close-button:hover {
  color: white;
}

.modal-body {
  padding: 30px;
}

.modal-body p {
  margin-bottom: 20px;
  color: #b3b3b3;
  line-height: 1.5;
}

.delete-form {
  margin-top: 20px;
}

.form-group {
  margin-bottom: 25px;
}

.form-group label {
  display: block;
  margin-bottom: 8px;
  color: #fff;
  font-weight: 500;
  font-size: 0.9rem;
}

.form-input {
  width: 100%;
  padding: 12px 15px;
  background: #333;
  border: 1px solid #444;
  border-radius: 4px;
  color: white;
  font-size: 0.95rem;
}

.form-input:focus {
  outline: none;
  border-color: #1db954;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 15px;
  margin-top: 25px;
}

.btn-cancel {
  padding: 12px 25px;
  background: transparent;
  border: 1px solid #666;
  border-radius: 25px;
  color: #fff;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-cancel:hover {
  border-color: #fff;
  transform: scale(1.05);
}

.btn-delete {
  padding: 12px 25px;
  background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
  border: none;
  border-radius: 25px;
  color: white;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-delete:hover:not(:disabled) {
  transform: scale(1.05);
  box-shadow: 0 4px 15px rgba(231, 76, 60, 0.4);
}

.btn-delete:disabled {
  opacity: 0.6;
  cursor: not-allowed;
} 
</style>