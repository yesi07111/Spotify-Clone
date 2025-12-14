<!-- src/components/layout/NavigationBar.vue -->
<template>
  <nav class="navigation-bar">
    <div class="navigation-brand">
      <i class="fab fa-spotify navigation-brand-icon"></i>
      <span>Spotify Clone</span>
    </div>
    
    <div class="navigation-auth">
      <!-- Perfil de usuario si está autenticado -->
      <UserProfile
        v-if="isAuthenticated"
        @logout-success="handleLogoutSuccess"
        @change-password="openChangePassword"
        @delete-account="openDeleteAccount"
      />
      
      <!-- Botón de login si no está autenticado -->
      <button 
        v-else 
        class="btn-login"
        @click="openAuthModal"
      >
        <i class="fas fa-sign-in-alt"></i>
        <span>Iniciar Sesión</span>
      </button>
    </div>
    
    <!-- Modal de Autenticación -->
    <AuthModal
      :is-visible="showAuthModal"
      @close="closeAuthModal"
      @success="handleAuthSuccess"
    />
    
    <!-- Modal para cambiar contraseña -->
    <ChangePasswordModal
      v-if="showChangePasswordModal"
      @close="closeChangePasswordModal"
      @success="handlePasswordChangeSuccess"
    />
    
    <!-- Modal para eliminar cuenta -->
    <DeleteAccountModal
      v-if="showDeleteAccountModal"
      @close="closeDeleteAccountModal"
      @success="handleAccountDeleteSuccess"
    />
  </nav>
</template>

<script>
import AuthService from '@/services/AuthService'
import AuthModal from '@/components/auth/AuthModal.vue'
import UserProfile from '@/components/auth/UserProfile.vue'
import ChangePasswordModal from '@/components/auth/ChangePasswordModal.vue'
import DeleteAccountModal from '@/components/auth/DeleteAccountModal.vue'

export default {
  name: 'NavigationBar',
  components: {
    AuthModal,
    UserProfile,
    ChangePasswordModal,
    DeleteAccountModal
  },
  data() {
    return {
      showAuthModal: false,
      showChangePasswordModal: false,
      showDeleteAccountModal: false
    }
  },
  computed: {
    isAuthenticated() {
      return AuthService.isAuthenticated()
    }
  },
  methods: {
    openAuthModal() {
      this.showAuthModal = true
    },
    
    closeAuthModal() {
      this.showAuthModal = false
    },
    
    openChangePassword() {
      this.showChangePasswordModal = true
    },
    
    closeChangePasswordModal() {
      this.showChangePasswordModal = false
    },
    
    openDeleteAccount() {
      this.showDeleteAccountModal = true
    },
    
    closeDeleteAccountModal() {
      this.showDeleteAccountModal = false
    },
    
    handleAuthSuccess(message) {
      console.log(message)
      this.showAuthModal = false
    },
    
    handleLogoutSuccess() {
      console.log('Sesión cerrada exitosamente')
    },
    
    handlePasswordChangeSuccess(message) {
      alert(message)
      this.showChangePasswordModal = false
    },
    
    handleAccountDeleteSuccess(message) {
      alert(message)
      this.showDeleteAccountModal = false
      window.location.reload()
    }
  }
}
</script>

<style scoped>
.navigation-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 15px 30px;
  background: linear-gradient(90deg, #121212 0%, #212121 100%);
  border-bottom: 1px solid #333;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
}

.navigation-brand {
  display: flex;
  align-items: center;
  gap: 10px;
  color: white;
  font-size: 1.5rem;
  font-weight: 700;
}

.navigation-brand-icon {
  color: #1db954;
  font-size: 2rem;
}

.navigation-auth {
  display: flex;
  align-items: center;
}

.btn-login {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  background: linear-gradient(135deg, #1db954 0%, #1ed760 100%);
  color: white;
  border: none;
  border-radius: 50px;
  font-weight: 600;
  font-size: 0.95rem;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-login:hover {
  transform: scale(1.05);
  box-shadow: 0 4px 15px rgba(29, 185, 84, 0.4);
}

.btn-login i {
  font-size: 0.9rem;
}
</style>