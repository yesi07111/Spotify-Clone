<!-- src/components/auth/UserProfile.vue -->
<template>
  <div class="user-profile-dropdown" v-if="isAuthenticated">
    <div class="user-profile-trigger" @click="toggleDropdown">
      <div class="user-avatar">
        <i class="fas fa-user"></i>
      </div>
      <span class="username">{{ currentUser.username }}</span>
      <i class="fas fa-chevron-down dropdown-icon"></i>
    </div>
    
    <div v-if="showDropdown" class="user-profile-menu">
      <div class="user-profile-header">
        <div class="user-profile-avatar">
          <i class="fas fa-user"></i>
        </div>
        <div class="user-profile-info">
          <h4>{{ currentUser.username }}</h4>
          <p v-if="currentUser.email">{{ currentUser.email }}</p>
          <p class="user-id">ID: {{ currentUser.id }}</p>
        </div>
      </div>
      
      <div class="user-profile-actions">
        <button class="profile-action" @click="openChangePassword">
          <i class="fas fa-key"></i>
          <span>Cambiar contraseña</span>
        </button>
        
        <button class="profile-action" @click="openDeleteAccount">
          <i class="fas fa-trash-alt"></i>
          <span>Eliminar cuenta</span>
        </button>
        
        <hr class="divider">
        
        <button class="profile-action logout" @click="handleLogout">
          <i class="fas fa-sign-out-alt"></i>
          <span>Cerrar sesión</span>
        </button>
      </div>
    </div>
  </div>
</template>

<script>
import AuthService from '@/services/AuthService'
import { UIState } from '@/store/ui'


export default {
  name: 'UserProfile',

  data() {
    return {
      showDropdown: false
    }
  },

  computed: {
    isAuthenticated() {
      return UIState.isAuthenticated
    },

    currentUser() {
      return UIState.currentUser || {}
    }
  },

  methods: {
    toggleDropdown() {
      this.showDropdown = !this.showDropdown
    },

    openChangePassword() {
      this.showDropdown = false
      UIState.currentUser = this.currentUser
      UIState.showChangePasswordModal = true
    },

    openDeleteAccount() {
      this.showDropdown = false
      UIState.currentUser = this.currentUser
      UIState.showDeleteAccountModal = true
    },

    async handleLogout() {
    try {
      await AuthService.logout()
      
      // ✅ Actualizar UIState para que la UI reaccione
      UIState.isAuthenticated = false
      UIState.currentUser = null
      
      this.showDropdown = false
      
      console.log('✅ Logout exitoso, UI actualizada')
    } catch (error) {
      console.error('❌ Error en logout:', error)
      // Igual limpiamos el estado local
      UIState.isAuthenticated = false
      UIState.currentUser = null
      this.showDropdown = false
    }
  }
  }
}
</script>


<style scoped>
.user-profile-dropdown {
  position: relative;
  display: inline-block;
}

.user-profile-trigger {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 16px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 50px;
  cursor: pointer;
  transition: all 0.2s;
  border: 1px solid transparent;
}

.user-profile-trigger:hover {
  background: rgba(255, 255, 255, 0.15);
  border-color: #555;
}

.user-avatar {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--color-primary) 0%, #1ed760 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  font-size: 0.9rem;
}

.username {
  color: white;
  font-weight: 500;
  font-size: 0.95rem;
}

.dropdown-icon {
  color: #999;
  font-size: 0.8rem;
  transition: transform 0.2s;
}

.user-profile-dropdown:hover .dropdown-icon {
  transform: rotate(180deg);
}

.user-profile-menu {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  background: #282828;
  border-radius: 12px;
  min-width: 280px;
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
  border: 1px solid #404040;
  z-index: 1000;
  overflow: hidden;
}

.user-profile-header {
  padding: 20px;
  background: #1f1f1f;
  border-bottom: 1px solid #404040;
  display: flex;
  align-items: center;
  gap: 15px;
}

.user-profile-avatar {
  width: 50px;
  height: 50px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--color-primary) 0%, #1ed760 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  font-size: 1.2rem;
}

.user-profile-info h4 {
  margin: 0 0 5px 0;
  color: white;
  font-size: 1rem;
}

.user-profile-info p {
  margin: 0;
  color: #999;
  font-size: 0.85rem;
}

.user-id {
  font-size: 0.75rem;
  color: #666;
  margin-top: 5px;
}

.user-profile-actions {
  padding: 10px 0;
}

.profile-action {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
  padding: 12px 20px;
  background: none;
  border: none;
  color: #ccc;
  text-align: left;
  cursor: pointer;
  transition: all 0.2s;
  font-size: 0.9rem;
}

.profile-action:hover {
  background: rgba(255, 255, 255, 0.05);
  color: white;
}

.profile-action i {
  width: 20px;
  color: #999;
}

.profile-action.logout {
  color: #ff6b6b;
}

.profile-action.logout:hover {
  background: rgba(255, 107, 107, 0.1);
}

.profile-action.logout i {
  color: #ff6b6b;
}

.divider {
  border: none;
  height: 1px;
  background: #404040;
  margin: 10px 0;
}
</style>