import { reactive } from 'vue'

export const UIState = reactive({
  // UI
  showAuthModal: false,
  showChangePasswordModal: false,
  showDeleteAccountModal: false,

  // sesión
  isAuthenticated: false,
  currentUser: null,
})
