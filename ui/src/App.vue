<!-- src/App.vue -->
<template>
  <div id="app">
    <NavigationBar />
    <router-view />

    <!-- MODALES CENTRALIZADOS -->
    <AuthModal
      :isVisible="UIState.showAuthModal"
      @close="UIState.showAuthModal = false"
      @success="onAuthSuccess"
    />

    <ChangePasswordModal
      v-if="UIState.showChangePasswordModal"
      :user="UIState.currentUser"
      @close="UIState.showChangePasswordModal = false"
      @success="onPasswordChangeSuccess"
    />

    <DeleteAccountModal
      :isVisible="UIState.showDeleteAccountModal"
      :user="UIState.currentUser"
      @close="UIState.showDeleteAccountModal = false"
      @success="onAccountDeleteSuccess"
    />
  </div>
</template>

<script>
import { UIState } from '@/store/ui'
import NavigationBar from '@/components/layout/NavigationBar.vue'
import AuthModal from '@/components/auth/AuthModal.vue'
import ChangePasswordModal from '@/components/auth/ChangePasswordModal.vue'
import DeleteAccountModal from '@/components/auth/DeleteAccountModal.vue'

export default {
  name: 'App',
  components: {
    NavigationBar,
    AuthModal,
    ChangePasswordModal,
    DeleteAccountModal
  },
  data() {
    return {
      UIState
    }
  },
  methods: {
    onAuthSuccess(message) {
      console.log('✅ [APP] Auth exitosa:', message)
      UIState.showAuthModal = false
      // cualquier otra lógica post-login
    },
    onPasswordChangeSuccess(msg) {
      alert(msg)
      UIState.showChangePasswordModal = false
    },
    onAccountDeleteSuccess(msg) {
      alert(msg)
      UIState.showDeleteAccountModal = false
    }
  }

}
</script>
