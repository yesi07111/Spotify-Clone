<!-- AuthRequiredMessage.vue -->
<template>
  <div class="auth-message-backdrop" @click.self="close">
    <div class="auth-message-card">
      <!-- Header -->
      <div class="header">
        <div class="icon-wrapper">
          <i class="fas fa-lock"></i>
        </div>
        <h5 class="title">{{ title }}</h5>
      </div>

      <!-- Mensaje -->
      <p class="message">
        {{ message }}
      </p>

      <!-- Razones -->
      <ul v-if="reasons?.length" class="reasons">
        <li v-for="(reason, index) in reasons" :key="index">
          <i class="fas fa-info-circle"></i>
          <span>{{ reason }}</span>
        </li>
      </ul>

      <!-- Acciones -->
      <div class="actions">
        <!-- <button class="btn btn-outline-light" @click="close">
          Más tarde
        </button> -->

        <button class="btn btn-primary" @click="close">
          <i class="fas fa-sign-in-alt me-2"></i>
          Aceptar
        </button>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'AuthRequiredMessage',
  props: {
    title: {
      type: String,
      default: 'Inicia sesión para continuar'
    },
    message: {
      type: String,
      default: 'Esta acción requiere que tengas una cuenta activa.'
    },
    reasons: {
      type: Array,
      default: () => []
    }
  },
  emits: ['close', 'login'],
  methods: {
    close() {
      this.$emit('close')
    },
  }
}
</script>

<style scoped>
.auth-message-backdrop {
  position: fixed;
  inset: 0;
  background: radial-gradient(
    circle at center,
    rgba(0, 0, 0, 0.55),
    rgba(0, 0, 0, 0.85)
  );
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
}

.auth-message-card {
  background: linear-gradient(180deg, #1f1f1f, #161616);
  color: #fff;
  border-radius: 16px;
  padding: 28px;
  width: 100%;
  max-width: 440px;
  border: 1px solid rgba(13, 110, 253, 0.35);
  box-shadow:
    0 20px 40px rgba(0, 0, 0, 0.7),
    inset 0 0 0 1px rgba(255, 255, 255, 0.02);
  animation: fadeUp 0.25s ease-out;
}

/* Header */
.header {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 16px;
}

.icon-wrapper {
  width: 42px;
  height: 42px;
  border-radius: 50%;
  background: rgba(13, 110, 253, 0.15);
  display: flex;
  align-items: center;
  justify-content: center;
}

.icon-wrapper i {
  font-size: 1.2rem;
  color: #0d6efd;
}

.title {
  margin: 0;
  font-weight: 600;
}

/* Mensaje */
.message {
  color: #d1d1d1;
  margin-bottom: 16px;
  line-height: 1.4;
}

/* Razones */
.reasons {
  list-style: none;
  padding: 12px;
  margin: 0 0 20px;
  background: rgba(255, 255, 255, 0.03);
  border-radius: 10px;
}

.reasons li {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  margin-bottom: 8px;
  font-size: 0.9rem;
  color: #e0e0e0;
}

.reasons li:last-child {
  margin-bottom: 0;
}

.reasons i {
  margin-top: 2px;
  color: #0dcaf0;
}

/* Acciones */
.actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

/* Animación */
@keyframes fadeUp {
  from {
    opacity: 0;
    transform: translateY(12px) scale(0.98);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}
</style>
