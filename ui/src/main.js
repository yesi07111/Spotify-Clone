import { createApp } from 'vue'
import App from './App.vue'
import router from './routing'
import store from './store'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap'
import '@fortawesome/fontawesome-free/css/all.min.css'

import '@/assets/styles/main.css'
import '@/assets/styles/components.css'
import '@/assets/styles/variables.css'

const app = createApp(App)
app.use(router)
app.use(store)
app.mount('#app')