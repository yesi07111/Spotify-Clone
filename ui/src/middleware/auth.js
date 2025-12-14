// middleware/auth.js
import AuthService from '@/services/AuthService'

export const authGuard = (to, from, next) => {
    const requiresAuth = to.matched.some(record => record.meta.requiresAuth)
    const isAuthenticated = AuthService.isAuthenticated()
    
    if (requiresAuth && !isAuthenticated) {
        next({
            path: '/',
            query: { redirect: to.fullPath }
        })
    } else if (!requiresAuth && isAuthenticated && (to.path === '/login' || to.path === '/register')) {
        next('/')
    } else {
        next()
    }
}

export const guestGuard = (to, from, next) => {
    if (AuthService.isAuthenticated()) {
        next('/')
    } else {
        next()
    }
}