// proxy-server.js
const http = require('http');
const httpProxy = require('http-proxy');
const dns = require('dns');
const crypto = require('crypto');

// Configuración
const PROXY_PORT = 3000;
const BACKEND_ALIAS = 'spotify_cluster'; // Alias DNS a buscar
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

// Crear proxy
const proxy = httpProxy.createProxyServer({});

// Almacenar backends descubiertos y su estado
let discoveredBackends = [];
let currentIndex = 0; // Para round-robin

// Función para descubrir backends por alias DNS
async function discoverBackendsByAlias(alias, retries = RETRY_ATTEMPTS) {
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            console.log(`[Attempt ${attempt + 1}/${retries}] Discovering backends for alias: ${alias}`);
            
            // Resolver el alias DNS
            const addresses = await dns.promises.resolve4(alias);
            
            // Filtrar IPs válidas (no localhost)
            const validIPs = addresses.filter(ip => 
                ip && ip !== '127.0.0.1' && ip !== '::1'
            );
            
            console.log(`Found ${validIPs.length} backend(s):`, validIPs);
            
            // Mapear a objetos backend
            const backends = validIPs.map(ip => ({
                ip: ip,
                hostname: ip,
                url: `http://${ip}`,
                healthy: true,
                lastChecked: Date.now()
            }));
            
            discoveredBackends = backends;
            return backends;
            
        } catch (error) {
            console.error(`[Attempt ${attempt + 1}/${retries}] DNS resolution failed for ${alias}:`, error.message);
            
            if (attempt === retries - 1) {
                console.error(`All ${retries} attempts failed for alias: ${alias}`);
                discoveredBackends = [];
                return [];
            }
            
            // Esperar antes de reintentar
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
        }
    }
    
    return [];
}

// Función para verificar salud de un backend
async function checkBackendHealth(backend) {
    return new Promise((resolve) => {
        const timeout = 3000; // 3 segundos timeout
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        
        fetch(`${backend.url}/health`, { 
            signal: controller.signal,
            method: 'HEAD'
        })
        .then(response => {
            clearTimeout(timeoutId);
            backend.healthy = response.ok;
            backend.lastChecked = Date.now();
            resolve(response.ok);
        })
        .catch(error => {
            clearTimeout(timeoutId);
            console.log(`Backend ${backend.ip} is unhealthy: ${error.message}`);
            backend.healthy = false;
            backend.lastChecked = Date.now();
            resolve(false);
        });
    });
}

// Seleccionar backend (estrategia round-robin con salud)
function selectBackend() {
    if (discoveredBackends.length === 0) {
        return null;
    }
    
    const healthyBackends = discoveredBackends.filter(b => b.healthy);
    if (healthyBackends.length === 0) {
        // Si ningún backend está saludable, intentamos con todos
        console.warn('No healthy backends found, trying all');
        const backend = discoveredBackends[currentIndex % discoveredBackends.length];
        currentIndex++;
        return backend;
    }
    
    // Round-robin entre backends saludables
    const backend = healthyBackends[currentIndex % healthyBackends.length];
    currentIndex++;
    return backend;
}

// Servidor HTTP proxy
const server = http.createServer(async (req, res) => {
    // Registrar la petición entrante
    const requestId = crypto.randomBytes(4).toString('hex');
    console.log(`\n[${requestId}] ${req.method} ${req.url} from ${req.socket.remoteAddress}`);
    console.log(`[${requestId}] Headers:`, req.headers);
    
    // Si no hay backends descubiertos, intentar descubrir
    if (discoveredBackends.length === 0) {
        console.log(`[${requestId}] No backends discovered, attempting discovery...`);
        await discoverBackendsByAlias(BACKEND_ALIAS);
    }
    
    // Seleccionar backend
    const backend = selectBackend();
    
    if (!backend) {
        console.error(`[${requestId}] No backends available`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            error: 'No backends available',
            message: 'Could not find any backend servers'
        }));
        return;
    }
    
    console.log(`[${requestId}] Forwarding to backend: ${backend.ip}`);
    
    // Configurar opciones del proxy
    const proxyOptions = {
        target: `${backend.url}:8000`,
        changeOrigin: true, // Cambiar el header Host al del target
        xfwd: true, // Añadir headers X-Forwarded-*
        headers: {
            'X-Forwarded-For': req.socket.remoteAddress,
            'X-Forwarded-Proto': req.socket.encrypted ? 'https' : 'http',
            'X-Request-ID': requestId
        }
    };
    
    // Manejar errores del proxy
    proxy.on('error', (err, req, res) => {
        console.error(`[${requestId}] Proxy error to ${backend.ip}:`, err.message);
        
        // Marcar backend como no saludable
        backend.healthy = false;
        
        // Intentar con otro backend si hay más
        if (discoveredBackends.filter(b => b.healthy).length > 0) {
            console.log(`[${requestId}] Retrying with another backend...`);
            // Podríamos implementar reintento aquí
        }
        
        if (!res.headersSent) {
            res.writeHead(502, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                error: 'Bad Gateway',
                message: `Failed to connect to backend: ${err.message}`,
                requestId: requestId
            }));
        }
    });
    
    // Proxy la petición
    proxy.web(req, res, proxyOptions, (err) => {
        if (err) {
            console.error(`[${requestId}] Proxy callback error:`, err.message);
        } else {
            console.log(`[${requestId}] Request successfully proxied to ${backend.ip}`);
        }
    });
});

// Eventos del proxy para logging
proxy.on('proxyReq', (proxyReq, req, res, options) => {
    console.log(`[${req.headers['x-request-id']}] Sending request to: ${options.target.href}`);
});

proxy.on('proxyRes', (proxyRes, req, res) => {
    console.log(`[${req.headers['x-request-id']}] Received response with status: ${proxyRes.statusCode}`);
});

// Iniciar el servidor
server.listen(PROXY_PORT, '0.0.0.0', () => {
    console.log(`✅ Proxy server running on http://0.0.0.0:${PROXY_PORT}`);
    console.log(`🔍 Backend alias: ${BACKEND_ALIAS}`);
    
    // Descubrir backends al iniciar
    discoverBackendsByAlias(BACKEND_ALIAS).then(backends => {
        if (backends.length > 0) {
            console.log(`✅ Discovered ${backends.length} backend(s) on startup`);
        } else {
            console.log('⚠️ No backends discovered on startup, will retry on first request');
        }
    });
    
    // Programar redescubrimiento periódico (cada 30 segundos)
    setInterval(() => {
        console.log('🔄 Periodic backend discovery...');
        discoverBackendsByAlias(BACKEND_ALIAS);
    }, 30000);
});

// Manejar shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down proxy server...');
    server.close(() => {
        console.log('✅ Proxy server closed');
        process.exit(0);
    });
});

// Endpoint de salud del proxy
const healthServer = http.createServer((req, res) => {
    if (req.url === '/health' && req.method === 'GET') {
        const healthyBackends = discoveredBackends.filter(b => b.healthy);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'healthy',
            proxy: 'running',
            backends: {
                total: discoveredBackends.length,
                healthy: healthyBackends.length,
                discovered: discoveredBackends.map(b => ({
                    ip: b.ip,
                    healthy: b.healthy,
                    lastChecked: b.lastChecked
                }))
            }
        }));
    } else {
        res.writeHead(404);
        res.end();
    }
});

healthServer.listen(3001, '0.0.0.0', () => {
    console.log(`✅ Health check server running on http://0.0.0.0:3001/health`);
});