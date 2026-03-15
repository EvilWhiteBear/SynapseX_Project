// SynapseX Service Worker v1.0
// Кэширует статику для быстрого запуска

const CACHE_NAME = 'synapsex-v1';
const STATIC_ASSETS = [
  '/',
  '/terminal',
  '/static/css/style.css',
  '/static/img/logo.png',
  '/static/img/icon-192.png',
  '/static/img/icon-512.png',
  '/manifest.json',
];

// Установка — кэшируем статику
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(STATIC_ASSETS).catch(err => {
        console.log('[SW] Cache addAll error (ok):', err);
      });
    })
  );
  self.skipWaiting();
});

// Активация — удаляем старые кэши
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// Fetch — сеть первая, кэш как fallback
self.addEventListener('fetch', event => {
  // Только GET запросы
  if (event.request.method !== 'GET') return;
  
  // API запросы — только сеть (без кэша)
  if (event.request.url.includes('/api/') || 
      event.request.url.includes('/auth/') ||
      event.request.url.includes('/ask_ai')) {
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Кэшируем успешные ответы на статику
        if (response.ok && event.request.url.includes('/static/')) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => {
        // Офлайн — отдаём из кэша
        return caches.match(event.request).then(cached => {
          if (cached) return cached;
          // Если терминал офлайн — показываем главную из кэша
          if (event.request.url.includes('/terminal')) {
            return caches.match('/');
          }
        });
      })
  );
});