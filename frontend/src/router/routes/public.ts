import type { RouteRecordRaw } from 'vue-router'
import { view } from './helpers'

export const publicRoutes: RouteRecordRaw[] = [
  {
    path: '/status',
    name: 'PublicStatus',
    component: view(() => import('@/views/public/Status.vue')),
    meta: { requiresAuth: false, authBootstrap: 'skip' }
  },
  {
    path: '/',
    name: 'Home',
    component: view(() => import('@/views/public/Home.vue')),
    meta: { requiresAuth: false, authBootstrap: 'home' }
  },
  {
    path: '/register',
    name: 'RegisterEntry',
    component: view(() => import('@/views/public/Home.vue')),
    meta: { requiresAuth: false, authBootstrap: 'background' }
  },
  {
    path: '/privacy-policy',
    name: 'PrivacyPolicy',
    component: view(() => import('@/views/public/PrivacyPolicy.vue')),
    meta: { requiresAuth: false, authBootstrap: 'skip' }
  },
  {
    path: '/guide',
    component: view(() => import('@/views/public/guide/GuideLayout.vue')),
    meta: { requiresAuth: false, authBootstrap: 'skip' },
    children: [
      {
        path: '',
        name: 'GuideOverview',
        component: view(() => import('@/views/public/guide/Overview.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'architecture',
        name: 'GuideArchitecture',
        component: view(() => import('@/views/public/guide/ArchitectureGuide.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'concepts',
        name: 'GuideConcepts',
        component: view(() => import('@/views/public/guide/ConceptsGuide.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'strategy',
        name: 'GuideStrategy',
        component: view(() => import('@/views/public/guide/StrategyGuide.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'advanced',
        name: 'GuideAdvanced',
        component: view(() => import('@/views/public/guide/AdvancedGuide.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'faq',
        name: 'GuideFaq',
        component: view(() => import('@/views/public/guide/GuideFaq.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      },
      {
        path: 'modules',
        name: 'GuideModules',
        component: view(() => import('@/views/public/guide/ModulesGuide.vue')),
        meta: { requiresAuth: false, authBootstrap: 'skip' }
      }
    ]
  },
  {
    path: '/logo-demo',
    name: 'LogoColorDemo',
    component: view(() => import('@/views/public/LogoColorDemo.vue')),
    meta: { requiresAuth: false, authBootstrap: 'skip' }
  },
  {
    path: '/auth/callback',
    name: 'AuthCallback',
    component: view(() => import('@/views/public/AuthCallback.vue')),
    meta: { requiresAuth: false, authBootstrap: 'owned' }
  }
]
