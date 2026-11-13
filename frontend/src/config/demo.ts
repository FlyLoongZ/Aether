import { translate } from '@/i18n'
/**
 * Demo Mode Configuration
 * 用于 GitHub Pages 等静态托管环境的演示模式
 */

// 检测是否为演示模式环境
export function isDemoMode(): boolean {
  const hostname = window.location.hostname
  return (
    hostname.includes('github.io') ||
    hostname.includes('vercel.app') ||
    hostname.includes('netlify.app') ||
    hostname.includes('pages.dev') ||
    import.meta.env.VITE_DEMO_MODE === 'true'
  )
}

// Demo 账号配置
export const DEMO_ACCOUNTS = {
  admin: {
    email: 'admin@demo.aether.io',
    password: 'demo123',
    hint: '管理员账号'
  },
  user: {
    email: 'user@demo.aether.io',
    password: 'demo123',
    get hint() { return translate('ui.user') }
  }
} as const
