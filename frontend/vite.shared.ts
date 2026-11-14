import path from 'path'

/**
 * 构建与测试共享的解析配置。
 * vite.config.ts 与 vitest.config.ts 此前各写一份 alias，路径调整时容易漏改。
 */
export const sharedResolve = {
  alias: {
    '@': path.resolve(import.meta.dirname, './src'),
  },
}
