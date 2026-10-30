import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'
import { execSync } from 'child_process'
import { legacyTemplateI18nPlugin } from './src/i18n/legacy-template-transform'

function normalizeVersion(version: string): string {
  const trimmed = version.trim()
  // Historical standalone-client tags are not gateway application versions.
  if (!trimmed || trimmed.startsWith('tunnel-v')) {
    return ''
  }
  return trimmed.startsWith('v') || trimmed.startsWith('V') ? trimmed.slice(1) : trimmed
}

function getGitVersion(): string {
  const envVersion = process.env.AETHER_BUILD_VERSION || process.env.AETHER_VERSION
  if (envVersion?.trim()) {
    const version = normalizeVersion(envVersion)
    if (version) {
      return version
    }
  }

  try {
    const version = normalizeVersion(execSync('git describe --tags --match "v[0-9]*" --always --dirty').toString())
    return version || '0.0.0.dev0'
  } catch {
    return '0.0.0.dev0'
  }
}

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const rootEnv = loadEnv(mode, path.resolve(__dirname, '..'), '')
  const appPort = rootEnv.APP_PORT || process.env.APP_PORT || '8084'
  const gatewayTarget = `http://127.0.0.1:${appPort}`

  return {
    // GitHub Pages 部署时使用仓库名作为 base
    base: process.env.GITHUB_PAGES === 'true' ? '/Aether/' : '/',
    plugins: [legacyTemplateI18nPlugin(), vue()],
    define: {
      __APP_VERSION__: JSON.stringify(getGitVersion()),
    },
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    build: {
      minify: true,
      rolldownOptions: {
        output: {
          minify: mode === 'production'
            ? { compress: { dropConsole: true, dropDebugger: true } }
            : false,
          manualChunks(id: string) {
            if (!id.includes('node_modules')) return undefined
            if (id.includes('chart.js')) return 'chart-vendor'
            if (id.includes('radix-vue') || id.includes('@lucide/vue')) return 'ui-vendor'
            if (id.includes('axios') || id.includes('marked') || id.includes('dompurify')) return 'utils-vendor'
            if (id.includes('/vue/') || id.includes('vue-router') || id.includes('/pinia/')) return 'vue-vendor'
            return undefined
          },
        },
      },
    },
    server: {
      port: 5173,
      proxy: {
        // 只代理真正的 API 路径；目标端口由根目录 APP_PORT 控制
        '/api/': {
          target: gatewayTarget,
          changeOrigin: true,
          secure: false,
          ws: true,
        },
        '/v1/': {
          target: gatewayTarget,
          changeOrigin: true,
          secure: false,
        },
        '/health': {
          target: gatewayTarget,
          changeOrigin: true,
          secure: false,
        },
        '/_gateway/': {
          target: gatewayTarget,
          changeOrigin: true,
          secure: false,
        },
      },
    },
    preview: {
      port: 5173,
    },
  }
})
