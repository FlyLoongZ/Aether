import { bootstrapTheme } from './core'
import { installStartupFallback } from '../startup/fallback'

// 编译为 head 中的同步内联脚本：不依赖 Vue、body、网络或外部资源。
bootstrapTheme(window, document.documentElement)
installStartupFallback(window, document)
