/**
 * 公告类型相关工具函数
 */
import { AlertCircle, AlertTriangle, Wrench, Info, type LucideIcon } from '@lucide/vue'

export type AnnouncementType = 'important' | 'warning' | 'maintenance' | 'info'

interface AnnouncementTypeConfig {
  icon: LucideIcon
  iconColor: string
  label: string
  bgColor: string
  borderColor: string
  textColor: string
}

const announcementTypeConfigs: Record<AnnouncementType, AnnouncementTypeConfig> = {
  important: {
    icon: AlertCircle,
    iconColor: 'text-rose-600 dark:text-rose-400',
    label: '重要公告',
    bgColor: 'bg-rose-50 dark:bg-rose-950/30',
    borderColor: 'border-rose-200 dark:border-rose-800',
    textColor: 'text-rose-800 dark:text-rose-200'
  },
  warning: {
    icon: AlertTriangle,
    iconColor: 'text-amber-600 dark:text-amber-400',
    label: '警告通知',
    bgColor: 'bg-amber-50 dark:bg-amber-950/30',
    borderColor: 'border-amber-200 dark:border-amber-800',
    textColor: 'text-amber-800 dark:text-amber-200'
  },
  maintenance: {
    icon: Wrench,
    iconColor: 'text-orange-600 dark:text-orange-400',
    label: '维护通知',
    bgColor: 'bg-orange-50 dark:bg-orange-950/30',
    borderColor: 'border-orange-200 dark:border-orange-800',
    textColor: 'text-orange-800 dark:text-orange-200'
  },
  info: {
    icon: Info,
    iconColor: 'text-primary dark:text-primary',
    label: '系统公告',
    bgColor: 'bg-primary/5',
    borderColor: 'border-primary/20',
    textColor: 'text-foreground'
  }
}

/**
 * 获取公告类型配置
 */
export function getAnnouncementConfig(type: string): AnnouncementTypeConfig {
  return announcementTypeConfigs[type as AnnouncementType] || announcementTypeConfigs.info
}

/**
 * 获取公告图标组件
 */
export function getAnnouncementIcon(type: string): LucideIcon {
  return getAnnouncementConfig(type).icon
}

/**
 * 获取公告图标颜色
 */
export function getAnnouncementIconColor(type: string): string {
  return getAnnouncementConfig(type).iconColor
}

/**
 * 获取公告类型标签
 */
export function getAnnouncementTypeLabel(type: string): string {
  return getAnnouncementConfig(type).label
}

/**
 * 获取公告背景颜色
 */
export function getAnnouncementBgColor(type: string): string {
  return getAnnouncementConfig(type).bgColor
}

/**
 * 获取公告边框颜色
 */
export function getAnnouncementBorderColor(type: string): string {
  return getAnnouncementConfig(type).borderColor
}

/**
 * 获取公告文字颜色
 */
export function getAnnouncementTextColor(type: string): string {
  return getAnnouncementConfig(type).textColor
}

/**
 * 将 Markdown 内容转换为纯文本摘要
 */
export function getPlainTextSummary(content: string, maxLength = 120): string {
  const cleaned = content
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`[^`]*`/g, ' ')
    .replace(/!\[[^\]]*]\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*]\(([^)]*)\)/g, '$1')
    .replace(/[#>*_~]/g, '')
    .replace(/\n+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  if (cleaned.length <= maxLength) {
    return cleaned
  }

  return `${cleaned.slice(0, maxLength).trim()}...`
}
