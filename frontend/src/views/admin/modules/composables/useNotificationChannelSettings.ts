import { computed, onMounted, ref } from 'vue'
import { adminApi } from '@/api/admin'
import { modulesApi } from '@/api/modules'
import { useToast } from '@/composables/useToast'
import { useI18n, type MessageKey } from '@/i18n'
import { parseApiError } from '@/utils/errorParser'
import { log } from '@/utils/logger'

type SystemConfigMap = Map<string, { value?: unknown; is_set?: boolean }>

export interface NotificationChannelSettingsOptions {
  /** 模块标识，例如 bark_push / server_chan_push */
  moduleId: string
  /** 测试通知使用的渠道标识 */
  channel: 'email' | 'all' | 'server_chan' | 'bark'
  /** 密钥配置 key（写入后只显示"已设置"） */
  secretKey: string
  /** 模板配置 key */
  templateKey: string
  /** 密钥与模板写入 system config 时的说明 */
  secretRemark: string
  templateRemark: string
  /** 日志前缀，例如 "Bark 推送" */
  logLabel: string
  /** 各环节的提示文案：传消息 key，由本 composable 在使用点求值，随语言切换更新 */
  messages: {
    loadFailed: MessageKey
    saveFailed: MessageKey
    saved: MessageKey
    testFailed: MessageKey
  }
  /** 读取配置后由页面接管自己的额外字段（如 Bark 服务器地址） */
  applyConfigs?: (configsByKey: SystemConfigMap) => void
  /** 保存时追加页面自己的配置项；用 add 注册即可 */
  collectExtraUpdates?: (add: (key: string, value: string, remark: string) => void) => void
}

export interface NotificationTestResultItem {
  channel: string
  success: boolean
  message: string
}

/**
 * 通知渠道设置页（Bark / Server 酱）共用的加载、保存、测试流程。
 * 两个页面此前逐段复制同一套实现，改动时容易只改一处。
 */
export function useNotificationChannelSettings(options: NotificationChannelSettingsOptions) {
  const { t } = useI18n()
  const { success, error } = useToast()

  const saving = ref(false)
  const testing = ref(false)
  const enabled = ref(false)
  const secretIsSet = ref(false)
  const secretInput = ref('')
  const templateInput = ref('')
  const lastTestResult = ref<NotificationTestResultItem[]>([])

  const canEnable = computed(() => secretIsSet.value || secretInput.value.trim() !== '')

  onMounted(() => {
    void loadConfig()
  })

  async function loadConfig(): Promise<void> {
    try {
      const [moduleStatus, configs] = await Promise.all([
        modulesApi.getStatus(options.moduleId),
        adminApi.getAllSystemConfigs({ cacheTtlMs: 30_000 }),
      ])
      const configsByKey: SystemConfigMap = new Map(configs.map(config => [config.key, config]))
      const secret = configsByKey.get(options.secretKey)
      const template = configsByKey.get(options.templateKey)

      enabled.value = moduleStatus.enabled === true
      secretIsSet.value = secret?.is_set === true
      secretInput.value = ''
      templateInput.value = typeof template?.value === 'string' ? template.value : ''
      options.applyConfigs?.(configsByKey)
    } catch (err) {
      error(parseApiError(err, t(options.messages.loadFailed)))
      log.error(`加载 ${options.logLabel}配置失败:`, err)
    }
  }

  async function saveConfig(): Promise<void> {
    saving.value = true
    try {
      const updates: Array<Promise<unknown>> = [
        adminApi.updateSystemConfig(options.templateKey, templateInput.value, options.templateRemark),
      ]
      options.collectExtraUpdates?.((key, value, remark) => {
        updates.push(adminApi.updateSystemConfig(key, value, remark))
      })
      const trimmedSecret = secretInput.value.trim()
      if (trimmedSecret) {
        updates.push(adminApi.updateSystemConfig(options.secretKey, trimmedSecret, options.secretRemark))
      }
      await Promise.all(updates)
      if (trimmedSecret) {
        secretIsSet.value = true
        secretInput.value = ''
      }
      if (!canEnable.value) {
        enabled.value = false
      }
      await modulesApi.setEnabled(options.moduleId, enabled.value)
      success(t(options.messages.saved))
    } catch (err) {
      error(parseApiError(err, t(options.messages.saveFailed)))
      log.error(`保存 ${options.logLabel}配置失败:`, err)
    } finally {
      saving.value = false
    }
  }

  async function handleTest(): Promise<void> {
    testing.value = true
    try {
      const result = await adminApi.testImportantNotification({ channel: options.channel })
      lastTestResult.value = result.channels || []
      if (result.success) {
        success(result.message || t('ui.testNotificationSent'))
      } else {
        error(result.message || t('ui.failedToSendTestNotification'))
      }
    } catch (err) {
      error(parseApiError(err, t(options.messages.testFailed)))
      log.error(`测试 ${options.logLabel}失败:`, err)
    } finally {
      testing.value = false
    }
  }

  return {
    saving,
    testing,
    enabled,
    secretIsSet,
    secretInput,
    templateInput,
    lastTestResult,
    canEnable,
    loadConfig,
    saveConfig,
    handleTest,
  }
}
