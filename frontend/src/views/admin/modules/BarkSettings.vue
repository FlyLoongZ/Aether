<template>
  <PageContainer>
    <PageHeader
      :title="t('ui.barkNotifications')"
      :description="t('ui.thirdPartyPushServiceForTheBarkNotificationChannel')"
    />

    <div class="mt-6 space-y-6">
      <CardSection
        :title="t('ui.serviceSettings')"
        :description="t('ui.configureBarkDeviceKeyServerUrlAndServiceEnabledState')"
      >
        <template #actions>
          <Button
            size="sm"
            :disabled="saving"
            @click="saveConfig"
          >
            {{ saving ? t('ui.saving') : t('ui.save') }}
          </Button>
        </template>

        <div class="space-y-5">
          <div class="flex items-center justify-between gap-4 rounded-lg border border-border/70 px-4 py-3">
            <div>
              <Label class="text-sm font-medium"> {{ t('ui.enableBarkNotifications') }} </Label>
              <p class="mt-1 text-xs text-muted-foreground">
                {{ t('ui.barkMustBeEnabledBeforeTheNotificationServiceCanUse') }}
              </p>
            </div>
            <Switch
              v-model="enabled"
              :disabled="!canEnable"
            />
          </div>

          <div class="grid gap-4 lg:grid-cols-2">
            <div>
              <Label
                for="bark-device-key"
                class="block text-sm font-medium"
              >
                Device Key
              </Label>
              <Input
                id="bark-device-key"
                v-model="secretInput"
                masked
                :placeholder="secretIsSet ? t('ui.setLeaveEmptyToKeepUnchanged') : t('ui.findThisInYourBarkAppPushUrl')"
                class="mt-1"
              />
              <p class="mt-1 text-xs text-muted-foreground">
                {{ $t('modules.bark.deviceKeyHint') }}
              </p>
            </div>

            <div>
              <Label
                for="bark-server-url"
                class="block text-sm font-medium"
              > {{ t('ui.serverAddress') }} </Label>
              <Input
                id="bark-server-url"
                v-model="serverUrlInput"
                placeholder="https://api.day.app"
                class="mt-1"
              />
              <p class="mt-1 text-xs text-muted-foreground">
                {{ t('ui.supportsTheOfficialServiceOrASelfHostedBarkServer') }}
              </p>
            </div>
          </div>
        </div>
      </CardSection>

      <CardSection
        :title="t('ui.notificationTemplates')"
        :description="t('ui.templatesSupportValueAndValueVariables')"
      >
        <div>
          <Label
            for="bark-template"
            class="block text-sm font-medium"
          > {{ t('ui.templateContent') }} </Label>
          <Textarea
            id="bark-template"
            v-model="templateInput"
            rows="10"
            class="mt-1 font-mono text-sm"
            placeholder="{body}"
            spellcheck="false"
          />
        </div>
      </CardSection>

      <CardSection
        :title="t('ui.testService')"
        :description="t('ui.sendABarkTestNotificationUsingTheSavedSettings')"
      >
        <div class="flex flex-wrap gap-2">
          <Button
            variant="outline"
            :disabled="testing || !secretIsSet"
            @click="handleTest"
          >
            {{ testing ? t('auth.register.sendingCode') : t('ui.sendTest') }}
          </Button>
          <RouterLink
            to="/admin/notification-service"
            class="inline-flex h-11 items-center rounded-xl px-3 text-sm text-primary hover:underline"
          >
            {{ t('ui.openNotificationService') }}
          </RouterLink>
        </div>

        <div
          v-if="lastTestResult.length > 0"
          class="mt-4 space-y-2"
        >
          <div
            v-for="item in lastTestResult"
            :key="item.channel"
            class="flex items-center justify-between gap-4 rounded-md border border-border px-3 py-2 text-sm"
          >
            <span>{{ formatNotificationChannel(item.channel) }}</span>
            <span :class="item.success ? 'text-green-600 dark:text-green-400' : 'text-destructive'">
              {{ item.message }}
            </span>
          </div>
        </div>
      </CardSection>
    </div>
  </PageContainer>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { RouterLink } from 'vue-router'
import { Button, Input, Label, Switch, Textarea } from '@/components/ui'
import { PageHeader, PageContainer, CardSection } from '@/components/layout'
import { useI18n } from '@/i18n'
import { formatNotificationChannel } from './notificationChannel'
import { useNotificationChannelSettings } from './composables/useNotificationChannelSettings'

const { t } = useI18n()

const DEFAULT_SERVER_URL = 'https://api.day.app'

const CONFIG_KEYS = {
  enabled: 'module.bark_push.enabled',
  device_key: 'module.bark_push.device_key',
  server_url: 'module.bark_push.server_url',
  template: 'module.bark_push.template',
} as const

const serverUrlInput = ref(DEFAULT_SERVER_URL)

function normalizeServerUrl(value: string): string {
  const trimmed = value.trim().replace(/\/+$/, '')
  return trimmed || DEFAULT_SERVER_URL
}

const {
  saving,
  testing,
  enabled,
  secretIsSet,
  secretInput,
  templateInput,
  lastTestResult,
  canEnable,
  saveConfig,
  handleTest,
} = useNotificationChannelSettings({
  moduleId: 'bark_push',
  channel: 'bark',
  secretKey: CONFIG_KEYS.device_key,
  templateKey: CONFIG_KEYS.template,
  secretRemark: 'Bark Device Key',
  templateRemark: 'Bark 推送模板',
  logLabel: 'Bark 推送',
  messages: {
    loadFailed: 'ui.failedToLoadBarkSettings',
    saveFailed: 'ui.failedToSaveBarkSettings',
    saved: 'ui.barkSettingsSaved',
    testFailed: 'ui.failedToSendTestNotification',
  },
  applyConfigs: (configsByKey) => {
    const serverUrl = configsByKey.get(CONFIG_KEYS.server_url)
    serverUrlInput.value = typeof serverUrl?.value === 'string' && serverUrl.value.trim()
      ? serverUrl.value
      : DEFAULT_SERVER_URL
  },
  collectExtraUpdates: (add) => {
    add(CONFIG_KEYS.server_url, normalizeServerUrl(serverUrlInput.value), 'Bark 服务器地址')
  },
})
</script>
