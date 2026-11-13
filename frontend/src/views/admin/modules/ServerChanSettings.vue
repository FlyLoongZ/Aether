<template>
  <PageContainer>
    <PageHeader
      :title="t('ui.serverChanNotifications')"
      :description="t('ui.thirdPartyPushServiceForTheServerChanNotificationChannel')"
    />

    <div class="mt-6 space-y-6">
      <CardSection
        :title="t('ui.serviceSettings')"
        :description="t('ui.configureServerChanTurboSendkeyAndServiceEnabledState')"
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
              <Label class="text-sm font-medium"> {{ t('ui.enableServerChanNotifications') }} </Label>
              <p class="mt-1 text-xs text-muted-foreground">
                {{ t('ui.serverChanMustBeEnabledBeforeTheNotificationServiceCan') }}
              </p>
            </div>
            <Switch
              v-model="enabled"
              :disabled="!canEnable"
            />
          </div>

          <div>
            <Label
              for="server-chan-send-key"
              class="block text-sm font-medium"
            >
              SendKey
            </Label>
            <Input
              id="server-chan-send-key"
              v-model="secretInput"
              masked
              :placeholder="secretIsSet ? t('ui.setLeaveEmptyToKeepUnchanged') : 'SCTxxxxxxxxxxxxxxxxxxxxxxxx'"
              class="mt-1"
            />
            <p class="mt-1 text-xs text-muted-foreground">
              {{ t('ui.availableInThe') }} <span class="font-mono">sct.ftqq.com</span> {{ t('ui.console') }}
            </p>
          </div>
        </div>
      </CardSection>

      <CardSection
        :title="t('ui.notificationTemplates')"
        :description="t('ui.markdownTemplatesSupportValueAndValueVariables')"
      >
        <div>
          <Label
            for="server-chan-template"
            class="block text-sm font-medium"
          > {{ t('ui.templateContent') }} </Label>
          <Textarea
            id="server-chan-template"
            v-model="templateInput"
            rows="10"
            class="mt-1 font-mono text-sm"
            placeholder="**{title}**&#10;&#10;{body}"
            spellcheck="false"
          />
        </div>
      </CardSection>

      <CardSection
        :title="t('ui.testService')"
        :description="t('ui.sendAServerChanTestNotificationUsingTheSavedSettings')"
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
import { Button, Input, Label, Switch, Textarea } from '@/components/ui'
import { PageHeader, PageContainer, CardSection } from '@/components/layout'
import { RouterLink } from 'vue-router'
import { useI18n } from '@/i18n'
import { formatNotificationChannel } from './notificationChannel'
import { useNotificationChannelSettings } from './composables/useNotificationChannelSettings'

const { t } = useI18n()

const CONFIG_KEYS = {
  enabled: 'module.server_chan_push.enabled',
  send_key: 'module.server_chan_push.send_key',
  template: 'module.server_chan_push.template',
} as const

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
  moduleId: 'server_chan_push',
  channel: 'server_chan',
  secretKey: CONFIG_KEYS.send_key,
  templateKey: CONFIG_KEYS.template,
  secretRemark: 'Server 酱 SendKey',
  templateRemark: 'Server 酱推送模板',
  logLabel: 'Server 酱推送',
  messages: {
    loadFailed: 'ui.failedToLoadServerChanSettings',
    saveFailed: 'ui.failedToSaveServerChanSettings',
    saved: 'ui.serverChanSettingsSaved',
    testFailed: 'ui.failedToSendTestNotification',
  },
})
</script>
