import { computed } from 'vue'
import { useI18n } from '@/i18n'
import type { ConversationLabels } from '../conversation/labels'

/**
 * Builds the labels the body-document worker needs.
 *
 * The worker has no locale (no `window`, so the i18n module would always fall
 * back to the default language), so every string it renders has to arrive with
 * the request. Returning a computed keeps the labels in sync with language
 * switching: the body viewer re-requests chunks after a locale change because
 * the labels are part of the render cache key.
 */
export function useConversationLabels() {
  const { t } = useI18n()
  return computed<ConversationLabels>(() => ({
    roleUser: t('conversation.role.user'),
    roleAssistant: t('conversation.role.assistant'),
    roleSystem: t('conversation.role.system'),
    roleTool: t('conversation.role.tool'),
    roleModel: t('conversation.role.model'),
    roleInstructions: t('conversation.role.instructions'),
    badgeThinking: t('conversation.badge.thinking'),
    badgeToolCall: t('conversation.badge.toolCall'),
    badgeToolResult: t('conversation.badge.toolResult'),
    badgeImage: t('conversation.badge.image'),
    badgeFunctionCall: t('conversation.badge.functionCall'),
    badgeFunctionResult: t('conversation.badge.functionResult'),
    emptyRequestBody: t('conversation.empty.requestBody'),
    emptyResponseBody: t('conversation.empty.responseBody'),
    emptyResponseData: t('conversation.empty.responseData'),
    unknownApiFormat: t('conversation.unknownApiFormat'),
    imageInline: t('ui.imagea71f445'),
    truncatedMarker: t('conversation.truncatedMarker'),
    largeImageAlt: t('conversation.largeImageAlt'),
  }))
}
