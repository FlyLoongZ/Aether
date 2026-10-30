<script setup lang="ts">
import { TooltipRoot } from 'reka-ui'

interface Props {
  defaultOpen?: boolean
  open?: boolean
  delayDuration?: number
  disableHoverableContent?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  defaultOpen: false,
  delayDuration: 200,
  disableHoverableContent: false
})

const emit = defineEmits<{
  'update:open': [value: boolean]
}>()
</script>

<template>
  <TooltipRoot
    :default-open="props.defaultOpen"
    :open="props.open"
    :delay-duration="props.delayDuration"
    :disable-hoverable-content="props.disableHoverableContent"
    @update:open="emit('update:open', $event)"
  >
    <slot />
  </TooltipRoot>
</template>
