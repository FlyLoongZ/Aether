<script setup lang="ts">
import { computed } from 'vue'
import { DropdownMenuRoot } from 'reka-ui'

interface Props {
  defaultOpen?: boolean
  open?: boolean
  modal?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  open: undefined,
  modal: true
})

defineEmits<{
  'update:open': [value: boolean]
}>()


const openProp = computed(() =>
  props.open !== undefined ? { open: props.open } : {}
)
</script>

<template>
  <DropdownMenuRoot
    :default-open="defaultOpen"
    v-bind="openProp"
    :modal="modal"
    @update:open="$emit('update:open', $event)"
  >
    <slot />
  </DropdownMenuRoot>
</template>
