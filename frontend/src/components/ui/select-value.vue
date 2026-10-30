<script setup lang="ts">
import { inject } from 'vue'
import { SelectValue as SelectValuePrimitive } from 'reka-ui'
import { SELECT_LABEL_CONTEXT_KEY } from './select-label-context'

interface Props {
  placeholder?: string
}

const props = defineProps<Props>()
const context = inject(SELECT_LABEL_CONTEXT_KEY, null)

function resolveText(selectedLabel: string[] | undefined): string {
  const value = context?.modelValue.value
  if (value !== null && value !== undefined) {
    const registered = context?.labels.get(String(value))
    if (registered) return registered
  }
  if (selectedLabel?.length) return selectedLabel.join(', ')
  return props.placeholder ?? ''
}
</script>

<template>
  <SelectValuePrimitive
    :placeholder="placeholder"
    v-slot="slotProps"
  >
    <slot
      v-if="$slots.default"
      v-bind="slotProps"
    />
    <template v-else>
      {{ resolveText(slotProps.selectedLabel) }}
    </template>
  </SelectValuePrimitive>
</template>
