<script setup lang="ts">
import { computed, provide, reactive } from 'vue'
import { SelectRoot as SelectRootPrimitive } from 'reka-ui'
import { SELECT_LABEL_CONTEXT_KEY } from './select-label-context'

interface Props {
  defaultValue?: string
  modelValue?: string | null
  open?: boolean
  defaultOpen?: boolean
  dir?: 'ltr' | 'rtl'
  name?: string
  autocomplete?: string
  disabled?: boolean
  required?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  defaultValue: undefined,
  modelValue: undefined,
  open: undefined,
  dir: undefined,
  name: undefined,
  autocomplete: undefined,
})

const emit = defineEmits<{
  'update:modelValue': [value: string]
  'update:open': [value: boolean]
}>()

const labels = reactive(new Map<string, string>())

provide(SELECT_LABEL_CONTEXT_KEY, {
  modelValue: computed(() => props.modelValue ?? null),
  labels,
})

const openProp = computed(() =>
  props.open !== undefined ? { open: props.open } : {}
)


const modelValueProp = computed(() =>
  props.modelValue !== undefined ? { modelValue: props.modelValue ?? '' } : {}
)
</script>

<template>
  <SelectRootPrimitive
    :default-value="defaultValue"
    v-bind="{ ...modelValueProp, ...openProp }"
    :default-open="defaultOpen"
    :dir="dir"
    :name="name"
    :autocomplete="autocomplete"
    :disabled="disabled"
    :required="required"
    @update:model-value="emit('update:modelValue', $event)"
    @update:open="emit('update:open', $event)"
  >
    <slot />
  </SelectRootPrimitive>
</template>
