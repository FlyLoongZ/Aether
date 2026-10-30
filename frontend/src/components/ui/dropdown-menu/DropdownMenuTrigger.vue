<script setup lang="ts">
import { DropdownMenuTrigger as DropdownMenuTriggerPrimitive } from 'reka-ui'

interface Props {
  asChild?: boolean
  disabled?: boolean
}

withDefaults(defineProps<Props>(), {
  asChild: false,
  disabled: false
})
</script>

<template>
  <DropdownMenuTriggerPrimitive
    :as-child="asChild"
    :disabled="disabled"
  >
    <slot />
  </DropdownMenuTriggerPrimitive>
</template>
