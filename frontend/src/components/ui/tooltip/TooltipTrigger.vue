<script setup lang="ts">
import { TooltipTrigger as TooltipTriggerPrimitive } from 'reka-ui'

interface Props {
  asChild?: boolean
}

withDefaults(defineProps<Props>(), {
  asChild: false
})
</script>

<template>
  <TooltipTriggerPrimitive :as-child="asChild">
    <slot />
  </TooltipTriggerPrimitive>
</template>
