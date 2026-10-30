<script setup lang="ts">
import { AvatarFallback as AvatarFallbackPrimitive } from 'reka-ui'
import { cn } from '@/lib/utils'
import { computed } from 'vue'

interface Props {
  class?: string
}

const props = defineProps<Props>()

const fallbackClass = computed(() =>
  cn(
    'flex h-full w-full items-center justify-center rounded-full bg-muted',
    props.class
  )
)
</script>

<template>
  <AvatarFallbackPrimitive :class="fallbackClass">
    <slot />
  </AvatarFallbackPrimitive>
</template>
