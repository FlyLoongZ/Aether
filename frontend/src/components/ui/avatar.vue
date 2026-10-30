<script setup lang="ts">
import { AvatarRoot } from 'reka-ui'
import { cn } from '@/lib/utils'
import { computed } from 'vue'

interface Props {
  class?: string
}

const props = defineProps<Props>()

const avatarClass = computed(() =>
  cn('relative flex h-10 w-10 shrink-0 overflow-hidden rounded-full', props.class)
)
</script>

<template>
  <AvatarRoot :class="avatarClass">
    <slot />
  </AvatarRoot>
</template>
