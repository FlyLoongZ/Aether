<script setup lang="ts">
import { AvatarImage as AvatarImagePrimitive } from 'reka-ui'
import { cn } from '@/lib/utils'
import { computed } from 'vue'

interface Props {
  class?: string
  src?: string
  alt?: string
}

const props = withDefaults(defineProps<Props>(), {
  class: undefined,
  src: undefined,
  alt: ''
})

const imageClass = computed(() =>
  cn('aspect-square h-full w-full', props.class)
)
</script>

<template>
  <AvatarImagePrimitive
    :class="imageClass"
    :src="src || ''"
    :alt="alt"
  />
</template>
