<script setup lang="ts">
import { Separator as SeparatorPrimitive } from 'reka-ui'
import { cn } from '@/lib/utils'
import { computed } from 'vue'

interface Props {
  class?: string
  orientation?: 'horizontal' | 'vertical'
  decorative?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  class: undefined,
  orientation: 'horizontal',
  decorative: true,
})

const separatorClass = computed(() =>
  cn(
    'shrink-0 bg-border',
    props.orientation === 'horizontal' ? 'h-[1px] w-full' : 'h-full w-[1px]',
    props.class
  )
)
</script>

<template>
  <SeparatorPrimitive
    :class="separatorClass"
    :orientation="orientation"
    :decorative="decorative"
  />
</template>
