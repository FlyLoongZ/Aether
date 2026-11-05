<template>
  <div class="pointer-events-none fixed inset-x-0 top-4 z-[100] flex justify-center px-4 max-lg:top-20">
    <TransitionGroup
      name="toast"
      tag="div"
      class="pointer-events-auto flex flex-col items-center gap-2"
    >
      <ToastWithProgress
        v-for="toast in toasts"
        :key="toast.id"
        :toast="toast"
        @remove="removeToast(toast.id)"
      />
    </TransitionGroup>
  </div>
</template>

<script setup lang="ts">
import ToastWithProgress from './ToastWithProgress.vue'
import { useToast } from '@/composables/useToast'

const { toasts, removeToast } = useToast()
</script>

<style scoped>
/* 进入动画 - 从上方弹入 */
.toast-enter-active {
  transition: all 0.4s cubic-bezier(0.2, 0.9, 0.3, 1);
}

.toast-enter-from {
  transform: translateY(-20px) scale(0.95);
  opacity: 0;
}

.toast-enter-to {
  transform: translateY(0) scale(1);
  opacity: 1;
}

/* 弹出动画 - 向上消失 */
.toast-leave-active {
  transition: all 0.2s ease-out;
}

.toast-leave-from {
  transform: translateY(0) scale(1);
  opacity: 1;
}

.toast-leave-to {
  transform: translateY(-20px) scale(0.95);
  opacity: 0;
}

/* 移动动画 */
.toast-move {
  transition: all 0.4s cubic-bezier(0.2, 0.9, 0.3, 1);
}
</style>
