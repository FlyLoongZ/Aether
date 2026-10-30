import type { InjectionKey, Ref } from 'vue'

export interface SelectLabelContext {
  modelValue: Ref<string | null>
  labels: Map<string, string>
}

export const SELECT_LABEL_CONTEXT_KEY: InjectionKey<SelectLabelContext> = Symbol('aether-select-label')
