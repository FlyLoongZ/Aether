import { nextTick, onMounted, onUnmounted, watch, type Ref } from 'vue'
import { Chart as ChartJS, type ChartData, type ChartOptions, type ChartType } from 'chart.js'
import { useI18n } from '@/i18n'

interface UseChartJsOptions<T extends ChartType> {
  /** 画布元素引用（由组件通过 ref="..." 绑定） */
  chartRef: Ref<HTMLCanvasElement | undefined>
  type: T
  /** 当前数据；调用方应传入 computed，引用变化即触发更新 */
  data: () => ChartData<T>
  /** 当前配置；locale 由本 composable 统一注入 */
  options: () => ChartOptions<T>
}

/**
 * Chart.js 图表的创建/更新/销毁生命周期。
 *
 * 三个图表组件（Bar/Line/Doughnut）此前各自复制同一段生命周期代码，
 * 且更新策略不一致（部分使用 deep watch 导致大数据集被深度遍历）。
 * 这里统一为浅监听：调用方传入 computed 数据，引用变化即可触发更新。
 */
export function useChartJs<T extends ChartType>(config: UseChartJsOptions<T>) {
  const { chartRef } = config
  const { locale } = useI18n()
  let chart: ChartJS<T> | null = null

  function buildOptions(): ChartOptions<T> {
    return { ...config.options(), locale: locale.value }
  }

  onMounted(async () => {
    await nextTick()
    if (!chartRef.value) return
    chart = new ChartJS(chartRef.value, {
      type: config.type,
      data: config.data(),
      options: buildOptions(),
    })
  })

  onUnmounted(() => {
    chart?.destroy()
    chart = null
  })

  watch(config.data, (data) => {
    if (!chart) return
    chart.data = data
    chart.update('none')
  })

  watch([config.options, locale], () => {
    if (!chart) return
    chart.options = buildOptions()
    chart.update('none')
  }, { deep: true })

}
