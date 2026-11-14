<template>
  <div class="w-full h-full">
    <canvas ref="chartRef" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import {
  ArcElement,
  Chart as ChartJS,
  DoughnutController,
  Legend,
  Title,
  Tooltip,
  type ChartData,
  type ChartOptions,
} from 'chart.js'
import { useChartJs } from './useChartJs'
import { CHART_LEGEND_COLOR, chartTooltipStyle } from './chart-theme'

interface Props {
  data: ChartData<'doughnut'>
  options?: ChartOptions<'doughnut'>
}

const props = withDefaults(defineProps<Props>(), {
  options: undefined,
})

ChartJS.register(ArcElement, DoughnutController, Title, Tooltip, Legend)

const mergedOptions = computed<ChartOptions<'doughnut'>>(() => ({
  responsive: true,
  maintainAspectRatio: false,
  cutout: '60%',
  plugins: {
    legend: {
      position: 'right',
      labels: {
        color: CHART_LEGEND_COLOR,
        usePointStyle: true,
        padding: 16,
        font: { size: 11 },
      },
    },
    tooltip: {
      ...chartTooltipStyle,
      callbacks: {
        label: (context) => {
          const value = context.raw as number
          const total = (context.dataset.data as number[]).reduce((a, b) => a + b, 0)
          const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : '0'
          return `${context.label}: $${value.toFixed(4)} (${percentage}%)`
        },
      },
    },
  },
  ...props.options,
}))

const chartRef = ref<HTMLCanvasElement>()
useChartJs({
  chartRef,
  type: 'doughnut',
  data: () => props.data,
  options: () => mergedOptions.value,
})
</script>
