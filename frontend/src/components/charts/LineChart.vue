<template>
  <div class="w-full h-full">
    <canvas ref="chartRef" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
  type ChartData,
  type ChartOptions,
} from 'chart.js'
import { useChartJs } from './useChartJs'
import { CHART_LEGEND_COLOR, chartScaleStyle, chartTooltipStyle } from './chart-theme'

interface Props {
  data: ChartData<'line'>
  options?: ChartOptions<'line'>
}

const props = withDefaults(defineProps<Props>(), {
  options: undefined,
})

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, LineController, Title, Tooltip, Legend)

const mergedOptions = computed<ChartOptions<'line'>>(() => ({
  responsive: true,
  maintainAspectRatio: false,
  scales: {
    x: chartScaleStyle,
    y: chartScaleStyle,
  },
  plugins: {
    legend: {
      labels: { color: CHART_LEGEND_COLOR },
    },
    tooltip: chartTooltipStyle,
  },
  ...props.options,
}))

const { chartRef } = useChartJs({
  type: 'line',
  data: () => props.data,
  options: () => mergedOptions.value,
})
</script>
