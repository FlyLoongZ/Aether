<template>
  <div class="w-full h-full">
    <canvas ref="chartRef" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  Title,
  Tooltip,
  type ChartData,
  type ChartOptions,
} from 'chart.js'
import { useChartJs } from './useChartJs'
import {
  CHART_GRID_COLOR,
  CHART_LEGEND_COLOR,
  CHART_TICK_COLOR,
  chartTooltipStyle,
} from './chart-theme'

interface Props {
  data: ChartData<'bar'>
  options?: ChartOptions<'bar'>
  stacked?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  options: undefined,
  stacked: true,
})

ChartJS.register(CategoryScale, LinearScale, BarElement, BarController, Title, Tooltip, Legend)

const mergedOptions = computed<ChartOptions<'bar'>>(() => ({
  responsive: true,
  maintainAspectRatio: false,
  interaction: {
    mode: 'index',
    intersect: false,
  },
  scales: {
    x: {
      stacked: props.stacked,
      grid: { color: CHART_GRID_COLOR },
      ticks: { color: CHART_TICK_COLOR },
    },
    y: {
      stacked: props.stacked,
      grid: { color: CHART_GRID_COLOR },
      ticks: { color: CHART_TICK_COLOR },
    },
  },
  plugins: {
    legend: {
      position: 'top',
      labels: {
        color: CHART_LEGEND_COLOR,
        usePointStyle: true,
        padding: 16,
      },
    },
    tooltip: chartTooltipStyle,
  },
  ...props.options,
}))

const chartRef = ref<HTMLCanvasElement>()
useChartJs({
  chartRef,
  type: 'bar',
  data: () => props.data,
  options: () => mergedOptions.value,
})
</script>
