<template>
  <div class="w-full h-full relative">
    <canvas ref="chartRef" />
    <div
      v-if="crosshairStats"
      class="absolute top-2 max-w-[calc(100%-1rem)] break-words rounded-lg border border-gray-600 bg-gray-800/90 px-3 py-2 text-sm text-gray-100 shadow-lg transition-[left,right] duration-150"
      :class="cardSide === 'left' ? 'left-12' : 'right-2'"
    >
      <div class="font-medium text-yellow-400">
        {{ t('chart.crosshairValue', { value: crosshairStats.yValue.toFixed(1) }) }}
      </div>
      <!-- 单个 dataset 时显示简单统计 -->
      <div
        v-if="crosshairStats.datasets.length === 1"
        class="mt-1"
      >
        <span class="text-green-400">{{ crosshairStats.datasets[0].belowCount }}</span> / {{ crosshairStats.datasets[0].totalCount }} {{ t('chart.pointsBelow') }}
        <span class="ml-2 text-blue-400">({{ crosshairStats.datasets[0].belowPercent.toFixed(1) }}%)</span>
      </div>
      <!-- 多个 dataset 时按模型分别显示 -->
      <div
        v-else
        class="mt-1 space-y-0.5"
      >
        <div
          v-for="ds in crosshairStats.datasets"
          :key="ds.label"
          class="flex items-center gap-2"
        >
          <div
            class="w-2 h-2 rounded-full flex-shrink-0"
            :style="{ backgroundColor: ds.color }"
          />
          <span class="text-gray-300 truncate max-w-[80px]">{{ ds.label }}:</span>
          <span class="text-green-400">{{ ds.belowCount }}</span>/<span class="text-gray-400">{{ ds.totalCount }}</span>
          <span class="text-blue-400">({{ ds.belowPercent.toFixed(0) }}%)</span>
        </div>
        <!-- 总计 -->
        <div class="flex items-center gap-2 pt-1 border-t border-gray-600 mt-1">
          <span class="text-gray-300">{{ t('chart.total') }}:</span>
          <span class="text-green-400">{{ crosshairStats.totalBelowCount }}</span>/<span class="text-gray-400">{{ crosshairStats.totalCount }}</span>
          <span class="text-blue-400">({{ crosshairStats.totalBelowPercent.toFixed(1) }}%)</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { TimeScatterChartData, TimeScatterPoint } from './types'
import { getI18nLocale, useI18n } from '@/i18n'
import { ref, onMounted, onUnmounted, watch, nextTick, computed } from 'vue'
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  ScatterController,
  TimeScale,
  Title,
  Tooltip,
  Legend,
  type ChartOptions,
  type Plugin,
  type Scale
} from 'chart.js'
import './dateFnsAdapter'
import { enUS, zhCN } from 'date-fns/locale'

const props = withDefaults(defineProps<Props>(), {
  height: 300,
  options: undefined,
  compressGaps: false,
  gapThreshold: 60, // 默认 60 分钟以上的间隙会被压缩
  compressedGapSize: 5 // 压缩后的间隙显示为 5 分钟
})

ChartJS.register(
  LinearScale,
  PointElement,
  ScatterController,
  TimeScale,
  Title,
  Tooltip,
  Legend
)

interface Props {
  data: TimeScatterChartData
  options?: ChartOptions<'scatter'>
  height?: number
  compressGaps?: boolean
  gapThreshold?: number // 间隙阈值（分钟）
  compressedGapSize?: number // 压缩后显示大小（分钟）
}

interface DatasetStats {
  label: string
  color: string
  belowCount: number
  totalCount: number
  belowPercent: number
}

interface CrosshairStats {
  yValue: number
  datasets: DatasetStats[]
  totalBelowCount: number
  totalCount: number
  totalBelowPercent: number
}

interface GapInfo {
  startX: number // 压缩后的 X 位置
  originalStart: Date
  originalEnd: Date
  duration: number // 间隙时长（毫秒）
}

const chartRef = ref<HTMLCanvasElement>()
const { locale, t } = useI18n()
let chart: ChartJS<'scatter', TimeScatterPoint[]> | null = null

const crosshairY = ref<number | null>(null)
const cardSide = ref<'left' | 'right'>('right')
const gapInfoList = ref<GapInfo[]>([])

interface PreparedRenderData {
  chartData: TimeScatterChartData
  gaps: GapInfo[]
}

const crosshairStats = computed<CrosshairStats | null>(() => {
  if (crosshairY.value === null || !props.data.datasets) return null

  const datasetStats: DatasetStats[] = []
  let totalBelowCount = 0
  let totalCount = 0

  for (const dataset of props.data.datasets) {
    if (!dataset.data) continue

    let belowCount = 0
    let dsTotal = 0

    for (const point of dataset.data) {
      const p = point
      if (typeof p.y === 'number') {
        dsTotal++
        totalCount++
        if (p.y <= crosshairY.value) {
          belowCount++
          totalBelowCount++
        }
      }
    }

    if (dsTotal > 0) {
      datasetStats.push({
        label: dataset.label || t('chart.unknown'),
        color: (dataset.backgroundColor as string) || 'rgba(59, 130, 246, 0.7)',
        belowCount,
        totalCount: dsTotal,
        belowPercent: (belowCount / dsTotal) * 100
      })
    }
  }

  if (totalCount === 0) return null

  return {
    yValue: crosshairY.value,
    datasets: datasetStats,
    totalBelowCount,
    totalCount,
    totalBelowPercent: (totalBelowCount / totalCount) * 100
  }
})

const MAX_INTERVAL_MINUTES = 120
const TARGET_TICKS = [0, 1, 2, 5, 10, 30, 60, 120]

function toDisplayValue(realValue: number): number {
  const clamped = Math.min(Math.max(realValue, 0), MAX_INTERVAL_MINUTES)
  return Math.log1p(clamped)
}

function toRealValue(displayValue: number): number {
  return Math.expm1(displayValue)
}

// 压缩时间间隙的数据转换
function compressTimeGaps(data: TimeScatterChartData): {
  data: TimeScatterChartData
  gaps: GapInfo[]
  timeMapping: Map<number, number> // 原始时间 -> 压缩后时间
} {
  const gapThresholdMs = props.gapThreshold * 60 * 1000
  const compressedGapSizeMs = props.compressedGapSize * 60 * 1000

  // 收集所有数据点的时间戳并排序
  const allTimestamps: number[] = []
  for (const dataset of data.datasets) {
    for (const point of dataset.data) {
      allTimestamps.push(new Date(point.x).getTime())
    }
  }
  allTimestamps.sort((a, b) => a - b)

  if (allTimestamps.length < 2) {
    return { data, gaps: [], timeMapping: new Map() }
  }

  // 找出所有大间隙
  const gaps: GapInfo[] = []
  const timeMapping = new Map<number, number>()
  let totalCompression = 0

  for (let i = 1; i < allTimestamps.length; i++) {
    const gap = allTimestamps[i] - allTimestamps[i - 1]
    if (gap > gapThresholdMs) {
      const compression = gap - compressedGapSizeMs
      gaps.push({
        startX: allTimestamps[i - 1] - totalCompression + compressedGapSizeMs / 2,
        originalStart: new Date(allTimestamps[i - 1]),
        originalEnd: new Date(allTimestamps[i]),
        duration: gap
      })
      totalCompression += compression
    }
  }

  // 构建时间映射
  let currentCompression = 0
  let gapIndex = 0
  for (const ts of allTimestamps) {
    // 检查是否跨过了某个间隙
    while (gapIndex < gaps.length && ts >= gaps[gapIndex].originalEnd.getTime()) {
      const gapDuration = gaps[gapIndex].originalEnd.getTime() - gaps[gapIndex].originalStart.getTime()
      currentCompression += gapDuration - compressedGapSizeMs
      gapIndex++
    }
    timeMapping.set(ts, ts - currentCompression)
  }

  // 更新间隙的 startX 为压缩后的坐标
  currentCompression = 0
  for (const gap of gaps) {
    gap.startX = gap.originalStart.getTime() - currentCompression + compressedGapSizeMs / 2
    const gapDuration = gap.originalEnd.getTime() - gap.originalStart.getTime()
    currentCompression += gapDuration - compressedGapSizeMs
  }

  // 转换数据
  const compressedData: TimeScatterChartData = {
    ...data,
    datasets: data.datasets.map(dataset => ({
      ...dataset,
      data: (dataset.data).map(point => {
        const originalTs = new Date(point.x).getTime()
        const compressedTs = timeMapping.get(originalTs) ?? originalTs
        return {
          ...point,
          x: new Date(compressedTs).toISOString(),
          _originalX: point.x // 保存原始时间
        }
      })
    }))
  }

  return { data: compressedData, gaps, timeMapping }
}

// 转换数据点的 Y 值
function transformData(data: TimeScatterChartData): TimeScatterChartData {
  return {
    ...data,
    datasets: data.datasets.map(dataset => ({
      ...dataset,
      data: (dataset.data).map(point => ({
        ...point,
        y: toDisplayValue(point.y),
        _originalY: point._originalY ?? point.y  // 保存原始值用于 tooltip
      }))
    }))
  }
}

function prepareRenderData(): PreparedRenderData {
  let dataToUse = props.data
  let gaps: GapInfo[] = []

  if (props.compressGaps) {
    const compressedResult = compressTimeGaps(props.data)
    dataToUse = compressedResult.data
    gaps = compressedResult.gaps
  }

  return {
    chartData: transformData(dataToUse),
    gaps
  }
}

// 格式化时长
function formatDuration(ms: number): string {
  const hours = Math.floor(ms / (1000 * 60 * 60))
  const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60))
  if (hours > 0) {
    return `${hours}h${minutes > 0 ? `${minutes}m` : ''}`
  }
  return `${minutes}m`
}

const defaultOptions = computed<ChartOptions<'scatter'>>(() => ({
  locale: locale.value,
  responsive: true,
  maintainAspectRatio: false,
  interaction: {
    mode: 'nearest',
    intersect: true
  },
  scales: {
    x: {
      type: 'time',
      adapters: {
        date: { locale: locale.value === 'zh-CN' ? zhCN : enUS }
      },
      time: {
        displayFormats: {
          hour: 'HH:mm'
        },
        tooltipFormat: 'HH:mm'
      },
      grid: {
        color: 'rgba(156, 163, 175, 0.1)'
      },
      ticks: {
        color: 'rgb(107, 114, 128)',
        maxRotation: 0,
        autoSkip: true,
        maxTicksLimit: 10
      }
    },
    y: {
      type: 'linear',
      min: 0,
      max: toDisplayValue(MAX_INTERVAL_MINUTES),
      grid: {
        color: 'rgba(156, 163, 175, 0.1)'
      },
      ticks: {
        color: 'rgb(107, 114, 128)',
        autoSkip: false,
        callback(this: Scale, value: string | number) {
          const display = Number(value)
          const match = TARGET_TICKS.find(target => Math.abs(toDisplayValue(target) - display) < 1e-6)
          return match === undefined ? '' : `${match}`
        }
      },
      title: {
        display: true,
        text: t('chart.intervalAxis'),
        color: 'rgb(107, 114, 128)'
      },
      afterBuildTicks(scale: Scale) {
        scale.ticks = TARGET_TICKS.map(val => ({ value: toDisplayValue(val) }))
      }
    }
  },
  plugins: {
    legend: {
      display: false
    },
    tooltip: {
      backgroundColor: 'rgb(31, 41, 55)',
      titleColor: 'rgb(243, 244, 246)',
      bodyColor: 'rgb(243, 244, 246)',
      borderColor: 'rgb(75, 85, 99)',
      borderWidth: 1,
      callbacks: {
        title: (contexts) => {
          if (contexts.length === 0) return ''
          const point = contexts[0].raw as { x: string; _originalX?: string }
          const timeStr = point._originalX || point.x
          const date = new Date(timeStr)
          return date.toLocaleString(getI18nLocale(), {
            month: 'numeric',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
          })
        },
        label: (context) => {
          const point = context.raw as { x: string; y: number; _originalY?: number }
          const realY = point._originalY ?? toRealValue(point.y)
          return t('chart.intervalTooltip', { value: realY.toFixed(1) })
        }
      }
    }
  },
  onHover: (event, _elements, chartInstance) => {
    const canvas = chartInstance.canvas
    if (!canvas) return

    const rect = canvas.getBoundingClientRect()
    const mouseX = (event.native as MouseEvent)?.clientX
    const mouseY = (event.native as MouseEvent)?.clientY

    if (mouseY === undefined) {
      crosshairY.value = null
      cardSide.value = 'right'
      return
    }

    const { chartArea, scales } = chartInstance
    const yScale = scales.y

    if (!chartArea || !yScale) return

    const relativeY = mouseY - rect.top

    if (relativeY < chartArea.top || relativeY > chartArea.bottom) {
      crosshairY.value = null
      cardSide.value = 'right'
    } else {
      const displayValue = yScale.getValueForPixel(relativeY)
      crosshairY.value = displayValue !== undefined ? toRealValue(displayValue) : null

      if (mouseX !== undefined) {
        const relativeX = mouseX - rect.left
        const ratio = (relativeX - chartArea.left) / Math.max(chartArea.right - chartArea.left, 1)
        if (cardSide.value === 'right' && ratio > 0.62) {
          cardSide.value = 'left'
        } else if (cardSide.value === 'left' && ratio < 0.5) {
          cardSide.value = 'right'
        }
      }
    }

    chartInstance.draw()
  }
}))

// 修改 crosshairPlugin 使用显示值
const crosshairPluginWithTransform: Plugin<'scatter'> = {
  id: 'crosshairLine',
  afterDraw: (chartInstance) => {
    if (crosshairY.value === null) return

    const { ctx, chartArea, scales } = chartInstance
    const yScale = scales.y
    if (!yScale || !chartArea) return

    // 将实际值转换为显示值再获取像素位置
    const displayValue = toDisplayValue(crosshairY.value)
    const yPixel = yScale.getPixelForValue(displayValue)

    if (yPixel < chartArea.top || yPixel > chartArea.bottom) return

    ctx.save()
    ctx.beginPath()
    ctx.moveTo(chartArea.left, yPixel)
    ctx.lineTo(chartArea.right, yPixel)
    ctx.strokeStyle = 'rgba(250, 204, 21, 0.8)'
    ctx.lineWidth = 2
    ctx.setLineDash([6, 4])
    ctx.stroke()
    ctx.restore()
  }
}

// 绘制间隙标记的插件
const gapMarkerPlugin: Plugin<'scatter'> = {
  id: 'gapMarker',
  afterDraw: (chartInstance) => {
    if (gapInfoList.value.length === 0) return

    const { ctx, chartArea, scales } = chartInstance
    const xScale = scales.x
    if (!xScale || !chartArea) return

    ctx.save()

    for (const gap of gapInfoList.value) {
      const xPixel = xScale.getPixelForValue(gap.startX)
      if (xPixel < chartArea.left || xPixel > chartArea.right) continue

      // 绘制波浪线断点标记
      const waveHeight = 6
      const waveWidth = 8
      const y1 = chartArea.top
      const y2 = chartArea.bottom

      ctx.beginPath()
      ctx.strokeStyle = 'rgba(156, 163, 175, 0.5)'
      ctx.lineWidth = 1.5
      ctx.setLineDash([])

      // 绘制波浪线
      for (let y = y1; y < y2; y += waveHeight * 2) {
        ctx.moveTo(xPixel - waveWidth / 2, y)
        ctx.quadraticCurveTo(xPixel + waveWidth / 2, y + waveHeight, xPixel - waveWidth / 2, y + waveHeight * 2)
      }
      ctx.stroke()

      // 绘制间隙时长标签
      ctx.fillStyle = 'rgba(107, 114, 128, 0.8)'
      ctx.font = '10px sans-serif'
      ctx.textAlign = 'center'
      const label = formatDuration(gap.duration)
      ctx.fillText(label, xPixel, chartArea.top - 4)
    }

    ctx.restore()
  }
}

function handleMouseLeave() {
  crosshairY.value = null
  if (chart) {
    chart.draw()
  }
}

function createChart() {
  if (!chartRef.value) return

  const { chartData, gaps } = prepareRenderData()
  gapInfoList.value = gaps

  chart = new ChartJS<'scatter', TimeScatterPoint[]>(chartRef.value, {
    type: 'scatter',
    data: chartData,
    options: {
      ...defaultOptions.value,
      ...props.options
    },
    plugins: [crosshairPluginWithTransform, gapMarkerPlugin]
  })

  chartRef.value.addEventListener('mouseleave', handleMouseLeave)
}

function updateChart() {
  if (chart) {
    const { chartData, gaps } = prepareRenderData()
    gapInfoList.value = gaps
    chart.data = chartData
    chart.update('none')
  }
}

onMounted(async () => {
  await nextTick()
  createChart()
})

onUnmounted(() => {
  if (chartRef.value) {
    chartRef.value.removeEventListener('mouseleave', handleMouseLeave)
  }
  if (chart) {
    chart.destroy()
    chart = null
  }
})

watch(
  [
    () => props.data,
    () => props.compressGaps,
    () => props.gapThreshold,
    () => props.compressedGapSize
  ],
  updateChart
)
watch([() => props.options, defaultOptions], () => {
  if (chart) {
    chart.options = {
      ...defaultOptions.value,
      ...props.options
    }
    chart.update('none')
  }
})
</script>
