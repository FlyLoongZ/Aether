/**
 * Chart.js 共享样式：图表组件原本各自复制同一组网格/刻度/提示框颜色。
 * 这里集中维护，避免暗色适配或样式调整时漏改。
 */

/** 网格线颜色（gray-400 + 低透明度） */
export const CHART_GRID_COLOR = 'rgba(156, 163, 175, 0.1)'

/** 坐标轴刻度文字颜色（gray-500） */
export const CHART_TICK_COLOR = 'rgb(107, 114, 128)'

/** 图例文字颜色（gray-500） */
export const CHART_LEGEND_COLOR = 'rgb(107, 114, 128)'

/** 提示框配色（gray-800 / gray-100 / gray-600） */
export const chartTooltipStyle = {
  backgroundColor: 'rgb(31, 41, 55)',
  titleColor: 'rgb(243, 244, 246)',
  bodyColor: 'rgb(243, 244, 246)',
  borderColor: 'rgb(75, 85, 99)',
  borderWidth: 1,
}

/** 坐标轴通用样式 */
export const chartScaleStyle = {
  grid: { color: CHART_GRID_COLOR },
  ticks: { color: CHART_TICK_COLOR },
}
