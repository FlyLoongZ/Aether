import { _adapters } from 'chart.js'
import {
  addDays,
  addHours,
  addMilliseconds,
  addMinutes,
  addMonths,
  addQuarters,
  addSeconds,
  addWeeks,
  addYears,
  differenceInDays,
  differenceInHours,
  differenceInMilliseconds,
  differenceInMinutes,
  differenceInMonths,
  differenceInQuarters,
  differenceInSeconds,
  differenceInWeeks,
  differenceInYears,
  endOfDay,
  endOfHour,
  endOfMinute,
  endOfMonth,
  endOfQuarter,
  endOfSecond,
  endOfWeek,
  endOfYear,
  format,
  isValid,
  parse,
  parseISO,
  startOfDay,
  startOfHour,
  startOfMinute,
  startOfMonth,
  startOfQuarter,
  startOfSecond,
  startOfWeek,
  startOfYear,
  toDate,
} from 'date-fns'
import type { Locale } from 'date-fns/locale'

const FORMATS = {
  datetime: 'MMM d, yyyy, h:mm:ss aaaa',
  millisecond: 'h:mm:ss.SSS aaaa',
  second: 'h:mm:ss aaaa',
  minute: 'h:mm aaaa',
  hour: 'ha',
  day: 'MMM d',
  week: 'PP',
  month: 'MMM yyyy',
  quarter: 'qqq - yyyy',
  year: 'yyyy',
} as const

interface AdapterOptions {
  locale?: Locale
}

interface AdapterContext {
  options: AdapterOptions
}

_adapters._date.override({
  formats(this: AdapterContext): typeof FORMATS {
    return FORMATS
  },

  parse(this: AdapterContext, value: unknown, fmt?: string): number | null {
    if (value === null || typeof value === 'undefined') return null

    let date: Date
    if (typeof value === 'number' || value instanceof Date) {
      date = toDate(value)
    } else if (typeof value === 'string') {
      date = typeof fmt === 'string'
        ? parse(value, fmt, new Date(), { locale: this.options.locale })
        : parseISO(value)
    } else {
      return null
    }

    return isValid(date) ? date.getTime() : null
  },

  format(this: AdapterContext, time: number, fmt: string): string {
    return format(time, fmt, { locale: this.options.locale })
  },

  add(time: number, amount: number, unit: string): number {
    switch (unit) {
      case 'millisecond': return addMilliseconds(time, amount).getTime()
      case 'second': return addSeconds(time, amount).getTime()
      case 'minute': return addMinutes(time, amount).getTime()
      case 'hour': return addHours(time, amount).getTime()
      case 'day': return addDays(time, amount).getTime()
      case 'week': return addWeeks(time, amount).getTime()
      case 'month': return addMonths(time, amount).getTime()
      case 'quarter': return addQuarters(time, amount).getTime()
      case 'year': return addYears(time, amount).getTime()
      default: return time
    }
  },

  diff(max: number, min: number, unit: string): number {
    switch (unit) {
      case 'millisecond': return differenceInMilliseconds(max, min)
      case 'second': return differenceInSeconds(max, min)
      case 'minute': return differenceInMinutes(max, min)
      case 'hour': return differenceInHours(max, min)
      case 'day': return differenceInDays(max, min)
      case 'week': return differenceInWeeks(max, min)
      case 'month': return differenceInMonths(max, min)
      case 'quarter': return differenceInQuarters(max, min)
      case 'year': return differenceInYears(max, min)
      default: return 0
    }
  },

  startOf(time: number, unit: string, weekday?: number): number {
    switch (unit) {
      case 'second': return startOfSecond(time).getTime()
      case 'minute': return startOfMinute(time).getTime()
      case 'hour': return startOfHour(time).getTime()
      case 'day': return startOfDay(time).getTime()
      case 'week': return startOfWeek(time).getTime()
      case 'isoWeek': return startOfWeek(time, { weekStartsOn: (weekday ?? 1) as 0 | 1 | 2 | 3 | 4 | 5 | 6 }).getTime()
      case 'month': return startOfMonth(time).getTime()
      case 'quarter': return startOfQuarter(time).getTime()
      case 'year': return startOfYear(time).getTime()
      default: return time
    }
  },

  endOf(time: number, unit: string): number {
    switch (unit) {
      case 'second': return endOfSecond(time).getTime()
      case 'minute': return endOfMinute(time).getTime()
      case 'hour': return endOfHour(time).getTime()
      case 'day': return endOfDay(time).getTime()
      case 'week': return endOfWeek(time).getTime()
      case 'month': return endOfMonth(time).getTime()
      case 'quarter': return endOfQuarter(time).getTime()
      case 'year': return endOfYear(time).getTime()
      default: return time
    }
  },
})
