/**
 * 解析模型 include/exclude 模式输入：逗号分隔、去空白、去重。
 * Key 表单与 OAuth 账号编辑弹窗此前各写一份，这里统一。
 */
export function parseModelPatternText(text: string): string[] {
  if (!text.trim()) return []
  const patterns = text
    .split(',')
    .map(pattern => pattern.trim())
    .filter(pattern => pattern.length > 0)
  return [...new Set(patterns)]
}
