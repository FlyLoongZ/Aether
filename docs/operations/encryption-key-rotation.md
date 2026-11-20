# 加密密钥轮换

网关用 `ENCRYPTION_KEY` 派生根密钥，加密两类持久化机密：

- **信封密文**：`aether-secret:v1:<key_id>:<payload>`，解密只依赖根密钥。
- **用途绑定密文**：在根密钥之上再按用途与盐派生（OAuth 客户端密钥、
  LDAP bind 密码、系统配置里的敏感项、供应商目录凭据、支付网关密钥等），
  解密需要用途版本号。

## 轮换步骤

1. 设置新的 `ENCRYPTION_KEY`，同时把旧值放进 `ENCRYPTION_KEY_PREVIOUS`。
   旧值只支持保留**一代**，再旧的密钥不再尝试。
2. 重启网关。此后读取旧密文会回退到旧密钥；读到之后，凡是走“读时迁移”的
   路径（用途绑定密文的 `decrypt_or_migrate_*`、系统配置、LDAP、OAuth、
   供应商目录快照）会用当前密钥与当前用途版本重新封装并写回。
3. 观察日志里的 `*_secret_decrypt_failed` / `*_secret_migrate_failed` 与
   迁移统计，确认没有持续失败的记录。
4. 确认没有记录仍在用旧密钥，再删除 `ENCRYPTION_KEY_PREVIOUS`。

## 判断是否还有记录用旧密钥

信封格式自带 `key_id`，可以直接统计：

```sql
-- 旧 key_id 为旧 ENCRYPTION_KEY 派生出的标识，从旧密文前缀里读取。
SELECT count(*) FROM api_keys
 WHERE key_encrypted LIKE 'aether-secret:v1:<old_key_id>:%';
```

用途绑定密文的 `key_id` 同样在信封前缀里，可对相应列（`client_secret_encrypted`、
`bind_password_encrypted`、`encrypted_key`、`auth_config`、`proxy_password`、
`system_configs.value`）做同样的前缀匹配。

## 已知缺口

**没有后台批量重写任务。** 轮换后仍然只有两条路径会把旧密文搬到新密钥：

- 读时迁移（上述用途绑定密文与系统配置）；
- 该行被正常写入一次。

因此，长期不再被读取或写入的行会一直停在旧密钥下。删除
`ENCRYPTION_KEY_PREVIOUS` 之前必须用上面的 SQL 确认计数为零，否则这些行
会解密失败，调用点按各自的历史行为回落（多数会报错并拒绝使用该凭据）。
未实现批量重写是有意为之：写一个覆盖六类密文、且与读路径派生规则完全一致
的重写器风险高于收益，当前用“保留旧密钥 + 读时迁移”达成同样的可用性。

## 密钥长度与格式

`ENCRYPTION_KEY` 与 `ENCRYPTION_KEY_PREVIOUS` 都要求至少 32 字节的随机串
（`validate_encryption_key`），两者格式校验一致。生成示例：

```bash
openssl rand -base64 48
```
