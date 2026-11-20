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

## 体检与排空

```bash
# 只读：报告"当前钥 / 旧钥 / 旧格式 / 明文回落 / 不可解"五类行数与样例
aether-gateway secrets verify

# 机器可读
aether-gateway secrets verify --json

# 把所有可枚举的行喂给迁移路径，随后自动复检
aether-gateway secrets rotate
```

两条命令都是离线维护命令，复用与 `db status` 相同的状态装配；退出码含义：

| 退出码 | 含义 |
|---|---|
| `0` | 所有行都由当前钥封装 |
| `1` | 仍有残留（旧钥 / 旧格式 / 明文回落 / 不可解） |
| `2` | 得不出结论：没配 `ENCRYPTION_KEY_PREVIOUS` 但库里有旧钥行、枚举失败、或迁移行失败 |

`rotate` 允许网关在线执行：每行都走 `compare_and_set`，并发写不会被覆盖，
冲突行重试三次后跳过并计入报告。

## 覆盖范围

`verify` 覆盖 13 个密文位置：`api_keys`、`oauth_providers`、
`provider_api_keys`（凭据、`auth_config`、proxy 密码）、`providers` /
`provider_endpoints` 的 proxy 密码、`providers.config` 里的 Provider Ops
凭据、`system_configs`（6 个已知敏感键）、`ldap_configs`、`proxy_nodes`、
`payment_gateway_configs`，以及**未过期的 pending** `payment_orders`。

两类不参与轮换：

- **已终结的支付订单**保留已失效的 Stripe client secret，迁移路径明确拒绝
  改写非活跃订单，因此扫描时排除，避免永久红灯。
- **S3 备份对象**有独立密钥（`AETHER_BACKUP_ENCRYPTION_KEY`，缺省才回落
  `ENCRYPTION_KEY`），对象在对象存储里且每次备份重写；轮换做法是用新钥继续
  备份，需要恢复旧对象时给 `aether-backup-restore --key-file` 传候选钥。

历史 Python-Fernet 密文没有旧钥回退，无法重写，`verify` 会把它计入
`legacy-format`，需要人工重存。

## 读路径会自动排空

`secrets rotate` 只是兜底。读路径本身已经按 `key_id` 判断：任何被读到的、
由旧钥封装的密文都会用当前钥与当前用途重新封装并写回，所以有流量的行在
轮换期间会自动收敛。

