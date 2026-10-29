# Rust 依赖分批升级记录

## 验证约定

每批同时提交依赖版本、`Cargo.lock`、必要的 API 适配和回归测试。
预发布的 `wreq` / `wreq-util` 使用精确版本，避免其他批次解析锁文件时跨越 RC 的不兼容 API。
不修改数据库迁移历史、密文格式、签名输入或缓存键编码。

本地 `.cargo/config.toml` 使用 Cranelift；升级后的 AES 实现存在未支持的 intrinsic 警告，
因此此次验证显式使用 LLVM，不改变开发者的本地后端配置：

```bash
export CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm
export CARGO_PROFILE_TEST_CODEGEN_BACKEND=llvm
```

## 第 1 批：基础依赖

升级 AWS-LC、base64、bcrypt、Brotli、object_store、tokio-tungstenite、TOML、
tower-http、webpki-roots、Zstd 和 jemalloc。加密 API、Redis、SQLx、sysinfo、
wreq 的不兼容升级留给后续独立批次。

已通过：

- `cargo check --workspace --all-targets --locked --offline -j 2`
- `cargo check -p aether-gateway --lib --features jemalloc --locked --offline -j 2`
- `cargo test -p aether-http -p aether-data-schema --locked --offline -j 2`（27 项）
- `cargo test -p aether-data-postgres body_codec --lib --locked --offline -j 2`（12 项，涵盖 Zstd 格式、窗口和损坏帧）

以上是编译和定向回归验证，不代表所有网关端到端场景已经执行。
