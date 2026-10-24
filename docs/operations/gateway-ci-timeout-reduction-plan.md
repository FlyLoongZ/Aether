# Test (Gateway) CI 耗时减负方案

- 文档日期：2026-09-23（Asia/Shanghai）
- 目标 job：`.github/workflows/rust-ci.yml` 中的 `test_gateway`（显示名 `Test (Gateway)`）
- 优化目标：**缩短 CI 耗时**，不减少安全/回归断言
- 关联审计：`docs/operations/system-slimming-audit-2026-09-08.md`
- **第一批（A1+A2+A4）已实施**，待 CI 前后对照确认分钟数。
- **冗余 CI 步骤清理已实施**：移除 Nightly 空 doctest、收敛重复的 data feature / adapter 矩阵，并清掉 Tunnel 遗留的测试参数与重复 `#[cfg(test)]` 标注（见 2.1、A1、B4、C3）。

本文只新增方案文档，不修改业务代码、测试或工作流。文中收益均为基于历史日志与源码结构的**估计值**，每批改动落地后须用同一提交做前后对照实测。

---

## 一、结论

`Test (Gateway)` 的耗时大头是**巨型单一 lib 测试目标的编译**（约 60%），其次是 **5300+ 测试的执行**（约 40%）。单纯减少“测试项数”对总分钟数帮助有限；应优先砍掉**重复编译指纹浪费**和**少数重型夹具**。

- 单 job 实测约 **11～17 分钟**，是普通 Rust CI 的关键路径。
- lib 测试约 5300 项、bins 约 78 项，全部 `#[cfg(test)]` 代码编进**同一个** rustc 调用，单进程峰值约 **7GB**，曾出现 OOM。
- 执行段历史波动 **214s～390s**；其中 4 个慢用例合计约 **54 秒**（约占快样本执行时间的 25%）。
- 仓库已有瘦身审计的立场不变：**减编译与运行重量，不先减安全断言**。

### 不做清单（铁律）

- 不删除 OAuth、配额、安全头、并发门禁、备份密码学、PII/断开结算等断言。
- 不下调 `PBKDF2_ITERATIONS`（`crates/aether-crypto/src/python_fernet.rs`）。
- 不把关键测试标 `ignore`，不跨测试共享可变 `AppState`。
- 暂不做 nextest 多 runner 分片（每个 runner 各自重编 7GB 巨型目标会净亏；须先复用同一次构建产物再评估）。

---

## 二、现状基线

### 2.1 Job 做什么

`test_gateway` 关键步骤（下表为 **A1/A2/A4 落地前** 的基线，保留用于对照历史耗时）：

| 步骤 | 基线配置 | 现状 |
| --- | --- | --- |
| Rust cache | `shared-key: rust-ci-${{ runner.os }}`（与 Data/Rest/Integration 等共用） | 已拆为 `rust-ci-gateway-test-${{ runner.os }}` |
| Setup mold | 仅此 job 安装 mold | 不变 |
| Test lib | `cargo nextest run -p aether-gateway --lib` | 不变 |
| Test bins | `cargo nextest run -p aether-gateway --bins` | 不变 |
| Test integration targets | 无 | 新增，覆盖 `architecture_guard` 与 `admin_unsigned_identity_headers` |

- 构建指纹：基线把 `RUSTC_WRAPPER=sccache`、`RUST_MIN_STACK=16777216`、`RUSTFLAGS: "-C link-arg=-fuse-ld=mold"` 放在 **step 级**；A2 已统一上移到 **job 级 `env`**。
- Workflow 级 env：`CARGO_INCREMENTAL=0`、`CARGO_PROFILE_DEV_DEBUG=0`、`CARGO_PROFILE_TEST_DEBUG=0`。
- nextest 配置：`.config/nextest.toml` 只固定 `slow-timeout`（60s 标记慢测试、连续两轮未结束即终止），**不固定** `test-threads`，CI 仍用 nextest 默认并行度。
- 覆盖缺口已补齐：基线时两条 nextest 命令只覆盖 `--lib` 与 `--bins`，`tests/admin_unsigned_identity_headers.rs` 不在 CI 中；A4 已新增 integration 步骤执行它。

### 2.2 耗时拆分（历史日志）

来源：`docs/operations/system-slimming-audit-2026-09-08.md`。

| 运行 | Gateway lib 编译 | Gateway lib 执行 | Test lib 步骤合计 |
| --- | --- | --- | --- |
| `34174603131` | 4:57（297s） | **213.807s**（5,139 项） | 514s |
| `34153166516` | 6:22（382s） | **390.039s** | — |

同轮 bins：编译约 2:27～3:05，执行仅 **0.27～0.33s**（78 项）。

- 编译 : 执行 ≈ **57:43**（快样本）～ **49:51**（慢样本）。
- 合并 `--lib --bins` 为一条命令**不能**省掉普通库与 `cfg(test)` 测试库的两种构建。

### 2.3 测试规模（静态统计）

范围：`apps/aether-gateway`。

| 分区 | `#[test]` / `#[tokio::test]` 数量 |
| --- | --- |
| `src/handlers` 内联 | 1233 |
| `src/tests/` 测试树（其中 control 700、frontdoor 224） | 1118 |
| `src/execution_runtime` 内联 | 576 |
| `src/control` 内联 | 461 |
| `src/ai_serving` 内联 | 301 |
| `src/main.rs` + `src/bin/*`（bins 目标） | 69 |
| 其余分散内联 | ~1322 |
| **合计** | **5080** |

数据为 2026-10-24 静态统计（`rg -c '#\[(tokio::)?test(\([^)]*\))?\]'`），与 nextest 实测条数差异来自 cfg 门控口径。

架构守卫：已迁出 lib，落在独立集成目标 `apps/aether-gateway/tests/architecture_guard.rs`，现为 **1 个文件、468 行、16 个 `#[test]`**（迁出时的 208 项字符串断言已按「只保留编译器无法表达的检查」合并）。断言全部是 `fs::read_to_string` + 字符串/`Cargo.toml` 规则检查，**不依赖 gateway 私有类型**。CI 由 `Test integration targets` 步骤执行。

### 2.4 编译为何是单点瓶颈

- `apps/aether-gateway/src/lib.rs` 的 `pub(crate) mod tests;` 把整棵 `tests/` 树挂进**同一个** lib test binary。
- `src/tests/` 现为 140 个文件、约 14.5 万行；连同业务代码，单 rustc 调用编译约 53 万行。
- 历史记录（`openai-responses-websocket-plan.md`）：单进程 rustc 峰值约 **7GB**，极端时 RSS 8.9GB 并触发 `oom_kill`；`-j` / `--test-threads` **不影响**该单进程峰值。
- 对照实验：临时裁掉无关测试模块后，编译可降至约 3 分钟内且零 OOM——证明“编译面”而非“并行度”是主因。
- 本地 `RUSTC_WRAPPER=sccache` 命中率曾低至约 1.5%；CI 样本 sccache 命中约 95% 仍要数分钟——剩余问题在 **crate-type 不可缓存调用与 RUSTFLAGS 指纹不一致**，不是“再加一层缓存”。

### 2.5 执行段慢点（已实测或源码可证）

| 优先级 | 模式 | 位置 | 估计可省 |
| --- | --- | --- | --- |
| 1【已实施】 | 池调度超大夹具（1700/2048 账号，每 key 真实 `seal_provider_catalog_key_api_key`） | `src/dispatch/pool_scheduler/tests.rs` 的 `large_pool_fixture`；现留空凭据，只验证扫描/跳过/游标语义 | **30～50s**（Top4 中 3 项） |
| 2【已实施】 | 备份 v1 兼容 + 17 个 historical 密钥各走 10 万次 PBKDF2 | `src/backup/executor.rs`；17 个计数用密钥已改为直接 base64 直接密钥，跳过 PBKDF2 | **10～15s**（与上表 14.3s 重叠） |
| 3 | 每测试独立进程重复付 DEVELOPMENT_ENCRYPTION_KEY 的 PBKDF2（nextest 无法跨用例共享缓存） | 全包大量 `encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, …)` | **10～25s**（非密码学夹具改直接密钥后） |
| 4 | 真实 `start_server` 起服极多（tests 树约 1794 次）+ `AppState::new()`（tests 树约 816 次） | `src/tests/mod.rs:33-47` 等 | **15～40s**（改 oneshot 仅限纯路由断言） |
| 5 | 固定 `sleep(100ms)` 扇出、个别 mock 内 5s/30s sleep | `tests/ai_execute/**`、`stream_pump.rs` 等 | **5～20s** |
| 6 | 架构守卫重复全树扫描（迁出后不再计入 lib） | `tests/architecture_guard.rs` | 执行 **~3-10s** + 编译见阶段 A |

**执行优化硬顶**：即使执行砍掉一半，关键路径大约只省 **40～80 秒**；样本间 214s vs 390s 的抖动本身就 ±176s。因此**编译侧（阶段 A/C）才是分钟级收益来源**。

---

## 三、分阶段方案

### 阶段 A：低风险、优先实施（预计单 job 省 1～3 分钟）

#### A1. 架构守卫迁出独立测试目标【已实施】

- **改动**：`src/tests/architecture/**`（208 项 / 约 1.5 万行）已迁出 `src/tests`，落到独立集成目标 `apps/aether-gateway/tests/architecture_guard.rs`，并从 `src/tests/mod.rs` 移除 `mod architecture;`。迁出后进一步合并为 **单文件 16 项 / 468 行**，只保留 SQL 归属、敏感日志字段、依赖分层、已移除组件等编译器无法表达的检查。
- **为什么省**：从巨型 lib `cfg(test)` 编译单元削掉约 15k 行，压低 rustc 7GB 峰值，降低 OOM 与编译墙钟；对照实验表明裁测试面可明显缩短编译。
- **收益估计**：编译 **-20～60s**，执行 **-数秒**；OOM 风险显著下降。
- **风险**：低。断言逻辑一字不改，仅更换宿主；helper 已改为 `pub(crate)`。
- **CI**：`test_gateway` 新增 `Test integration targets`：`cargo nextest run -p aether-gateway --tests`（同时覆盖 A4 的 `admin_unsigned_identity_headers`）。
- **验收**：架构 16 项 + 安全 1 项在新 target 全绿；lib 测试数相应减少；lib 编译时间与 rustc 峰值待 CI 对照。

#### A2. 统一构建指纹（RUSTFLAGS 提到 job 级）【已实施】

- **改动**：
  1. `test_gateway` 的 mold `RUSTFLAGS`、`RUST_MIN_STACK`、`RUSTC_WRAPPER` 已上移至 **job 级 `env`**，lib/bins/integration 三步共用同一指纹。
  2. **不钉工具链版本**：仓库已删除 `rust-toolchain.toml`，`rust-ci.yml` / `nightly.yml` / `release.yml` 全部改用最新 stable（`dtolnay/rust-toolchain` 不传 `toolchain:` 输入）。缓存隔离交给 rust-cache：它默认把 `rustc -vV` 哈希进缓存 key，stable 升级会自然换 key，不需要人工同步版本号。
  3. `test_gateway` 的 `shared-key` 改为 `rust-ci-gateway-test-${{ runner.os }}`，避免 mold 指纹与无 mold 的 job 互相污染共享缓存（一次冷缓存成本）；`nightly` 的 `shared-key` 也不再写入版本号。
- **为什么省**：原先 step 级 RUSTFLAGS + 十余个 job 共用同一 cache key，造成“看似命中、实际重编”，放大 4:57 vs 6:22 的波动。
- **收益估计**：热缓存编译 **-1～2 分钟波动收窄**；gateway 独立 cache key 后与 Rest/Data 不再争抢/覆盖。
- **风险**：低。mold 本就在用；独立 key 首次为冷缓存。浮动 stable 意味着小版本升级时会全量重编一次（rust-cache key 随 rustc 版本变），属预期行为；如需可复现性可用环境变量临时钉版本，但不写回 workflow。
- **验收**：连续多次 run 的 lib 编译时间方差下降；workflow 中不再出现 `toolchain: 1.95.0` 之类的手工版本号。

#### A3.（可选，紧随 A2）按用途拆分 rust-cache key

- **改动**：Gateway **test** 与 lint/check 类 job 不再共用同一 `shared-key`；或评估 `cache-workspace-crates: true`。
- **收益估计**：编译 **-30～90s**（估计）。
- **风险**：中。缓存体积上升；勿为每个细碎目标无限新建 key，勿直接缓存完整巨型 `target/`。

#### A4. 补跑漏掉的安全集成测试【已实施】

- **改动**：`test_gateway` 增加 `Test integration targets`：`cargo nextest run -p aether-gateway --tests`，覆盖 `apps/aether-gateway/tests/` 下全部目标（`architecture_guard` + `admin_unsigned_identity_headers`）。
- **收益**：时长 **+10～30s**，换取已确认的覆盖缺口（安全优先，与 A1 同 PR）。
- **风险**：低。该测试走公开 API。本地已实测 1/1 passed。

### 阶段 B：只动测试代码（预计执行段省 40～70 秒）

#### B1. 池调度大夹具轻量化【已实施】

- **改动**：`large_pool_fixture` 生产的 key 改为留空凭据（`encrypted_api_key = None`），使纯调度用例不再为每个 key 支付 Fernet 加解密；扫描预算、跳过计数、分页/游标语义断言与 1700/2048 规模边界全部保留。
- **收益估计**：执行 **-30～50s**。
- **风险**：中。不得把池规模缩到失去原回归条件；不得删断言。

#### B2. PBKDF2 夹具去重【已实施（加密侧夹具）】

- **改动**：对**不测密码学语义**的夹具，改用直接 32 字节 base64 密钥（`decode_direct_fernet_key` 路径），绕开每进程 10 万次迭代。
  - `apps/aether-gateway/src/tests/support.rs` 定义 `DEV_FERNET_KEY`（由 `DEVELOPMENT_ENCRYPTION_KEY` 预派生），单元测试 `dev_fernet_key_is_the_derived_development_key` 保证常量与生产派生一致；`src/tests/**` 以及 17 个模块内测试（`orchestration/effects`、`state/oauth`、`execution_runtime/stream/execution`、`handlers/shared/system_config_values` 等）统一改用该常量。
  - 验收口径：`rg 'encrypt_python_fernet_plaintext\(DEVELOPMENT_ENCRYPTION_KEY' apps/aether-gateway/src` 只剩 `src/testkit.rs` 与 `monitoring/test_support.rs` 两个**非测试门控**模块（生产/`testkit` feature 构建也会编译，无法引用 `crate::tests::DEV_FERNET_KEY`），其余用例已不再同时支付加密+解密两次派生。
  - `src/backup/executor.rs` 中「legacy 候选 >16」的 17 个计数用密钥改为直接 base64 密钥，跳过 PBKDF2。
- **仍保留**：
  - `backup/*`、`python_fernet` 等**备份/密码学语义**用例继续用口令形式，它们验证的正是派生与 historical 兼容路径。
  - 非测试门控的辅助模块（`src/testkit.rs`、`monitoring/test_support.rs`）无法引用 `crate::tests::DEV_FERNET_KEY`（生产构建也会编译），保持原样；它们只是状态配置，每进程只派生一次。
- **收益估计**：执行 **-10～25s**。
- **风险**：中。**绝不降低迭代次数**；`DEV_FERNET_KEY` 只用于夹具，不进入生产路径。

#### B3.（可选，后置）起服与 helper 瘦身

- 纯鉴权/路由断言：`start_server` → `tower` oneshot（已有 `send_request`）；**涉及超时、连接、完整中间件链的用例保留真实 server**。
- 合并 8+ 份同构 `run_*_test` 大栈 helper 为单一 helper；OAuth/keys/quota 同构用例可数据驱动，**表内逐行保留断言**。
- 固定 `sleep` 改为 channel/notify 事件驱动，**不删除等待本身**（防 flaky）。
- **收益估计**：执行 **-15～40s**；改动面大于 B1/B2，单独 PR。

#### B4. nextest 稳定性配置（非直接减分钟）【已实施】

- `.config/nextest.toml` 只保留 `slow-timeout = { period = "60s", terminate-after = 2, grace-period = "10s" }`，用于防止单个挂起用例拖满整个 job。
- **不固定** `test-threads`：nextest 默认按 num-cpus 并发，写死数值会在更大规格 runner 上反而压低并发。
- **不要**为提速下调 `RUST_MIN_STACK`（16MB 为深栈/管理面用例正确性所需）。

### 阶段 C：流水线级（主要缩短累计 runner，间接稳定 Gateway 缓存）

| # | 改动 | 收益 | 风险 |
| --- | --- | --- | --- |
| C1【已完成】 | **移除 Tunnel 组件**（`apps/aether-tunnel`、`crates/aether-gateway/tunnel`、`src/tunnel` 均已删除，端到端场景迁入 integration package） | Rest/Clippy 不再重复编 Gateway → 流水线累计约 **-8～10 分钟 runner**；共享缓存不再被污染 | 已由 `architecture_guard::tunnel_feature_is_removed` 守卫，禁止回流 |
| C2 | **路径过滤分层**（`rust-ci.yml` push/PR paths）：README、安装脚本、Compose、部分 `tests/*.sh` 不再触发全量 Rust 编译 | 非 Rust 变更 **整段跳过 Test (Gateway)**；须保留稳定 gate 防止 required check 永久 pending；补 `Cargo.toml`/`Cargo.lock`/`.cargo/**` 等触发项 | 中 |
| C3【已完成】 | Nightly 空 doctest、重复 adapter/feature job 治理 | 全仓无 doctest，删除 Nightly `cargo test --doc` 步骤；`check_data_features` 的 `postgres`/`all-drivers` 与 `test_data_adapters` 的单元素矩阵均收敛为单次构建 | 低 |

---

## 四、实施顺序与验收

| 批次 | 内容 | 预期（估计） | 验收标准 |
| --- | --- | --- | --- |
| **第一批 PR** | A1 架构守卫迁出 + A2 指纹统一 + A4 补安全集成测试【已实施】 | 单 job **-1～2 分钟** + 覆盖补齐 | 架构 16 + 安全 1 全绿；CI 对照编译时间下降 |
| **第二批 PR** | 冗余 CI 步骤清理：Nightly 空 doctest、重复 feature/adapter 矩阵、Tunnel 遗留测试参数、重复 `#[cfg(test)]`【已实施】 | 流水线约 **-2～4 分钟** runner | 无断言减少；`cargo check`/`nextest` 命令与清理前等价 |
| **第三批 PR** | B1 池调度夹具 + B2 PBKDF2 去重 | 执行 **-40～70s** | 断言集合不减；4 个历史慢用例计时明显下降；密码学用例仍为生产强度 |
| **第四批 PR** | A3 拆缓存 key + C2 路径过滤 | 缓存更稳 + 流水线累计大幅下降 | 无关路径 PR 不再拉起全量编译；Gateway 编译方差下降 |
| **可选** | B3 起服/helper | 再 **-15～40s** + 稳定性 | 无新增 flaky；慢测试超时有告警 |

### 对照方法（每批必做）

1. 固定同一 SHA、runner 规格、工具链与 feature 集合。
2. 记录：`Test lib` / `Test bins` 的**编译墙钟**、**执行墙钟**、测试总数、失败数。
3. 记录 rustc 峰值内存（如有）与 sccache 命中率。
4. 区分**冷缓存 / 热缓存**，区分 **job 耗时 / 流水线总耗时**。
5. **测试总数只允许**因 A1 迁移而在 lib 与新 target 之间搬家；禁止静默减少断言。

### 成功指标

- **首要**：普通 PR 上 `Test (Gateway)` 墙钟时间下降且结果稳定（方差收窄）。
- **次要**：全流水线累计 runner 分钟下降（阶段 C）。
- **禁止**把“删除测试数量”当作成功标准。

---

## 五、可重复的只读核查命令

```sh
# 测试属性数量（lib 树 / bins）
rg -c '#\[(tokio::)?test\]' apps/aether-gateway/src -g '*.rs' | awk -F: '{s+=$2} END {print s}'
rg -c '#\[(tokio::)?test\]' apps/aether-gateway/src/main.rs apps/aether-gateway/src/bin -g '*.rs'

# 架构守卫规模（迁移并合并后）
rg -c '#\[(tokio::)?test(\([^)]*\))?\]' apps/aether-gateway/tests/architecture_guard.rs
wc -l apps/aether-gateway/tests/architecture_guard.rs

# CI 集成测试步骤与指纹
rg -n 'Test integration targets|rust-ci-gateway-test|RUSTFLAGS' .github/workflows/rust-ci.yml

# 工具链不钉版本（应为空）
rg -n 'toolchain: 1\.' .github/workflows/

# nextest 稳定性配置
cat .config/nextest.toml

# Tunnel 组件不得回流
ls apps/aether-tunnel 2>/dev/null || echo 'tunnel removed'

# CI 中 mold/RUSTFLAGS/缓存 key
rg -n 'mold|RUSTFLAGS|shared-key|nextest run -p aether-gateway' .github/workflows/rust-ci.yml
```

历史耗时与慢用例计时以 `docs/operations/system-slimming-audit-2026-09-08.md` 及对应 GitHub Actions 运行 ID 为准；临时 API JSON 不入库。

---

## 六、风险与回滚

| 风险 | 缓解 |
| --- | --- |
| 迁出架构守卫后漏挂模块 | 迁移前后对比检查清单；CI 显式跑新 target |
| RUSTFLAGS 上移后某 job 链接失败 | 先在 `test_gateway` job 级验证，再推广到其它 job；保留 step 级回滚 diff |
| 夹具轻量化导致规模回归失效 | 强制保留 ≥1 条大规模边界用例；PR 中 diff 审查断言 |
| 路径过滤导致 required check 永久 pending | 增加始终运行的轻量 gate job |
| Tunnel 组件回流 | `architecture_guard::tunnel_feature_is_removed` 在 integration target 中持续守卫 |

回滚单位按 **PR 批次**：每批独立可 revert，不把 A/B/C 混在同一提交。

---

## 七、附录：明确不能减的测试（摘录）

| 类别 | 主要位置（约） | 说明 |
| --- | --- | --- |
| OAuth 导入/刷新/吊销 | `src/tests/control/admin/oauth/` 等 | 账户安全核心 |
| 配额与失效删 key | `src/tests/control/admin/endpoints/quota.rs` 等 | 计费与授权正确性 |
| 安全头 / 管理面访问 | `security.rs`、`health_access.rs`、`operational_auth` | 未签名身份头等 |
| 并发门禁 | `src/tests/concurrency.rs` | 过载拒绝与独立准入 |
| 备份历史兼容与密码学 | `src/backup/executor.rs` | 保留生产强度 PBKDF2 |
| 池调度扫描预算语义 | `src/dispatch/pool_scheduler.rs` | 可改夹具实现，不可删语义断言 |
| AI 断开结算 / PII | `src/tests/ai_execute/...` | 计费完整性与隐私 |

> 与 `system-slimming-audit-2026-09-08.md` 一致：首要结果是 **PR 更快得到正确反馈**，不是测试条数变少。
