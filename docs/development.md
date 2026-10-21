# 开发与验证规范

本文件是 `ROADMAP.md` 的配套规范，规定 **dsline 的改动必须在哪些环境、用哪些命令、达到什么标准才算验证通过**。

---

## 1. 双通道原则

| 通道 | 角色 | 说明 |
|---|---|---|
| Windows（开发主机） | 主开发环境 | 日常编辑、构建、快速回归 |
| WSL Ubuntu 24.04 | **Linux 验证通道（强制）** | 每个改动必须在 Linux 上再跑一遍 |

任何改动（含文档性改动以外的所有代码改动）在 Windows 和 WSL 两条通道都通过，才算验证完成。

必须双通道的原因：

1. **Linux 是一级平台**（ROADMAP §13.1）。Linux 是功能与性能的主平台，Windows 只是"暂缓/experimental"。
2. **Windows 的性能瓶颈在 Linux 不存在。** `docs/benchmarks.md` 测出 Windows 上 `FlushViewOfFile` 约 45–52 µs/次，是当前每消息成本的全部；Linux 的 `MAP_SHARED` 不需要 flush。**传输层的真实性能数字只能在 Linux 上取。**
3. **平台分支只有 Linux 能覆盖**：`crates/dsline-shm/src/process.rs` 的 `/proc/<pid>/stat` 存活探测（Windows 走 `GetProcessTimes`）、存储后端选择（Windows paging-file section vs 其他平台文件映射）、区域目录（`$XDG_RUNTIME_DIR` vs `%LOCALAPPDATA%`）。
4. 崩溃恢复、区域生命周期、`dsline shm` CLI 的语义在两个平台上并不等价（Windows 命名通道不留文件）。

---

## 2. 环境矩阵

| 环境 | 解释器 | 扩展安装 | 求解器依赖 | 状态 |
|---|---|---|---|---|
| Windows / conda | `C:\Users\86177\miniforge3\python.exe` (3.13.12) | `maturin develop` 到源码树 | z3 / ortools / pyscipopt 已装 | **可用，Windows 通道使用此解释器** |
| Windows / MSYS2 | `python` (3.14.7, `/mingw64/bin`) | — | 无 | **不可用**：无 dsline 扩展，且缺 `python3.lib`，`cargo test` 会在链接 `dsline-python` 测试二进制时失败 |
| WSL / Ubuntu 24.04 | `~/.venvs/dsline/bin/python` (3.12.3, `--system-site-packages`) | `maturin develop` | 继承系统 z3 / ortools / pyscipopt | **可用，Linux 通道** |

WSL 侧固定使用 `~/.venvs/dsline`（`--system-site-packages`，以便 `verify/check_invariants.py` 看到系统已装的求解器）。不要用 `pip install --break-system-packages` 污染系统 Python（PEP 668）。

---

## 3. 命令

### 3.1 Windows 通道

统一入口 `scripts/windows-test.sh`（Git Bash）：

```bash
bash scripts/windows-test.sh all      # rust + python + verify
bash scripts/windows-test.sh rust     # cargo fmt --check + cargo test --workspace
bash scripts/windows-test.sh python   # maturin develop + unittest discover
bash scripts/windows-test.sh verify   # check_invariants.py
```

脚本内部固定了两件容易踩空的事（见 §6）：`PYO3_PYTHON` 指向 conda 解释器，`CONDA_PREFIX` 指向 conda 环境根，并在构建后打印 `_dsline.pyd` 的时间戳。等价的手工命令：

```bash
export PYO3_PYTHON="C:/Users/86177/miniforge3/python.exe"
CONDA_PREFIX="C:/Users/86177/miniforge3" "$PYO3_PYTHON" -m maturin develop --manifest-path crates/dsline-python/Cargo.toml
PYO3_PYTHON="$PYO3_PYTHON" cargo fmt --all --check
PYO3_PYTHON="$PYO3_PYTHON" cargo test --workspace
"$PYO3_PYTHON" -m unittest discover -s tests/python
"$PYO3_PYTHON" verify/check_invariants.py
```

### 3.2 WSL / Linux 通道

统一入口 `scripts/wsl-test.sh`（幂等，可反复执行）：

```bash
# 从 Windows 发起（注意 MSYS_NO_PATHCONV=1，见 §6）
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 -- bash /mnt/c/Simpl/dsline/scripts/wsl-test.sh all

# 或先进入 WSL
wsl.exe -d Ubuntu-24.04
cd /mnt/c/Simpl/dsline
scripts/wsl-test.sh setup     # 安装/校验 rustup + venv + maturin（首次）
scripts/wsl-test.sh rust      # cargo fmt --check + cargo test --workspace
scripts/wsl-test.sh python    # maturin develop + unittest discover
scripts/wsl-test.sh verify    # check_invariants.py
scripts/wsl-test.sh bench     # benches/cross_process.py（Linux 上的真实性能数字）
scripts/wsl-test.sh all       # setup + rust + python + verify
```

首次安装工具链需要显式放行：`DSLINE_INSTALL=1 scripts/wsl-test.sh setup`。

可用环境变量覆盖：`DSLINE_REPO`、`DSLINE_VENV`、`DSLINE_CARGO_TARGET`、`DSLINE_INSTALL`。

### 3.3 网络慢时的镜像配置

本机实测：官方 Rust CDN（`static.rust-lang.org`）约 **31 KB/s**，USTC 镜像约 **9.8 MB/s**（差约 300 倍），因此首次安装工具链必须走镜像。

```bash
export DSLINE_INSTALL=1
export DSLINE_RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
export DSLINE_CARGO_MIRROR="sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
export DSLINE_PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
cd /mnt/c/Simpl/dsline && bash scripts/wsl-test.sh setup
```

- `DSLINE_RUSTUP_DIST_SERVER` / `DSLINE_RUSTUP_UPDATE_ROOT`：rustup 工具链与自举程序来源（默认官方）。
- `DSLINE_CARGO_MIRROR`：crates.io 稀疏索引镜像。**仅当 `~/.cargo/config.toml` 不存在时**写入；删掉该文件即回到官方源。
- `DSLINE_PIP_INDEX_URL`：pip 索引（仅用于安装 maturin）。

本机当前状态：已写入 `~/.cargo/config.toml`（USTC 稀疏索引），rustup 已通过 USTC 安装 stable 1.99.1。

---

## 4. 通过标准（当前基线）

| 检查 | 期望 | 说明 |
|---|---|---|
| `cargo fmt --all --check` | 无输出 | CI 第一个门槛 |
| `cargo test --workspace`（Windows） | 149 个单测 + 5 个 doc test 全过 | core 17 / ops 20 / pipeline 13 / shm 81 / transport 18（另有 1 个 `#[ignore]` 计时探针） |
| `cargo test --workspace`（Linux） | **140** 个单测 + 5 个 doc test 全过 | 比 Windows 少 9 项：`dsline-shm` 的 `#[cfg(windows)]` 专属测试（section 后端、句柄生命周期等） |
| `maturin develop` | 成功，且 `python/dsline/_dsline.pyd` 时间戳更新 | 见 §6 的环境要求；**时间戳没变就是没重建** |
| `unittest discover -s tests/python` | 137 通过，0 失败 | 含跨进程 spawn 与多线程测试 |
| `verify/check_invariants.py` | `84 passed, 0 failed` | Z3 + CP-SAT + SCIP + 真实实现 replay |
| `cargo test` 的编译告警 | 允许 dead_code / unused import 告警，不允许新增 | 现有告警见 §7 |

两平台测试数不同是预期的：`dsline-shm` 有一批 `#[cfg(windows)]` 测试（paging-file section 后端），Linux 上不编译。**反向也一样**：新增测试若只对某个平台有意义，必须用 `cfg` 显式标注，不能靠"某平台上恰好没跑"。

`verify/check_invariants.py` 的 replay 段需要已构建的扩展；扩展不可导入时该段会跳过并给出提示——**跳过不算通过**，Linux 与 Windows 都必须跑到完整 84 项。

新增测试必须同时进入两条通道：Rust 测试写在 crate 内（`#[cfg(test)]`），Python 测试写在 `tests/python/`，求解器检查追加到 `verify/check_invariants.py`。

---

## 5. 构建与文件系统约定

- **`CARGO_TARGET_DIR` 放在 Linux 侧**（默认 `~/.cache/dsline-target`）。`/mnt/c` 走 9p，Rust 构建慢约一个数量级；`scripts/wsl-test.sh` 已默认这样设置。
- **源码留在 `/mnt/c`**，不克隆到 Linux 家目录：WSL 通道必须测**当前工作树**（含未提交改动），而不是某个副本。
- 命名通道的区域文件在 Linux 侧落在 `$XDG_RUNTIME_DIR`（Ubuntu 下为 `/run/user/1000/`），Windows 侧命名通道走 paging-file section、不留文件。
- 区域布局格式版本为 4（region header 32 字节，含 `pin_floor`；帧版本 2，载荷校验由 FNV-1a 改为 CRC-32）。**升级构建后必须清掉陈旧区域**，否则 open 会被拒绝：`dsline shm cleanup --older-than 1h`，Linux 上也可直接 `rm -f "$XDG_RUNTIME_DIR"/dsline-*.bin`。

---

## 6. 已知陷阱

| 现象 | 原因 | 处理 |
|---|---|---|
| `maturin develop` 报 `Couldn't find a virtualenv or conda environment`，之后 Python 测试仍在跑 | 没有激活的环境；**失败被管道日志吞掉，测试实际跑的是上一次构建的扩展**（本仓库曾因此从 2026-10-02 起一直测旧 `.pyd`） | 用 `scripts/windows-test.sh`（已设 `CONDA_PREFIX`），或手工 `CONDA_PREFIX="C:/Users/86177/miniforge3" python -m maturin develop ...`；构建后核对 `python/dsline/_dsline.pyd` 的时间戳 |
| `cargo test` 链接失败 `LNK1181: 无法打开输入文件"python3.lib"` | pyo3 的构建脚本选中了 MSYS2 的 `python`，其 lib 目录没有 MSVC 可链接的 `python3.lib` | 设 `PYO3_PYTHON="C:/Users/86177/miniforge3/python.exe"` |
| `wsl.exe ... bash -lc '...'` 里的 `$VAR` 变空、命令报 `--version: command not found` | Git Bash → `wsl.exe` → Linux 的引号/变量传递不可靠 | 把命令写进脚本文件执行（`scripts/*.sh` 即为该约定的产物），或 `bash -c` 传不带 `$` 的字面命令 |
| `bash: C:/Program Files/Git/mnt/c/...: No such file or directory` | Git Bash 的 MSYS 路径转换改写了 `/mnt/c/...` | 命令前加 `MSYS_NO_PATHCONV=1` |
| `pip install maturin` 报 `externally-managed-environment` | Ubuntu 24.04 的 PEP 668 | 用 venv（`scripts/wsl-test.sh setup` 已处理） |
| `import dsline` 失败 / `DLL load failed` | 当前解释器没装扩展，或 `.pyd` 是别的解释器构建的 | 用 §2 表格里对应的解释器；Rust 侧改动后重跑 `maturin develop` |
| open 报区域格式不匹配 | 构建升级后遗留的旧格式区域 | 见 §5 清理陈旧区域 |

---

## 7. 现有告警与已知边界（不算失败，但不要增加）

- `cargo test` 的 `dsline-shm` 测试构建会报 unused import（`OpenOptions`、`Seek`、`Write`）与 `corrupt_checksum_for_test` / `overwrite_frame_seq_for_test` 的 dead_code 告警。
- `crates/dsline-shm/target/test-storage/` 下会累积测试用的 `.bin` 文件（`target/` 已在 `.gitignore` 内）。
- 零拷贝的既有边界：`alloc` 仅 SPSC、仅单槽、拒绝 `DropOldest`；Python 侧 `view.read()` 是显式拷贝（buffer protocol 视图待 pyo3 0.23 迁移）。详见 ADR-005 与 `docs/README.md`。
- 本仓库的 `python/dsline/_dsline.pyd` / `.pdb` 是 `maturin develop` 的构建产物，已被 `.gitignore` 忽略。

---

## 8. 提交前检查清单

1. `cargo fmt --all --check`（Windows 或 WSL 均可）
2. Windows 通道：`cargo test --workspace` + `unittest discover` + `verify/check_invariants.py`
3. WSL 通道：`scripts/wsl-test.sh all`
4. 涉及性能的改动：WSL 通道 `scripts/wsl-test.sh bench`，结果更新到 `docs/benchmarks.md`（注明平台、日期、机器）。**性能数字必须来自 release 构建**：`maturin develop` 默认 debug，debug 下 4 KiB 载荷的单次校验就要 ~16 µs，会把结论完全带偏；`scripts/wsl-test.sh bench` 已固定先构建 release（`DSLINE_RELEASE=1` 可用于 python 通道）。
5. 涉及区域布局 / 协议 / 平台行为的改动：更新对应 ADR 与 `README.md` 的边界说明，并在 `CHANGELOG.md`、`PROGRESS.md` 记录

---

## 9. 这条通道为什么是强制的（首次接入的实测证据）

2026-10-20 首次接入时，Linux 通道在几分钟内抓到 **3 个只有 Linux 才会暴露的缺陷**，而 Windows 通道全绿：

| 缺陷 | 性质 | 为什么 Windows 通道漏掉 |
|---|---|---|
| `process.rs` 的 `current_process_start_time()` 对 `Result<Option<u64>, ()>` 调用 `unwrap_or` | `dsline-shm` 在 Linux 上**完全无法编译** | 该函数在 `#[cfg(target_os = "linux")]` 分支内 |
| `lib.rs` 的 `slice.indices(size as i32)` | `dsline-python` 在 Linux 上无法编译 | `c_long` 在 Windows 上是 i32、Linux 上是 i64 |
| 文件后端 `create_or_open` 的创建窗口 | Unix 上并发首次 open 同一命名通道会偶发 `file storage is smaller than expected` | Windows 命名通道走 paging-file section，由 OS 保证原子创建，不存在该窗口 |

另一个背景事实：这些代码从未被 CI 编译过——本地 `main` 领先 `origin/main` 7 个提交（MPSC、Pipeline/CLI、崩溃恢复、分片 MPMC、零拷贝），CI 最后一次运行停在 2026-06-06。**双通道验证不能替代推送**：提交后要确认 CI 真的跑起来（`.github/workflows/ci.yml` 覆盖 ubuntu/macos/windows）。

同一轮还暴露了一个流程漏洞：**扩展静默陈旧**。Windows 上 `maturin develop` 因为找不到虚拟环境/conda 环境一直失败，而失败信息被管道日志吞掉，于是 Python 测试从 2026-10-02 起一直跑的是旧 `.pyd`——Rust 单测是新的，Python 测试却什么都没验证到。现在的 `scripts/windows-test.sh` 固定了环境并在构建后打印扩展时间戳；改动 Rust 侧后**务必确认时间戳变了**。

### 9.1 推送后的第一次 CI（2026-10-21，运行 #14）

7 个未推送提交 + 3 个新提交推送后，CI 第一次看到 9 月以来的全部代码：**ubuntu 3.12 与两个 Windows job 通过；ubuntu 3.11 与两个 macOS job 的 "Rust tests" 失败**。

- **macOS：确定的缺陷。** `process.rs` 的身份测试断言真实平台探测的结果，但只有 Windows 和 Linux 实现了探测（其他平台按 ROADMAP §6.2 的保守默认一律回 `Unknown`，从不自动回收）。这些测试已按平台门控，并新增一个测试固定"无探测平台一律 Unknown"——它正好在 CI 的 macOS job 上运行。
- **ubuntu 3.11：拥塞相关的超时。** 本机用 `taskset` 限到 2 核与 4 核各跑 20 次都无法复现（0/40），而 CI runner 只有 2–4 核且整套测试并行；压力测试与内部等待的预算因此放宽，但仍保持有界。
- 教训：**本机 12 核不会暴露的东西，CI 的 2–4 核会**。时序敏感的压力测试要给足预算，任何"永不失败"的路径都必须有有界失败兜底（有界失败也优于挂死）。
- 为让此类失败下次可诊断，CI 的 Rust/Python 测试步骤现在把失败测试名与 `error[...]` 以 `::error::` 注解输出（原始 job 日志需要仓库凭据，否则只看得到 "exit code 101"）。
