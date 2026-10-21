# dsline 仓库工作约定

本文件是仓库级规范，权威细则见 [`docs/development.md`](docs/development.md)（开发与验证规范）。与 `ROADMAP.md` 冲突时以 ROADMAP 为准。

## 1. 验证：双通道（强制）

任何代码改动必须在 **Windows** 和 **WSL Linux** 两条通道都通过才算完成。

Windows 通道（必须用 conda 解释器）：

```bash
bash scripts/windows-test.sh all      # rust + python + verify
```

WSL 通道（单一入口，幂等）：

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-24.04 -- bash /mnt/c/Simpl/dsline/scripts/wsl-test.sh all
```

子命令：`setup` / `rust` / `python` / `verify` / `bench` / `all`。首次安装工具链需 `DSLINE_INSTALL=1`；网络慢时用脚本头部列出的镜像变量（本机实测官方 Rust CDN 约 31 KB/s，USTC 镜像约 9.8 MB/s）。

通过基线：Rust 单测 Windows 149 / Linux 140（差 9 项是 `#[cfg(windows)]` 专属）+ 5 doc test；Python 137 通过；`verify/check_invariants.py` 输出 `84 passed, 0 failed`（replay 段被跳过不算通过）。性能数字必须来自 release 构建（`scripts/*-test.sh bench` 已固定）。

为什么必须双通道：Linux 是一级平台（ROADMAP §13.1）；`MAP_SHARED` 无需 flush，传输层真实性能只能在 Linux 上取；`/proc` 存活探测、存储后端选择、区域目录等平台分支只有 Linux 能覆盖。

## 2. 硬性约束

- **不要**用 MSYS2 的 `python`（`/mingw64/bin`）：没有 dsline 扩展，且缺 `python3.lib`，会让 `cargo test` 链接失败。
- **不要**用 `pip install --break-system-packages` 污染系统 Python（Ubuntu 24.04 PEP 668）；用 `~/.venvs/dsline`。
- **WSL 命令写进脚本文件再执行**，不要用 `wsl.exe ... bash -c '...'`：Git Bash → `wsl.exe` → Linux 的引号与 `$VAR` 传递不可靠（实测变量会被吞成空值）。同理，`/mnt/...` 路径要配 `MSYS_NO_PATHCONV=1`。
- **区域布局格式为 3**（region header 32 字节，含 `pin_floor`）。改动布局或协议：同步更新对应 ADR、`README.md` 边界说明、`docs/README.md`、`CHANGELOG.md`，并清理陈旧区域（`dsline shm cleanup` 或 `rm -f "$XDG_RUNTIME_DIR"/dsline-*.bin`）。
- **核心保持轻量**：不引入 DataFusion 或重依赖（ROADMAP §10）；Python 慢路径用 `_py` 后缀命名。
- **unsafe 与 lint**：`unsafe_op_in_unsafe_fn` 是 deny；`unwrap_used`、`panic` 是 warn——不要新增告警。
- **零拷贝措辞**遵循 ROADMAP §24 模板：禁止"所有数据零拷贝""任意 Python 对象零拷贝""比所有队列都快""exactly-once"。零拷贝只对共享内存 `alloc/publish` 且满足条件的单槽消息成立，`send/recv` 对已有内存是单次拷贝。

## 3. 文档同步

改了协议、布局或平台行为时，更新：对应 ADR（`docs/adr/`）、`README.md` 边界清单、`CHANGELOG.md`、`PROGRESS.md`（**在顶部追加新条目**）。注意 `PROGRESS.md` 底部"基线报告"段落停留在 2026-05-16，其中的行数、测试数和"未实现"清单均已过期，不要照抄。

性能相关改动：在 WSL 通道跑 `scripts/wsl-test.sh bench`，把结果按平台与日期写入 `docs/benchmarks.md`。
