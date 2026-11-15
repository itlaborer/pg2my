# pg2mysql

[![CI](https://github.com/itlaborer/pg2my/actions/workflows/ci.yml/badge.svg)](https://github.com/itlaborer/pg2my/actions/workflows/ci.yml)

一个用 Rust 写的多方言数据库迁移工具，支持 **PostgreSQL → PostgreSQL**、**MySQL → MySQL**、**PostgreSQL → MySQL**、**MySQL → PostgreSQL** 四种方向。结构与数据一起搬，适合整库迁移、指定表同步和大表续搬。

## 特性

- **四种方向**：由 `--source` / `--target` 连接串自动判断，无需额外参数
- **结构迁移**：表、列、主键、索引、外键、注释、枚举、数组、JSON、生成列、自增/序列
- **库对象**：同方言迁移视图定义与 PG 独立序列（含当前位点）；跨方言视图会明确提示跳过
- **结构漂移自愈**：目标表已存在时自动补缺失的列和索引，列类型/非空不一致会提示，加 `--sync-schema` 直接对齐
- **外键容错**：存量脏数据导致外键建不上时，PG 自动降级为 `NOT VALID`、MySQL 临时关闭 `foreign_key_checks` 重试，不中断迁移
- **并行搬运**：`--parallel` 按表并行，每个任务独立连接
- **单表分片**：`--shards N` 按整数主键范围把一张大表拆成 N 片并发搬运（PG 源共享同一快照）
- **PG COPY 加速**：`--copy` 走 `COPY FROM STDIN`，比多行 INSERT 快约一个量级；PG → PG 时直接 `COPY TO STDOUT` → `COPY FROM STDIN` 字节流直通，不做逐行类型转换
- **一致性快照**：PG 用 `pg_export_snapshot()` + REPEATABLE READ，MySQL 用 `START TRANSACTION WITH CONSISTENT SNAPSHOT`，并行任务共享同一快照
- **行级断点续搬**：`--resume` 按单列主键做 keyset 分页，中断后从上次主键继续，不重不漏
- **增量同步**：`--cdc` 支持轮询水位与 MySQL binlog（ROW 格式），可捕获 INSERT/UPDATE/DELETE
- **安全**：密码可走 `PGPASSWORD` / `MYSQL_PWD`，避免出现在命令行和进程列表
- **可预演**：`--ddl-only` 只看 DDL，`--dry-run` 输出完整计划且不写目标库

## 编译

需要 Rust 1.85+（edition 2024）：

```bash
cargo build --release
# 产物：target/release/pg2mysql
```

如需在 CentOS 7 等老系统（glibc 2.17）上运行，建议编静态版本：

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
# 产物：target/x86_64-unknown-linux-musl/release/pg2mysql（static-pie，无外部依赖）
```

ARM64 把 target 换成 `aarch64-unknown-linux-musl` 即可。

## 测试

```bash
cargo test --bins          # 单元测试：类型映射、DDL 生成、默认值翻译等
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

集成测试会真实跑四种迁移方向，需要两个可建库的连接串，未设置则自动跳过：

```bash
PG2MYSQL_IT_PG_URL=postgres://postgres@127.0.0.1:5432/postgres \
PG2MYSQL_IT_MYSQL_URL=mysql://root@127.0.0.1:3306/ \
cargo test --test integration -- --nocapture
```

GitHub Actions 的 `integration` job 用 `postgres:13` + `mariadb:10.5` 服务容器运行同一套用例。

## 快速开始

```bash
# PostgreSQL → MySQL
pg2mysql \
  --source postgres://app:pass@10.0.0.1:5432/appdb \
  --target mysql://app:pass@10.0.0.2:3306/appdb

# 只迁两张表，先清空目标表，PG 目标用 COPY 加速，迁完校验行数
pg2mysql \
  --source postgres://app@10.0.0.1:5432/appdb \
  --target postgres://app@10.0.0.3:5432/appdb \
  --schema public --db public \
  --tables users,orders \
  --truncate --copy --verify --parallel 2
```

## 指定同步的表

| 参数 | 说明 | 默认 |
|---|---|---|
| `--tables` | 只迁这些表，逗号分隔，自动去空格 | 整库 |
| `--exclude-tables` | 排除这些表，优先级高于 `--tables` | 无 |
| `--schema` | 源命名空间（PG schema / MySQL 库名） | PG 取 `public`，MySQL 取 URL 中的库名 |
| `--db` | 目标命名空间 | 与源相同 |

目标库/模式不存在会自动创建（MySQL 建库使用 `utf8mb4`，PG 建 schema）。表名不做重命名，源表叫什么目标就叫什么。

> PG 中未加双引号创建的表名都是小写，`--tables` 需按实际存储的表名填写。

## 密码与认证

**方式 1：写在连接串里**（特殊字符需 URL 百分号编码，如 `@` → `%40`、`:` → `%3A`、`/` → `%2F`）

```bash
pg2mysql --source 'postgres://app:P%40ss@10.0.0.1:5432/appdb' ...
```

**方式 2：环境变量（推荐，不会进入 shell 历史与 `ps`）**

| 数据库 | 环境变量 |
|---|---|
| PostgreSQL | `PGPASSWORD` |
| MySQL | `MYSQL_PWD` |

```bash
read -rs PGPASSWORD; export PGPASSWORD
read -rs MYSQL_PWD;   export MYSQL_PWD

pg2mysql --source postgres://app@10.0.0.1:5432/appdb \
         --target mysql://app@10.0.0.2:3306/appdb
```

连接串已带密码时环境变量被忽略；跨方言迁移时两个变量可同时设置。

**方式 3：免密**，连接串只写用户名，适用于 `trust` / 空密码场景。

## 常用参数

| 参数 | 说明 | 默认 |
|---|---|---|
| `--batch-size` | 每批写入行数 | `1000` |
| `--parallel` | 并行搬运的表数 | `1` |
| `--shards` | 单表按整数主键范围拆成 N 片并发搬运，仅对单列整数主键生效 | `1` |
| `--truncate` | 写入前清空目标表 | 否 |
| `--copy` | PG 目标使用 COPY 批量装载（upsert 时自动退回 INSERT；PG → PG 为字节流直通） | 否 |
| `--verify` | 迁移完成后比对行数 | 否 |
| `--ddl-only` | 只打印目标方言 DDL | 否 |
| `--dry-run` | 打印 DDL 与数据搬运计划，不连目标库 | 否 |
| `--sync-schema` | 目标表已存在时把列类型/非空对齐成源定义，默认只打印差异 | 否 |
| `--where` | 附加到源查询的过滤条件（原始 SQL） | 无 |
| `--checkpoint` | 断点文件路径（记录已完成表、水位、binlog 位点） | 无 |
| `--resume` | 跳过已完成表，未完成的按主键续搬 | 否 |
| `--cdc` | 全量后持续增量同步 | 否 |
| `--cdc-mode` | `poll`（水位轮询）或 `binlog`（MySQL） | `poll` |
| `--cdc-column` | 水位列，默认自动探测 `updated_at` 等，找不到用主键 | 自动 |
| `--cdc-interval` | 轮询间隔（秒） | `5` |

## 典型场景

### 大表断点续搬

```bash
pg2mysql \
  --source postgres://app@10.0.0.1:5432/appdb \
  --target mysql://app@10.0.0.2:3306/appdb \
  --tables big_events \
  --batch-size 5000 --parallel 4 \
  --checkpoint /var/tmp/pg2mysql.json --resume
```

中断后重跑同一条命令即可：已完成表跳过，未完成表从上次主键继续。

### 单张超大表分片并行

```bash
pg2mysql \
  --source postgres://app@10.0.0.1:5432/appdb \
  --target mysql://app@10.0.0.2:3306/appdb \
  --tables big_events \
  --shards 8 --copy
```

`--shards 8` 会先查主键 `min/max`，把 `[min, max]` 均分成最多 8 段闭区间并发搬运，段与段之间不重不漏。要点：

- 只对**单列整数主键**（`smallint` / `int` / `bigint`）生效，其它表自动退回整表搬运
- 与 `--resume`、upsert 互斥：续搬与增量写入始终走单连接
- 可与 `--where` 叠加，分片条件会与过滤条件取交集
- 总连接数约为 `--parallel × --shards × 2`（源 + 目标各一条）
- PG 源各片共享导出快照，跨片时点一致；**MySQL 源各片独立开一致性事务，跨片时点可能不同**，需要严格一致时不要用分片

### 结构漂移对齐与库对象

```bash
# 目标库是旧结构：先看差异，确认后再加 --sync-schema 对齐
pg2mysql --source postgres://app@10.0.0.1:5432/appdb \
         --target postgres://app@10.0.0.3:5432/appdb --tables orders
pg2mysql --source postgres://app@10.0.0.1:5432/appdb \
         --target postgres://app@10.0.0.3:5432/appdb --tables orders --sync-schema
```

- 视图与 PG 独立序列随整库一起迁移（同样受 `--tables` 过滤），只在同方言之间重建，跨方言会提示跳过
- 视图互相依赖时自动多轮重试；`--sync-schema` 改列类型前会先删除待重建的视图，搬完自动恢复
- PG 序列位点取源库磁盘上的 `last_value`（含 cache 预留），目标不会重复发出源已分配过的值
- 枚举等自定义类型的列变更需要人工处理；存量数据违反外键时 PG 建为 `NOT VALID`，事后可 `VALIDATE CONSTRAINT`

### 只同步部分数据

```bash
--tables orders --where "created_at >= '2025-01-01' AND status <> 'deleted'" --truncate
```

`--where` 对本次选中的所有表生效，列名必须存在。带 `--where` 时不要同时开 `--verify`，因为校验比对的是整表行数。

### 增量同步

```bash
# MySQL binlog：需 binlog_format=ROW、binlog_row_image=FULL，并具备复制权限
pg2mysql \
  --source mysql://app@10.0.0.2:3306/appdb \
  --target mysql://app@10.0.0.3:3306/appdb \
  --tables users,orders \
  --cdc --cdc-mode binlog \
  --checkpoint /var/tmp/state.json

# 轮询水位：PG 源只能用它，且捕获不到 DELETE
--cdc --cdc-mode poll --cdc-column updated_at --cdc-interval 5
```

### 目标表已存在（结构漂移）

直接重跑即可，工具会补缺失的列与索引：

```bash
pg2mysql ... --tables users --truncate
#   补列 users  phone, email
#   补索引 users  idx_users_phone
```

不加 `--truncate` 重跑会触发主键冲突（全量模式不是 upsert），应使用 `--truncate` 全量重灌或 `--resume` 续搬。

## 权限要求

```sql
-- PG 源
GRANT CONNECT ON DATABASE appdb TO migrator;
GRANT USAGE ON SCHEMA public TO migrator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO migrator;

-- PG 目标（建 schema / 表 / 索引 / 外键）
GRANT CREATE ON DATABASE appdb TO migrator;

-- MySQL 源
GRANT SELECT ON appdb.* TO 'migrator'@'%';
-- binlog 增量另需
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'migrator'@'%';

-- MySQL 目标
GRANT CREATE, INSERT, UPDATE, DELETE, ALTER, INDEX, DROP
  ON appdb.* TO 'migrator'@'%';
```

## 已知限制

- PG 源增量同步仅支持轮询水位，暂未实现逻辑复制（`pgoutput`）
- `poll` 模式基于水位列，只能捕获新增/更新，无法捕获删除
- `--where` 只作用于全量搬运，不作用于 CDC
- binlog 模式要求 `binlog_format=ROW` 且 `binlog_row_image=FULL`，`PARTIAL_UPDATE_ROWS_EVENT` 会跳过并告警
- 生成列不搬数据，目标库按同一表达式重算
- PG 索引在数据装载完成后创建，以缩短导入时间

## 常见错误

| 报错 | 处理 |
|---|---|
| `password authentication failed` | 检查密码；含特殊字符时改用 `PGPASSWORD` / `MYSQL_PWD` |
| `duplicate key ...` / `Duplicate entry` | 目标已有数据，加 `--truncate` 或 `--resume` |
| `schema "xxx" does not exist` | 用 `--db` 指定目标 schema，或手工创建 |
| binlog 相关权限错误 | 授予复制权限并确认 ROW 格式 |

## License

[Apache License 2.0](LICENSE)

Copyright 2026 itlaborer

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
