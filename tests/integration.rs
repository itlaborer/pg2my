//! 端到端集成测试：对真实 PostgreSQL / MySQL 跑四种迁移方向。
//!
//! 需要两个可建库的连接串，否则自动跳过（CI 里由 services 提供）：
//!   PG2MYSQL_IT_PG_URL=postgres://postgres@127.0.0.1:5432/postgres
//!   PG2MYSQL_IT_MYSQL_URL=mysql://root@127.0.0.1:3306/
//!
//! 运行：cargo test --test integration -- --nocapture

use std::process::Command;

use mysql_async::prelude::Queryable;

const SRC_PG: &str = "it_src_pg";
const DST_PG: &str = "it_dst_pg";
const SRC_MY: &str = "it_src_my";
const DST_MY: &str = "it_dst_my";
const ROWS: i64 = 200;

/// 把连接串的库名换成指定库（连接串里不要带查询参数）。
fn with_db(url: &str, db: &str) -> String {
    match url.rfind('/') {
        Some(i) => format!("{}/{db}", &url[..i]),
        None => format!("{url}/{db}"),
    }
}

async fn pg_client(url: &str) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("连接 PostgreSQL 失败");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}

async fn pg_exec(url: &str, sql: &str) {
    let (client, handle) = pg_client(url).await;
    if let Err(e) = client.batch_execute(sql).await {
        panic!("执行 PG SQL 失败: {e}\n{sql}");
    }
    drop(client);
    let _ = handle.await;
}

/// 返回 (行数, 非 NULL 数, 最小主键)。
async fn pg_query(url: &str, sql: &str) -> (i64, i64, i64) {
    let (client, handle) = pg_client(url).await;
    let row = client.query_one(sql, &[]).await.expect("PG 查询失败");
    let out = (row.get(0), row.get(1), row.get(2));
    drop(client);
    let _ = handle.await;
    out
}

async fn my_exec(url: &str, sql: &str) {
    let opts = mysql_async::Opts::from_url(url).expect("解析 MySQL 连接串失败");
    let mut conn = mysql_async::Conn::new(opts).await.expect("连接 MySQL 失败");
    conn.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("执行 MySQL SQL 失败: {e}\n{sql}"));
}

/// 返回 (行数, 非 NULL 数)。
async fn my_query(url: &str, sql: &str) -> (i64, i64) {
    let opts = mysql_async::Opts::from_url(url).expect("解析 MySQL 连接串失败");
    let mut conn = mysql_async::Conn::new(opts).await.expect("连接 MySQL 失败");
    conn.query_first::<(i64, i64), _>(sql)
        .await
        .expect("MySQL 查询失败")
        .expect("MySQL 查询无结果")
}

async fn run(args: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_pg2mysql"))
        .args(args)
        .output()
        .expect("启动 pg2mysql 失败");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pg2mysql 退出码 {:?}\n参数: {args:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    println!("{stdout}{stderr}");
}

/// 四种方向各跑一遍，校验行数、非 NULL 数与主键下界。
#[tokio::test]
async fn four_directions_roundtrip() {
    let (Ok(pg_url), Ok(my_url)) = (
        std::env::var("PG2MYSQL_IT_PG_URL"),
        std::env::var("PG2MYSQL_IT_MYSQL_URL"),
    ) else {
        eprintln!("跳过集成测试：未设置 PG2MYSQL_IT_PG_URL / PG2MYSQL_IT_MYSQL_URL");
        return;
    };

    for db in [SRC_PG, DST_PG] {
        pg_exec(&pg_url, &format!("DROP DATABASE IF EXISTS {db}")).await;
        pg_exec(&pg_url, &format!("CREATE DATABASE {db}")).await;
    }
    for db in [SRC_MY, DST_MY] {
        my_exec(&my_url, &format!("DROP DATABASE IF EXISTS {db}")).await;
        my_exec(&my_url, &format!("CREATE DATABASE {db}")).await;
    }

    let src_pg = with_db(&pg_url, SRC_PG);
    let dst_pg = with_db(&pg_url, DST_PG);
    let src_my = with_db(&my_url, SRC_MY);
    let dst_my = with_db(&my_url, DST_MY);

    // PG 源：数组、NULL、制表符/换行等需要转义的字符
    pg_exec(
        &src_pg,
        &format!(
            "CREATE TABLE it_users (
                 id int PRIMARY KEY,
                 name text,
                 score numeric(10,2),
                 tags text[],
                 created timestamptz
             );
             INSERT INTO it_users
             SELECT g,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE 'user-' || g || E'\\t\\\\\\n' END,
                    g * 1.5,
                    ARRAY['a', 'b'],
                    '2026-01-01 00:00:00+00'::timestamptz + g * interval '1 minute'
             FROM generate_series(1, {ROWS}) g;"
        ),
    )
    .await;

    // MySQL 源：用拼接 VALUES 造数据，避免依赖厂商特有的序列表
    let mut values = Vec::with_capacity(ROWS as usize);
    for g in 1..=ROWS {
        let name = if g % 7 == 0 {
            "NULL".to_string()
        } else {
            format!("'user-{g}'")
        };
        values.push(format!(
            "({g}, {name}, {:.2}, '2026-01-01 00:00:00')",
            g as f64 * 1.5
        ));
    }
    my_exec(
        &src_my,
        &format!(
            "CREATE TABLE it_users (
                 id int PRIMARY KEY,
                 name varchar(64),
                 score decimal(10,2),
                 created datetime(6)
             );
             INSERT INTO it_users VALUES {};",
            values.join(", ")
        ),
    )
    .await;

    let non_null = ROWS - ROWS / 7;

    // 1. PG → PG，走 COPY 直通
    run(&[
        "--source",
        &src_pg,
        "--target",
        &dst_pg,
        "--schema",
        "public",
        "--db",
        "public",
        "--truncate",
        "--copy",
    ])
    .await;
    assert_eq!(
        pg_query(
            &dst_pg,
            "SELECT count(*), count(name), min(id)::bigint FROM it_users"
        )
        .await,
        (ROWS, non_null, 1)
    );

    // 2. PG → MySQL，走逐行批量写入（text[] 转 JSON）
    run(&[
        "--source",
        &src_pg,
        "--target",
        &dst_my,
        "--schema",
        "public",
        "--truncate",
        "--batch-size",
        "64",
    ])
    .await;
    assert_eq!(
        my_query(&dst_my, "SELECT count(*), count(name) FROM it_users").await,
        (ROWS, non_null)
    );

    // 3. MySQL → MySQL，走主键分片
    run(&[
        "--source",
        &src_my,
        "--target",
        &dst_my,
        "--truncate",
        "--shards",
        "2",
    ])
    .await;
    assert_eq!(
        my_query(&dst_my, "SELECT count(*), count(name) FROM it_users").await,
        (ROWS, non_null)
    );

    // 4. MySQL → PG
    run(&[
        "--source",
        &src_my,
        "--target",
        &dst_pg,
        "--db",
        "public",
        "--truncate",
    ])
    .await;
    assert_eq!(
        pg_query(
            &dst_pg,
            "SELECT count(*), count(name), min(id)::bigint FROM it_users"
        )
        .await,
        (ROWS, non_null, 1)
    );
}
