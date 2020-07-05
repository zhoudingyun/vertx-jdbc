package com.cloud.vertx.jdbc.sql;

import io.vertx.core.json.JsonArray;

import java.util.Map;

/**
 * sql生成接口。
 *
 * @author zhoudingun
 */
public interface SqlBuilder {
    public SqlBuilder insert();

    public SqlBuilder into(String table);

    public SqlBuilder values(String... columns);

    public SqlBuilder values(Map<String, String> args, String... columns);

    public SqlBuilder update(String table);

    public SqlBuilder set(String... columns);

    public SqlBuilder select(String... columns);

    public SqlBuilder select(JsonArray columns);

    public SqlBuilder delete();

    public SqlBuilder from(String table);

    public SqlBuilder join(String table);

    public SqlBuilder on(String... conditions);

    public SqlBuilder where(String... conditions);

    public SqlBuilder groupBy(String... columns);

    public SqlBuilder having(String... conditions);

    public SqlBuilder orderBy(String... columns);

    public SqlBuilder limit(int limit);

    public SqlBuilder offset(int offset);
}
