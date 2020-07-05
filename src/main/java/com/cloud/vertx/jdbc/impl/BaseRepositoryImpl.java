package com.cloud.vertx.jdbc.impl;

import com.cloud.vertx.jdbc.BaseRepository;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

/**
 * jdbc基础操作统一接口实现类。
 *
 * @author zhoudingyun
 */
public class BaseRepositoryImpl implements BaseRepository {
    protected JDBCClient client;
    protected Vertx vertx;
    protected JsonObject config;

    /**
     * 构造方法。
     *
     * @param vertx  vertx
     * @param config config
     */
    public BaseRepositoryImpl(Vertx vertx, JsonObject config) {
        client = JDBCClient.create(vertx, config);
        this.vertx = vertx;
        this.config = config;
    }

    /**
     * A helper methods that generates async handler for SQLConnection
     *
     * @return generated handler
     */
    public <T> Handler<AsyncResult<SQLConnection>> connHandler(Handler<AsyncResult<T>> h1, Handler<SQLConnection> h2) {
        return conn -> {
            if (conn.succeeded()) {
                final SQLConnection connection = conn.result();
                h2.handle(connection);
            } else {
                h1.handle(Future.failedFuture(conn.cause()));
            }
        };
    }

    /**
     * 获取数据库连接。
     *
     * @return Future<SQLConnection>
     */
    @Override
    public Future<SQLConnection> getConnection() {
        Promise<SQLConnection> promise = Promise.promise();
        client.getConnection(conn -> {
            if (conn.succeeded()) {
                final SQLConnection connection = conn.result();
                promise.complete(connection);
            } else {
                promise.fail(conn.cause());
            }
        });
        return promise.future();
    }
}
