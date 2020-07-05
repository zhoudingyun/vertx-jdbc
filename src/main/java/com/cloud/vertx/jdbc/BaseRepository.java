package com.cloud.vertx.jdbc;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.List;

/**
 * jdbc基础操作统一接口。
 *
 * @author zhoudingyun
 */
public interface BaseRepository {

    /**
     * 获取数据库连接。
     *
     * @return Future<SQLConnection>
     */
    Future<SQLConnection> getConnection();

    /**
     * 开始事务。
     *
     * @param connection 数据库连接
     * @return Future<AsyncResult>
     */
    default Future<AsyncResult> startTransaction(SQLConnection connection) {
        Promise<AsyncResult> promise = Promise.promise();
        connection.setAutoCommit(false, voidAsyncResult -> {
            if (voidAsyncResult.succeeded()) {
                promise.complete(Future.succeededFuture());
            } else {
                // 关闭 connection
                connection.close();
                promise.fail(voidAsyncResult.cause());
            }
        });

        return promise.future();
    }

    /**
     * 提交事务.
     *
     * @param connection 数据库连接
     * @param commit     提交事务标志 ture：提交 false：回滚
     * @return Future<Void>
     */
    default Future<Void> endTransaction(SQLConnection connection, boolean commit) {
        if (commit) {
            return commit(connection);
        }

        return rollback(connection);
    }

    /**
     * 提交事务。
     *
     * @param connection 数据库连接
     * @return Future<Void>
     */
    default Future<Void> commit(SQLConnection connection) {
        Promise<Void> promise = Promise.promise();
        if (connection != null) {
            connection.commit(voidAsyncResult -> {
                if (voidAsyncResult.succeeded()) {
                    promise.complete();
                } else {
                    connection.rollback(ar -> {
                        if (ar.succeeded()) {
                            promise.fail(ar.cause());
                        } else {
                            RuntimeException runtimeException = new RuntimeException(voidAsyncResult.cause().getMessage(), ar.cause());
                            promise.fail(runtimeException);
                        }
                    });
                }
                connection.close();
            });
        }
        return promise.future();
    }

    /**
     * 回滚事务。
     *
     * @param connection 数据库连接
     * @return Future<Void>
     */
    default Future<Void> rollback(SQLConnection connection) {
        Promise<Void> promise = Promise.promise();
        if (connection != null) {
            connection.rollback(voidAsyncResult -> {
                if (voidAsyncResult.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(voidAsyncResult.cause());
                }
                connection.close();
            });
        }
        return promise.future();
    }

    /**
     * 执行 ddl语句。
     *
     * @param connection 数据库连接
     * @param sql        ddl语句
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository execute(SQLConnection connection, String sql, Handler<AsyncResult<Void>> handler) {
        connection.execute(sql, handler);
        return this;
    }

    /**
     * 执行 ddl语句。
     *
     * @param connection 数据库连接
     * @param sql        ddl语句
     * @return Future<Void>
     */
    default Future<Void> execute(SQLConnection connection, String sql) {
        Promise<Void> promise = Promise.promise();
        execute(connection, sql, promise);
        return promise.future();
    }

    /**
     * 查询多条记录。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository query(SQLConnection connection, String sql, Handler<AsyncResult<ResultSet>> handler) {
        connection.query(sql, handler);
        return this;
    }

    /**
     * 查询多条记录。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @return Future<ResultSet>
     */
    default Future<ResultSet> query(SQLConnection connection, String sql) {
        Promise<ResultSet> promise = Promise.promise();
        query(connection, sql, promise);
        return promise.future();
    }

    /**
     * 查询数据流。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository queryStream(SQLConnection connection, String sql, Handler<AsyncResult<SQLRowStream>> handler) {
        connection.queryStream(sql, handler);
        return this;
    }

    /**
     * 查询数据流。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @return Future<SQLRowStream>
     */
    default Future<SQLRowStream> queryStream(SQLConnection connection, String sql) {
        Promise<SQLRowStream> promise = Promise.promise();
        queryStream(connection, sql, promise);
        return promise.future();
    }

    /**
     * 根据参数查询多条记录。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository queryWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<ResultSet>> handler) {
        connection.queryWithParams(sql, arguments, handler);
        return this;
    }

    /**
     * 根据参数查询多条记录。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @return Future<ResultSet>
     */
    default Future<ResultSet> queryWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<ResultSet> promise = Promise.promise();
        queryWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 根据参数查询数据流。
     * select * from user where name='张三'
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =?
     * @param arguments  参数 -> ['张三']
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository queryStreamWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<SQLRowStream>> handler) {
        connection.queryStreamWithParams(sql, arguments, handler);
        return this;
    }

    /**
     * 根据参数查询数据流。
     * select * from user where name='张三'
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =?
     * @param arguments  参数 -> ['张三']
     * @return Future<SQLRowStream>
     */
    default Future<SQLRowStream> queryStreamWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<SQLRowStream> promise = Promise.promise();
        queryStreamWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 查询单条记录。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository querySingle(SQLConnection connection, String sql, Handler<AsyncResult<JsonArray>> handler) {
        connection.querySingle(sql, handler);
        return this;
    }

    /**
     * 查询单条记录。
     * select * from user where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where id=1
     * @return Future<JsonArray>
     */
    default Future<JsonArray> querySingle(SQLConnection connection, String sql) {
        Promise<JsonArray> promise = Promise.promise();
        querySingle(connection, sql, promise);
        return promise.future();
    }

    /**
     * 根据参数查询单条记录。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository querySingleWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<JsonArray>> handler) {
        connection.querySingleWithParams(sql, arguments, handler);
        return this;
    }

    /**
     * 根据参数查询单条记录。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @return Future<JsonArray>
     */
    default Future<JsonArray> querySingleWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<JsonArray> promise = Promise.promise();
        querySingleWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三' where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 ->  update user set name='张三' where id=1
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository update(SQLConnection connection, String sql, Handler<AsyncResult<UpdateResult>> handler) {
        connection.update(sql, handler);
        return this;
    }

    /**
     * 修改。
     * update user set name='张三' where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 ->  update user set name='张三' where id=1
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> update(SQLConnection connection, String sql) {
        Promise<UpdateResult> promise = Promise.promise();
        update(connection, sql, promise);
        return promise.future();
    }

    /**
     * 根据参数修改。
     * update user set name='张三' where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> update user set name=? where id=?
     * @param arguments  参数 -> ['张三', 1]
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository updateWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<UpdateResult>> handler) {
        connection.updateWithParams(sql, arguments, handler);
        return this;
    }

    /**
     * 根据参数修改。
     * update user set name='张三' where id=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> update user set name=? where id=?
     * @param arguments  参数 -> ['张三', 1]
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> updateWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<UpdateResult> promise = Promise.promise();
        updateWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 执行sql。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository call(SQLConnection connection, String sql, Handler<AsyncResult<ResultSet>> handler) {
        connection.call(sql, handler);
        return this;
    }

    /**
     * 执行sql。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @return BaseRepository
     */
    default Future<ResultSet> call(SQLConnection connection, String sql) {
        Promise<ResultSet> promise = Promise.promise();
        call(connection, sql, promise);
        return promise.future();
    }

    /**
     * 带参数执行sql.
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository callWithParams(SQLConnection connection, String sql, JsonArray arguments1, JsonArray arguments2, Handler<AsyncResult<ResultSet>> handler) {
        connection.callWithParams(sql, arguments1, arguments2, handler);
        return this;
    }

    /**
     * 带参数执行sql.
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return BaseRepository
     */
    default Future<ResultSet> callWithParams(SQLConnection connection, String sql, JsonArray arguments1, JsonArray arguments2) {
        Promise<ResultSet> promise = Promise.promise();
        callWithParams(connection, sql, arguments1, arguments2, promise);
        return promise.future();
    }

    /**
     * 批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository batch(SQLConnection connection, List<String> sql, Handler<AsyncResult<List<Integer>>> handler) {
        connection.batch(sql, handler);
        return this;
    }

    /**
     * 批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @return BaseRepository
     */
    default Future<List<Integer>> batch(SQLConnection connection, List<String> sql) {
        Promise<List<Integer>> promise = Promise.promise();
        batch(connection, sql, promise);
        return promise.future();
    }

    /**
     * 根据参数批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments  参数
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository batchWithParams(SQLConnection connection, String sql, List<JsonArray> arguments, Handler<AsyncResult<List<Integer>>> handler) {
        connection.batchWithParams(sql, arguments, handler);
        return this;
    }

    /**
     * 根据参数批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments  参数
     * @return BaseRepository
     */
    default Future<List<Integer>> batchWithParams(SQLConnection connection, String sql, List<JsonArray> arguments) {
        Promise<List<Integer>> promise = Promise.promise();
        connection.batchWithParams(sql, arguments, promise);
        return promise.future();
    }

    /**
     * 根据参数批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @param handler    回调函数
     * @return BaseRepository
     */
    default BaseRepository batchCallableWithParams(SQLConnection connection, String sql, List<JsonArray> arguments1, List<JsonArray> arguments2, Handler<AsyncResult<List<Integer>>> handler) {
        connection.batchCallableWithParams(sql, arguments1, arguments2, handler);
        return this;
    }

    /**
     * 根据参数批处理。
     *
     * @param connection 数据库连接
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return BaseRepository
     */
    default Future<List<Integer>> batchCallableWithParams(SQLConnection connection, String sql, List<JsonArray> arguments1, List<JsonArray> arguments2) {
        Promise<List<Integer>> promise = Promise.promise();
        batchCallableWithParams(connection, sql, arguments1, arguments2, promise);
        return promise.future();
    }
}
