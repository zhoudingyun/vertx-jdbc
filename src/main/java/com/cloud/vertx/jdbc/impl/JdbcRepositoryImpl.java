package com.cloud.vertx.jdbc.impl;

import com.cloud.vertx.jdbc.JdbcRepository;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JDBC操作统一接口实现类。
 *
 * @author zhoudingyun
 */
public class JdbcRepositoryImpl extends CurdRepositoryImpl implements JdbcRepository {

    public JdbcRepositoryImpl(Vertx vertx, JsonObject config, String tableName) {
        super(vertx, config, tableName);
    }

    /**
     * 执行 ddl语句。
     *
     * @param sql ddl语句
     * @return BaseRepository
     */
    @Override
    public Future<Void> execute(String sql) {
        Promise<Void> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            execute(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 查询多条记录。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> query(String sql) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            query(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 查询数据流。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<SQLRowStream>
     */
    @Override
    public Future<SQLRowStream> queryStream(String sql) {
        Promise<SQLRowStream> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            queryStream(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询多条记录。
     * select * from user where name='张三' and status=1
     *
     * @param sql       sql语句 -> select * from user where name =? and status=?
     * @param arguments 参数 -> ['张三', '1']
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> queryWithParams(String sql, JsonArray arguments) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            queryWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询数据流。
     * select * from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name =?
     * @param arguments 参数 -> ['张三']
     * @return Future<SQLRowStream>
     */
    @Override
    public Future<SQLRowStream> queryStreamWithParams(String sql, JsonArray arguments) {
        Promise<SQLRowStream> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            queryStreamWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 查询单条记录。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<JsonArray>
     */
    @Override
    public Future<JsonArray> querySingle(String sql) {
        Promise<JsonArray> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            querySingle(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询单条记录。
     * select * from user where id=1
     *
     * @param sql       sql语句 -> select * from user where id =?
     * @param arguments 参数 -> [1]
     * @return Future<JsonArray>
     */
    @Override
    public Future<JsonArray> querySingleWithParams(String sql, JsonArray arguments) {
        Promise<JsonArray> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            querySingleWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询单条记录[取第一条数据]。
     * select * from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name =?
     * @param arguments 参数 -> ['张三']
     * @return Future<JsonObject> | 如果记录不存在返回 null
     */
    @Override
    public Future<JsonObject> queryOneWithParams(String sql, JsonArray arguments) {
        Promise<JsonObject> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            queryWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    ResultSet rs = r.result();

                    ResultSet newxt = rs.getNext();
                    List<JsonObject> resList = r.result().getRows();
                    if (resList == null || resList.isEmpty()) {
                        promise.complete(null);
                    } else {
                        promise.complete(resList.get(0));
                    }
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询分页数据。
     * select * from user where name='张三'  limit 0, 10
     *
     * @param sql       sql语句 -> select * from user where name=?  limit ?, ?
     * @param arguments 参数 -> ['张三']
     * @param page      页号 -> 0
     * @param limit     数量 -> 10
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> queryPageWithParams(String sql, JsonArray arguments, int page, int limit) {
        Promise<List<JsonObject>> promise = Promise.promise();
        arguments.add(calcPage(page, limit)).add(limit);
        client.getConnection(connHandler(promise, connection -> {
            queryWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    List<JsonObject> resList = r.result().getRows();
                    promise.complete(resList);
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数查询数据总数。
     * select count(*) from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name=?
     * @param arguments 参数 -> ['张三']
     * @return Future<Integer>
     */
    @Override
    public Future<Integer> queryCountWithParams(String sql, JsonArray arguments) {
        Promise<Integer> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            queryWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    List<JsonArray> resList = r.result().getResults();
                    promise.complete(resList.get(0).getInteger(0));
                } else {
                    promise.handle(Future.failedFuture(r.cause()));
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三' where id=1
     *
     * @param sql sql语句 ->  update user set name='张三' where id=1
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> update(String sql) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            update(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数修改。
     * update user set name='张三' where id=1
     *
     * @param sql       sql语句 -> update user set name=? where id=?
     * @param arguments 参数 -> ['张三', 1]
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> updateWithParams(String sql, JsonArray arguments) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            updateWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }


    /**
     * 执行。
     *
     * @param sql sql语句
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> call(String sql) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            call(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 带参数执行.
     *
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> callWithParams(String sql, JsonArray arguments1, JsonArray arguments2) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            callWithParams(connection, sql, arguments1, arguments2, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 批处理。
     *
     * @param sql sql语句
     * @return Future<List < Integer>>
     */
    @Override
    public Future<List<Integer>> batch(List<String> sql) {
        Promise<List<Integer>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            batch(connection, sql, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数批处理。
     *
     * @param sql       sql语句
     * @param arguments 参数
     * @return Future<List < Integer>>
     */
    @Override
    public Future<List<Integer>> batchWithParams(String sql, List<JsonArray> arguments) {
        Promise<List<Integer>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            batchWithParams(connection, sql, arguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 根据参数批处理。
     *
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return Future<List < Integer>>
     */
    @Override
    public Future<List<Integer>> batchCallableWithParams(String sql, List<JsonArray> arguments1, List<JsonArray> arguments2) {
        Promise<List<Integer>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            batchCallableWithParams(connection, sql, arguments1, arguments2, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));
        return promise.future();
    }

    /**
     * 新增。
     * insert into user(name,uuid) values('张三', 'abc')
     *
     * @param values 参数 -> {name='张三'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> create(JsonObject values) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            create(connection, values, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param sets           sets -> {name='张三', sex ='1'}
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> update(JsonObject sets, String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            update(connection, sets, where, whereArguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param sets  sets -> {name='张三', sex ='1'}
     * @param where 条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> update(JsonObject sets, JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            update(connection, sets, where);
            update(connection, sets, where, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> delete(String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            delete(connection, where, whereArguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param where 条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> delete(JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            delete(connection, where, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @param columns        columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    @Override
    public Future<JsonObject> findOne(String where, JsonArray whereArguments, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOne(connection, where, whereArguments, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 查询单条数据
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param where   where -> {id=1, uuid='abc'}
     * @param columns columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    @Override
    public Future<JsonObject> findOne(JsonObject where, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOne(connection, where, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 查询单条数据排序取第一条
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param orderBy        orderBy -> "name desc, uuid asc"
     * @param columns        columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    @Override
    public Future<JsonObject> findOneOrder(String where, JsonArray whereArguments, String orderBy, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOneOrder(connection, where, whereArguments, orderBy, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 查询单条数据排序取第一条
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param orderBy orderBy -> "name desc, uuid asc"
     * @param columns columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    @Override
    public Future<JsonObject> findOneOrder(JsonObject where, String orderBy, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOneOrder(connection, where, orderBy, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param columns        查询列columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> find(String where, JsonArray whereArguments, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            find(connection, where, whereArguments, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param columns 查询列columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> find(JsonObject where, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            find(connection, where, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param orderBy        orderBy -> name desc, uuid asc
     * @param columns        columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> findOrder(String where, JsonArray whereArguments, String orderBy, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOrder(connection, where, whereArguments, orderBy, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param orderBy orderBy -> name desc, uuid asc
     * @param columns columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> findOrder(JsonObject where, String orderBy, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findOrder(connection, where, orderBy, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments where -> ['张三','abc']
     * @param page           page -> 0
     * @param limit          limit -> 10
     * @param columns        columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> findPage(String where, JsonArray whereArguments, int page, int limit, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findPage(connection, where, whereArguments, page, limit, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param page    page -> 0
     * @param limit   limit -> 10
     * @param columns columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    @Override
    public Future<List<JsonObject>> findPage(JsonObject where, int page, int limit, JsonArray columns) {
        Promise<List<JsonObject>> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            findPage(connection, where, page, limit, columns, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result().getRows());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @return Future<Integer>
     */
    @Override
    public Future<Integer> count(String where, JsonArray whereArguments) {
        Promise<Integer> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            count(connection, where, whereArguments, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param where where ->  {name='张三', uuid='abc'}
     * @return Future<Integer>
     */
    @Override
    public Future<Integer> count(JsonObject where) {
        Promise<Integer> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            count(connection, where, r -> {
                if (r.succeeded()) {
                    promise.complete(r.result());
                } else {
                    promise.fail(r.cause());
                }
                connection.close();
            });
        }));

        return promise.future();
    }

    /**
     * 执行多个sql带事务。
     *
     * @param arguments 参数 -> JsonObject.get("sql") JsonObject.get("param")[JsonArray]
     * @return Future<UpdateResult>
     */
    @Override
    public Future<UpdateResult> updateMultWithParams(JsonArray arguments) {
        Promise<UpdateResult> promise = Promise.promise();
        client.getConnection(connHandler(promise, connection -> {
            List<Future> list = new ArrayList<>();
            // 手动开启事务
            Promise<Void> beginTransaction = Promise.promise();
            connection.setAutoCommit(false, beginTransaction);
            list.add(beginTransaction.future());

            Promise<UpdateResult> future;
            for (int i = 0; i < arguments.size(); i++) {
                JsonObject object = arguments.getJsonObject(i);
                future = Promise.promise();
                connection.updateWithParams(object.getString("sql"), object.getJsonArray("param"), future);
                list.add(future.future());
            }

            CompositeFuture.all(list).onSuccess(success -> {
                connection.commit(voidAsyncResult -> {
                    if (voidAsyncResult.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(voidAsyncResult.cause());
                    }
                    connection.close();
                });
            }).onFailure(throwable -> {
                connection.rollback(voidAsyncResult -> {
                    if (voidAsyncResult.succeeded()) {
                        promise.fail(throwable);
                    } else {
                        promise.fail(voidAsyncResult.cause());
                    }
                    connection.close();
                });
            });
        }));

        return promise.future();
    }
}
