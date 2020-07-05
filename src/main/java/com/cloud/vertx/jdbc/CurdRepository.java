package com.cloud.vertx.jdbc;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

import java.util.Iterator;
import java.util.List;

/**
 * curd 操作接口。
 *
 * @author zhoudingyun
 */
public interface CurdRepository extends BaseRepository {

    /**
     * 根据jsonobject 生成where条件和参数。
     *
     * @param where      {name='张三', uuid='abc'}
     * @param conditions "name=?, uuid=?"
     * @param arguments  ['张三', 'abc']
     */
    default void generateWhere(JsonObject where, StringBuilder conditions, JsonArray arguments) {
        int i = 0;
        String key;
        for (Iterator<String> it = where.fieldNames().iterator(); it.hasNext(); ) {
            if (i != 0) {
                conditions.append(" AND ");
            }
            i++;
            key = it.next();
            conditions.append(key).append("=?");
            arguments.add(where.getValue(key));
        }
    }

    /**
     * 分页计算.
     *
     * @param page  页号
     * @param limit 数量
     * @return int
     */
    default int calcPage(int page, int limit) {
        if (page <= 0)
            return 0;
        return limit * (page - 1);
    }

    /**
     * 根据参数查询单条记录【没有记录回调结果为null】。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @param handler    回调函数
     * @return CurdRepository
     */
    default CurdRepository queryOneWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<JsonObject>> handler) {
        connection.queryWithParams(sql, arguments, r -> {
            if (r.succeeded()) {
                List<JsonObject> resList = r.result().getRows();
                if (resList == null || resList.isEmpty()) {
                    handler.handle(Future.succeededFuture(null));
                } else {
                    handler.handle(Future.succeededFuture(resList.get(0)));
                }
            } else {
                handler.handle(Future.failedFuture(r.cause()));
            }
            connection.close();
        });

        return this;
    }

    /**
     * 根据参数查询单条记录【没有记录回调结果为null】。
     * select * from user where name='张三' and status=1
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name =? and status=?
     * @param arguments  参数 -> ['张三', '1']
     * @return Future<JsonObject>
     */
    default Future<JsonObject> queryOneWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<JsonObject> promise = Promise.promise();
        queryOneWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 根据参数查询分页数据。
     * select * from user where name='张三'  limit 0, 10
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name=?  limit ?, ?
     * @param arguments  参数 -> ['张三']
     * @param page       页号 -> 0
     * @param limit      数量 -> 10
     * @param handler    回调函数
     * @return CurdRepository
     */
    default CurdRepository queryPageWithParams(SQLConnection connection, String sql, JsonArray arguments, int page, int limit, Handler<AsyncResult<ResultSet>> handler) {
        arguments.add(calcPage(page, limit)).add(limit);
        connection.queryWithParams(sql, arguments, r -> {
            if (r.succeeded()) {
                handler.handle(Future.succeededFuture(r.result()));
            } else {
                handler.handle(Future.failedFuture(r.cause()));
            }

        });

        return this;
    }

    /**
     * 根据参数查询分页数据。
     * select * from user where name='张三'  limit 0, 10
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name=?  limit ?, ?
     * @param arguments  参数 -> ['张三']
     * @param page       页号 -> 0
     * @param limit      数量 -> 10
     * @return Future<ResultSet>
     */
    default Future<ResultSet> queryPageWithParams(SQLConnection connection, String sql, JsonArray arguments, int page, int limit) {
        Promise<ResultSet> promise = Promise.promise();
        queryPageWithParams(connection, sql, arguments, page, limit, promise);
        return promise.future();
    }

    /**
     * 根据参数查询数据总数。
     * select count(*) from user where name='张三'
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name=?
     * @param arguments  参数 -> ['张三']
     * @param handler    回调函数
     * @return CurdRepository
     */
    default CurdRepository queryCountWithParams(SQLConnection connection, String sql, JsonArray arguments, Handler<AsyncResult<Integer>> handler) {
        connection.queryWithParams(sql, arguments, r -> {
            if (r.succeeded()) {
                List<JsonArray> resList = r.result().getResults();
                handler.handle(Future.succeededFuture(resList.get(0).getInteger(0)));
            } else {
                handler.handle(Future.failedFuture(r.cause()));
            }

        });

        return this;
    }

    /**
     * 根据参数查询数据总数。
     * select count(*) from user where name='张三'
     *
     * @param connection 数据库连接
     * @param sql        sql语句 -> select * from user where name=?
     * @param arguments  参数 -> ['张三']
     * @return Future<Integer>
     */
    default Future<Integer> queryCountWithParams(SQLConnection connection, String sql, JsonArray arguments) {
        Promise<Integer> promise = Promise.promise();
        queryCountWithParams(connection, sql, arguments, promise);
        return promise.future();
    }

    /**
     * 新增。
     * insert into user(name,uuid) values('张三', 'abc')
     *
     * @param connection 数据库连接
     * @param values     参数 -> {name='张三'，uuid='abc'}
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository create(SQLConnection connection, JsonObject values, Handler<AsyncResult<UpdateResult>> handler);

    /**
     * 新增。
     * insert into user(name,uuid) values('张三', 'abc')
     *
     * @param connection 数据库连接
     * @param values     参数 -> {name='张三'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> create(SQLConnection connection, JsonObject values) {
        Promise<UpdateResult> promise = Promise.promise();
        create(connection, values, promise);
        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param sets           sets -> {name='张三', sex ='1'}
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository update(SQLConnection connection, JsonObject sets, String where, JsonArray whereArguments, Handler<AsyncResult<UpdateResult>> handler);

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param sets           sets -> {name='张三', sex ='1'}
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> update(SQLConnection connection, JsonObject sets, String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        update(connection, sets, where, whereArguments, promise);
        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param sets       sets -> {name='张三', sex ='1'}
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository update(SQLConnection connection, JsonObject sets, JsonObject where, Handler<AsyncResult<UpdateResult>> handler);

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param sets       sets -> {name='张三', sex ='1'}
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> update(SQLConnection connection, JsonObject sets, JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        update(connection, sets, where, promise);
        return promise.future();
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository delete(SQLConnection connection, String where, JsonArray whereArguments, Handler<AsyncResult<UpdateResult>> handler);

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> delete(SQLConnection connection, String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        delete(connection, where, whereArguments, promise);
        return promise.future();
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository delete(SQLConnection connection, JsonObject where, Handler<AsyncResult<UpdateResult>> handler);

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    default Future<UpdateResult> delete(SQLConnection connection, JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        delete(connection, where, promise);
        return promise.future();
    }

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @param columns        columns -> "id, name ,uuid"
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository findOne(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns, Handler<AsyncResult<JsonObject>> handler);

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @param columns        columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    default Future<JsonObject> findOne(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        findOne(connection, where, whereArguments, columns, promise);
        return promise.future();
    }

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where -> {id=1, uuid='abc'}
     * @param columns    columns -> "id, name ,uuid"
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository findOne(SQLConnection connection, JsonObject where, JsonArray columns, Handler<AsyncResult<JsonObject>> handler);

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where -> {id=1, uuid='abc'}
     * @param columns    columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    default Future<JsonObject> findOne(SQLConnection connection, JsonObject where, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        findOne(connection, where, columns, promise);
        return promise.future();
    }

    /**
     * 查询单条数据排序取第一条.
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection     数据库连接
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments where -> ['张三','abc']
     * @param orderBy        orderBy -> "name desc, uuid asc"
     * @param columns        columns -> "id, name ,uuid"
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository findOneOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns, Handler<AsyncResult<JsonObject>> handler);

    /**
     * 查询单条数据排序取第一条.
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection     数据库连接
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments where -> ['张三','abc']
     * @param orderBy        orderBy -> "name desc, uuid asc"
     * @param columns        columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    default Future<JsonObject> findOneOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        findOneOrder(connection, where, whereArguments, orderBy, columns, promise);
        return promise.future();
    }

    /**
     * 查询单条数据排序取第一条.
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param orderBy    orderBy -> "name desc, uuid asc"
     * @param columns    columns -> "id, name ,uuid"
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository findOneOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns, Handler<AsyncResult<JsonObject>> handler);

    /**
     * 查询单条数据排序取第一条.
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param orderBy    orderBy -> "name desc, uuid asc"
     * @param columns    columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    default Future<JsonObject> findOneOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns) {
        Promise<JsonObject> promise = Promise.promise();
        findOneOrder(connection, where, orderBy, columns, promise);
        return promise.future();
    }

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param columns        查询列columns -> id, name ,uuid
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository find(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param columns        查询列columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> find(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        find(connection, where, whereArguments, columns, promise);
        return promise.future();
    }

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param columns    查询列columns -> id, name ,uuid
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository find(SQLConnection connection, JsonObject where, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param columns    查询列columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> find(SQLConnection connection, JsonObject where, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        find(connection, where, columns, promise);
        return promise.future();
    }

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param orderBy        orderBy -> name desc, uuid asc
     * @param columns        columns -> id, name ,uuid
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository findOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param orderBy        orderBy -> name desc, uuid asc
     * @param columns        columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> findOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        findOrder(connection, where, whereArguments, orderBy, columns, promise);
        return promise.future();
    }

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param orderBy    orderBy -> name desc, uuid asc
     * @param columns    columns -> id, name ,uuid
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository findOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param orderBy    orderBy -> name desc, uuid asc
     * @param columns    columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> findOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        findOrder(connection, where, orderBy, columns, promise);
        return promise.future();
    }

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param page           page -> 0
     * @param limit          limit -> 10
     * @param columns        columns -> id, name ,uuid
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository findPage(SQLConnection connection, String where, JsonArray whereArguments, int page, int limit, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param page           page -> 0
     * @param limit          limit -> 10
     * @param columns        columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> findPage(SQLConnection connection, String where, JsonArray whereArguments, int page, int limit, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        findPage(connection, where, whereArguments, page, limit, columns, promise);
        return promise.future();
    }

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param page       page -> 0
     * @param limit      limit -> 10
     * @param columns    columns -> id, name ,uuid
     * @param handler    回调函数
     * @return CurdRepository
     */
    CurdRepository findPage(SQLConnection connection, JsonObject where, int page, int limit, JsonArray columns, Handler<AsyncResult<ResultSet>> handler);

    /**
     * 根据条件查询分页数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' limit 0, 10
     *
     * @param connection 数据库连接
     * @param where      where -> {name='张三', uuid='abc'}
     * @param page       page -> 0
     * @param limit      limit -> 10
     * @param columns    columns -> id, name ,uuid
     * @return Future<ResultSet>
     */
    default Future<ResultSet> findPage(SQLConnection connection, JsonObject where, int page, int limit, JsonArray columns) {
        Promise<ResultSet> promise = Promise.promise();
        findPage(connection, where, page, limit, columns, promise);
        return promise.future();
    }

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param handler        回调函数
     * @return CurdRepository
     */
    CurdRepository count(SQLConnection connection, String where, JsonArray whereArguments, Handler<AsyncResult<Integer>> handler);

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @return Future<Integer>
     */
    default Future<Integer> count(SQLConnection connection, String where, JsonArray whereArguments) {
        Promise<Integer> promise = Promise.promise();
        count(connection, where, whereArguments, promise);
        return promise.future();
    }

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where ->  {name='张三', uuid='abc'}
     * @param handler    回调函数
     * @return CurdRepository
     */
    BaseRepository count(SQLConnection connection, JsonObject where, Handler<AsyncResult<Integer>> handler);

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      where ->  {name='张三', uuid='abc'}
     * @return Future<Integer>
     */
    default Future<Integer> count(SQLConnection connection, JsonObject where) {
        Promise<Integer> promise = Promise.promise();
        count(connection, where, promise);
        return promise.future();
    }
}
