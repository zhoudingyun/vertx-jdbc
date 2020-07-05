package com.cloud.vertx.jdbc.impl;

import com.cloud.vertx.jdbc.CurdRepository;
import com.cloud.vertx.jdbc.sql.SqlBuilder;
import com.cloud.vertx.jdbc.sql.TSqlBuilder;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

/**
 * curd操作实现类。
 *
 * @author zhoudingyun
 */
public class CurdRepositoryImpl extends BaseRepositoryImpl implements CurdRepository {
    private String tableName;

    /**
     * 构造方法。
     *
     * @param vertx     vertx
     * @param config    config
     * @param tableName
     * @return CurdRepository
     */
    public CurdRepositoryImpl(Vertx vertx, JsonObject config, String tableName) {
        super(vertx, config);
        this.tableName = tableName;
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
    @Override
    public CurdRepository create(SQLConnection connection, JsonObject values, Handler<AsyncResult<UpdateResult>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        String[] columns = new String[values.fieldNames().size()];
        JsonArray arguments = new JsonArray();
        values.fieldNames().toArray(columns);
        sql.insert().into(this.tableName).values(columns);
        values.forEach(value -> {
            arguments.add(value.getValue());
        });
        this.updateWithParams(connection, sql.toString(), arguments, handler);

        return this;
    }

    /**
     * 新增。
     * insert into user(name,uuid) values('张三', 'abc')
     *
     * @param connection 数据库连接
     * @param values     参数 -> {name='张三'，uuid='abc'}
     * @return CurdRepository
     */
    @Override
    public Future<UpdateResult> create(SQLConnection connection, JsonObject values) {
        Promise<UpdateResult> promise = Promise.promise();
        this.create(connection, values, updateResultAsyncResult -> {
            if (updateResultAsyncResult.succeeded()) {
                promise.complete(updateResultAsyncResult.result());
            } else {
                promise.fail(updateResultAsyncResult.cause());
            }
        });

        return promise.future();
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param set            set -> {name='张三', sex ='1'}
     * @param where          conditions -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @param handler        回调函数
     * @return CurdRepository
     */
    @Override
    public CurdRepository update(SQLConnection connection, JsonObject set, String where, JsonArray whereArguments, Handler<AsyncResult<UpdateResult>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        String[] columns = new String[set.fieldNames().size()];
        JsonArray arguments = new JsonArray();
        set.fieldNames().toArray(columns);
        sql.update(this.tableName).values(columns).where(where);
        set.forEach(value -> {
            arguments.add(value.getValue());
        });
        arguments.addAll(whereArguments);

        this.updateWithParams(connection, sql.toString(), arguments, handler);
        return this;
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param sets           sets -> {name='张三', sex ='1'}
     * @param where          conditions -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @return CurdRepository
     */
    @Override
    public Future<UpdateResult> update(SQLConnection connection, JsonObject sets, String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        this.update(connection, sets, where, whereArguments, updateResultAsyncResult -> {
            if (updateResultAsyncResult.succeeded()) {
                promise.complete(updateResultAsyncResult.result());
            } else {
                promise.fail(updateResultAsyncResult.cause());
            }
        });

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
    @Override
    public CurdRepository update(SQLConnection connection, JsonObject sets, JsonObject where, Handler<AsyncResult<UpdateResult>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray argumentsWhere = new JsonArray();
        generateWhere(where, conditions, argumentsWhere);

        SqlBuilder sql = new TSqlBuilder();
        String[] columns = new String[sets.fieldNames().size()];
        JsonArray arguments = new JsonArray();
        sets.fieldNames().toArray(columns);
        sql.update(this.tableName).values(columns).where(conditions.toString());
        sets.forEach(value -> {
            arguments.add(value.getValue());
        });
        arguments.addAll(argumentsWhere);

        this.updateWithParams(connection, sql.toString(), arguments, handler);
        return this;
    }

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param sets       sets -> {name='张三', sex ='1'}
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @return CurdRepository
     */
    @Override
    public Future<UpdateResult> update(SQLConnection connection, JsonObject sets, JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        this.update(connection, sets, where, updateResultAsyncResult -> {
            if (updateResultAsyncResult.succeeded()) {
                promise.complete(updateResultAsyncResult.result());
            } else {
                promise.fail(updateResultAsyncResult.cause());
            }
        });

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
    @Override
    public CurdRepository delete(SQLConnection connection, String where, JsonArray whereArguments, Handler<AsyncResult<UpdateResult>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        sql.delete().from(this.tableName).where(where);
        this.updateWithParams(connection, sql.toString(), whereArguments, handler);
        return this;
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection     数据库连接
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @return CurdRepository
     */
    @Override
    public Future<UpdateResult> delete(SQLConnection connection, String where, JsonArray whereArguments) {
        Promise<UpdateResult> promise = Promise.promise();
        this.delete(connection, where, whereArguments, updateResultAsyncResult -> {
            if (updateResultAsyncResult.succeeded()) {
                promise.complete(updateResultAsyncResult.result());
            } else {
                promise.fail(updateResultAsyncResult.cause());
            }
        });

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
    @Override
    public CurdRepository delete(SQLConnection connection, JsonObject where, Handler<AsyncResult<UpdateResult>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);

        SqlBuilder sql = new TSqlBuilder();
        sql.delete().from(this.tableName).where(conditions.toString());
        this.updateWithParams(connection, sql.toString(), arguments, handler);

        return this;
    }

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param connection 数据库连接
     * @param where      条件 -> {id='1'，uuid='abc'}
     * @return CurdRepository
     */
    @Override
    public Future<UpdateResult> delete(SQLConnection connection, JsonObject where) {
        Promise<UpdateResult> promise = Promise.promise();
        this.delete(connection, where, updateResultAsyncResult -> {
            if (updateResultAsyncResult.succeeded()) {
                promise.complete(updateResultAsyncResult.result());
            } else {
                promise.fail(updateResultAsyncResult.cause());
            }
        });

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
    @Override
    public CurdRepository findOne(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns, Handler<AsyncResult<JsonObject>> handler) {
        SqlBuilder sql = new TSqlBuilder();

        sql.select(columns).from(this.tableName).where(where);
        this.queryOneWithParams(connection, sql.toString(), whereArguments, handler);

        return this;
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
    @Override
    public CurdRepository findOne(SQLConnection connection, JsonObject where, JsonArray columns, Handler<AsyncResult<JsonObject>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);

        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(conditions.toString());
        this.queryOneWithParams(connection, sql.toString(), arguments, handler);

        return this;
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
    @Override
    public CurdRepository findOneOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns, Handler<AsyncResult<JsonObject>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(where).orderBy(orderBy);
        this.queryOneWithParams(connection, sql.toString(), whereArguments, handler);

        return this;
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
    @Override
    public CurdRepository findOneOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns, Handler<AsyncResult<JsonObject>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);

        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(conditions.toString()).orderBy(orderBy);
        this.queryOneWithParams(connection, sql.toString(), arguments, handler);

        return this;
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
    @Override
    public CurdRepository find(SQLConnection connection, String where, JsonArray whereArguments, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(where);
        this.queryWithParams(connection, sql.toString(), whereArguments, handler);

        return this;
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
    @Override
    public CurdRepository find(SQLConnection connection, JsonObject where, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);
        SqlBuilder sql = new TSqlBuilder();

        sql.select(columns).from(this.tableName).where(conditions.toString());
        this.queryWithParams(connection, sql.toString(), arguments, handler);
        return this;
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
    @Override
    public CurdRepository findOrder(SQLConnection connection, String where, JsonArray whereArguments, String orderBy, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        SqlBuilder sql = new TSqlBuilder();

        sql.select(columns).from(this.tableName).where(where).orderBy(orderBy);
        this.queryWithParams(connection, sql.toString(), whereArguments, handler);
        return this;
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
    @Override
    public CurdRepository findOrder(SQLConnection connection, JsonObject where, String orderBy, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);

        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(conditions.toString()).orderBy(orderBy);
        this.queryWithParams(connection, sql.toString(), arguments, handler);
        return this;
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
    @Override
    public CurdRepository findPage(SQLConnection connection, String where, JsonArray whereArguments, int page, int limit, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        sql.select(columns).from(this.tableName).where(where);
        this.queryPageWithParams(connection, sql.toString(), whereArguments, page, limit, handler);
        return this;
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
    @Override
    public CurdRepository findPage(SQLConnection connection, JsonObject where, int page, int limit, JsonArray columns, Handler<AsyncResult<ResultSet>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);

        SqlBuilder sql = new TSqlBuilder();

        sql.select(columns).from(this.tableName).where(conditions.toString());
        this.queryPageWithParams(connection, sql.toString(), arguments, page, limit, handler);
        return this;
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
    @Override
    public CurdRepository count(SQLConnection connection, String where, JsonArray whereArguments, Handler<AsyncResult<Integer>> handler) {
        SqlBuilder sql = new TSqlBuilder();
        sql.select("count(1)").from(this.tableName).where(where);
        this.queryCountWithParams(connection, sql.toString(), whereArguments, handler);
        return this;
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
    @Override
    public CurdRepository count(SQLConnection connection, JsonObject where, Handler<AsyncResult<Integer>> handler) {
        StringBuilder conditions = new StringBuilder();
        JsonArray arguments = new JsonArray();
        generateWhere(where, conditions, arguments);
        SqlBuilder sql = new TSqlBuilder();
        sql.select("count(1)").from(this.tableName).where(conditions.toString());
        this.queryCountWithParams(connection, sql.toString(), arguments, handler);
        return this;
    }
}
