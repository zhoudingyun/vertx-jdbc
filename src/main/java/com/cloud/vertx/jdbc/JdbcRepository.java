package com.cloud.vertx.jdbc;

import com.cloud.vertx.jdbc.impl.JdbcRepositoryImpl;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;

import java.util.List;

/**
 * JDBC操作统一接口。
 *
 * @author zhoudingyun
 */
public interface JdbcRepository extends CurdRepository {

    /**
     * 执行 ddl语句。
     *
     * @param sql ddl语句
     * @return BaseRepository
     */
    Future<Void> execute(String sql);

    /**
     * 查询多条记录。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> query(String sql);

    /**
     * 查询数据流。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<SQLRowStream>
     */
    Future<SQLRowStream> queryStream(String sql);

    /**
     * 根据参数查询多条记录。
     * select * from user where name='张三' and status=1
     *
     * @param sql       sql语句 -> select * from user where name =? and status=?
     * @param arguments 参数 -> ['张三', '1']
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> queryWithParams(String sql, JsonArray arguments);

    /**
     * 根据参数查询数据流。
     * select * from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name =?
     * @param arguments 参数 -> ['张三']
     * @return Future<SQLRowStream>
     */
    Future<SQLRowStream> queryStreamWithParams(String sql, JsonArray arguments);

    /**
     * 查询单条记录。
     * select * from user where id=1
     *
     * @param sql sql语句 -> select * from user where id=1
     * @return Future<JsonArray>
     */
    Future<JsonArray> querySingle(String sql);

    /**
     * 根据参数查询单条记录。
     * select * from user where id=1
     *
     * @param sql       sql语句 -> select * from user where id =?
     * @param arguments 参数 -> [1]
     * @return Future<JsonArray>
     */
    Future<JsonArray> querySingleWithParams(String sql, JsonArray arguments);

    /**
     * 根据参数查询单条记录[取第一条数据]。
     * select * from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name =?
     * @param arguments 参数 -> ['张三']
     * @return Future<JsonObject> | 如果记录不存在返回 null
     */
    Future<JsonObject> queryOneWithParams(String sql, JsonArray arguments);

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
    Future<List<JsonObject>> queryPageWithParams(String sql, JsonArray arguments, int page, int limit);

    /**
     * 根据参数查询数据总数。
     * select count(*) from user where name='张三'
     *
     * @param sql       sql语句 -> select * from user where name=?
     * @param arguments 参数 -> ['张三']
     * @return Future<Integer>
     */
    Future<Integer> queryCountWithParams(String sql, JsonArray arguments);

    /**
     * 修改。
     * update user set name='张三' where id=1
     *
     * @param sql sql语句 ->  update user set name='张三' where id=1
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> update(String sql);

    /**
     * 根据参数修改。
     * update user set name='张三' where id=1
     *
     * @param sql       sql语句 -> update user set name=? where id=?
     * @param arguments 参数 -> ['张三', 1]
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> updateWithParams(String sql, JsonArray arguments);

    /**
     * 执行。
     *
     * @param sql sql语句
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> call(String sql);

    /**
     * 带参数执行.
     *
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> callWithParams(String sql, JsonArray arguments1, JsonArray arguments2);

    /**
     * 批处理。
     *
     * @param sql sql语句
     * @return Future<List < Integer>>
     */
    Future<List<Integer>> batch(List<String> sql);

    /**
     * 根据参数批处理。
     *
     * @param sql       sql语句
     * @param arguments 参数
     * @return Future<List < Integer>>
     */
    Future<List<Integer>> batchWithParams(String sql, List<JsonArray> arguments);

    /**
     * 根据参数批处理。
     *
     * @param sql        sql语句
     * @param arguments1 参数1
     * @param arguments2 参数2
     * @return Future<List < Integer>>
     */
    Future<List<Integer>> batchCallableWithParams(String sql, List<JsonArray> arguments1, List<JsonArray> arguments2);

    /**
     * 新增。
     * insert into user(name,uuid) values('张三', 'abc')
     *
     * @param values 参数 -> {name='张三'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> create(JsonObject values);

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param sets           sets -> {name='张三', sex ='1'}
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments whereArguments -> [1,'abc']
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> update(JsonObject sets, String where, JsonArray whereArguments);

    /**
     * 修改。
     * update user set name='张三', sex ='1' where id =1 and uuid='abc'
     *
     * @param sets  sets -> {name='张三', sex ='1'}
     * @param where 条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> update(JsonObject sets, JsonObject where);

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> delete(String where, JsonArray whereArguments);

    /**
     * 删除.
     * delete from user where id=1 and uuid='abc'
     *
     * @param where 条件 -> {id='1'，uuid='abc'}
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> delete(JsonObject where);

    /**
     * 查询单条数据.
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param where          where -> "id =? and uuid=?"
     * @param whereArguments 条件 -> [1,'abc']
     * @param columns        columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    Future<JsonObject> findOne(String where, JsonArray whereArguments, JsonArray columns);

    /**
     * 查询单条数据
     * select id, name ,uuid from user where id=1 and uuid='abc'
     *
     * @param where   where -> {id=1, uuid='abc'}
     * @param columns columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    Future<JsonObject> findOne(JsonObject where, JsonArray columns);

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
    Future<JsonObject> findOneOrder(String where, JsonArray whereArguments, String orderBy, JsonArray columns);

    /**
     * 查询单条数据排序取第一条
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param orderBy orderBy -> "name desc, uuid asc"
     * @param columns columns -> "id, name ,uuid"
     * @return Future<JsonObject>
     */
    Future<JsonObject> findOneOrder(JsonObject where, String orderBy, JsonArray columns);

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param where          where -> "name =? and uuid=?"
     * @param whereArguments whereArguments -> ['张三','abc']
     * @param columns        查询列columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> find(String where, JsonArray whereArguments, JsonArray columns);

    /**
     * 更加条件查询数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc'
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param columns 查询列columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> find(JsonObject where, JsonArray columns);

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
    Future<List<JsonObject>> findOrder(String where, JsonArray whereArguments, String orderBy, JsonArray columns);

    /**
     * 根据条件查询排序数据。
     * select id, name ,uuid from user where name='张三' and uuid='abc' order by id asc
     *
     * @param where   where -> {name='张三', uuid='abc'}
     * @param orderBy orderBy -> name desc, uuid asc
     * @param columns columns -> id, name ,uuid
     * @return Future<List < JsonObject>>
     */
    Future<List<JsonObject>> findOrder(JsonObject where, String orderBy, JsonArray columns);

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
    Future<List<JsonObject>> findPage(String where, JsonArray whereArguments, int page, int limit, JsonArray columns);

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
    Future<List<JsonObject>> findPage(JsonObject where, int page, int limit, JsonArray columns);

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param where          where -> {name =? and uuid=?}
     * @param whereArguments whereArguments -> ['张三','abc']
     * @return Future<Integer>
     */
    Future<Integer> count(String where, JsonArray whereArguments);

    /**
     * 根据条件查询数据总数。
     * select count(1) user where name='张三' and uuid='abc'
     *
     * @param where where ->  {name='张三', uuid='abc'}
     * @return Future<Integer>
     */
    Future<Integer> count(JsonObject where);

    /**
     * 执行多个sql带事务。
     *
     * @param arguments 参数 -> JsonObject.get("sql") JsonObject.get("param")[JsonArray]
     * @return Future<UpdateResult>
     */
    Future<UpdateResult> updateMultWithParams(JsonArray arguments);
}
