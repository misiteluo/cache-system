%%%-------------------------------------------------------------------
%%% @author luoruiyu
%%% @copyright (C) 2025, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 6月 2025 12:48
%%%-------------------------------------------------------------------

% 次模块为应用层API
-module(game_cache).
-author("luoruiyu").

%% API
-export([start/0, stop/0, get/1, get_batch/1, set/2, set_batch/1, delete/1, delete_batch/1, async_set/2, async_set_batch/1, async_delete/1, async_delete_batch/1]).

% 启动应用，确保所有相关进程启动
start() ->
  application:ensure_all_started(game_cache).

% 停止应用，关闭所有相关进程
stop() ->
  application:stop(game_cache).

% 获取指定键的值，参数为键
get(Key) ->
  try
    case game_cache_mapping:get_pid(Key) of
      {ok, Pid} ->
        game_cache_store:get(Pid);
      {error, not_found} ->
        {error, not_found}
    end
  catch
    Error:Reason:Stacktrace ->
      logger:error("获取键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.

% 批量获取多个键的值，参数为键列表
get_batch(Keys) ->
  try
    KeyPidList = game_cache_mapping:get_pids(Keys),
    Results = [{Key, case PidResult of
                       {ok, Pid} ->
                         game_cache_store:get(Pid);
                       {error, not_found} ->
                         {error, not_found}
                     end} || {Key, PidResult} <- KeyPidList],
    {ok, Results}
  catch
    Error:Reason:Stacktrace ->
      logger:error("批量获取键 ~p 失败: ~p:~p, stacktrace: ~p", [Keys, Error, Reason, Stacktrace]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.

% 设置键值对，参数为键和值
set(Key, Value) ->
  try
    game_cache_mapping:set(Key, Value)
  catch
    Error:Reason:Stacktrace ->
      logger:error("设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.

% 批量设置多个键值对，参数为键值对列表 [{Key, Value}]
set_batch(KeyValueList) ->
  try
    game_cache_mapping:set_batch(KeyValueList)
  catch
    Error:Reason:Stacktrace ->
      logger:error("批量设置键 ~p 失败: ~p:~p, stacktrace: ~p", [[Key || {Key, _} <- KeyValueList], Error, Reason, Stacktrace]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.

% 删除指定键，参数为键
delete(Key) ->
  try
    % 通过 game_cache_mapping 的 gen_server 调用删除
    game_cache_mapping:delete(Key)
  catch
    Error:Reason ->
      logger:error("删除键 ~p 失败: ~p:~p", [Key, Error, Reason]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.

% 批量删除多个键，参数为键列表
delete_batch(Keys) ->
  try
    % 通过 game_cache_mapping 的 gen_server 调用批量删除
    game_cache_mapping:delete_batch(Keys)
  catch
    Error:Reason ->
      logger:error("批量删除键 ~p 失败: ~p:~p", [Keys, Error, Reason]),
      % 触发一致性检查
      game_cache_mapping:check_consistency(),
      {error, Reason}
  end.


% 异步设置键值对，参数为键和值
async_set(Key, Value) ->
  % 使用 spawn 创建新进程异步执行 set 操作
  spawn(fun() -> set(Key, Value) end),
  % 返回 ok，不等待操作完成
  ok.

% 异步批量设置键值对，参数为键值对列表
async_set_batch(KeyValueList) ->
  % 使用 spawn 创建新进程异步执行 set_batch 操作
  spawn(fun() -> set_batch(KeyValueList) end),
  % 返回 ok，不等待操作完成
  ok.

% 异步删除指定键，参数为键
async_delete(Key) ->
  % 使用 spawn 创建新进程异步执行 delete 操作
  spawn(fun() -> delete(Key) end),
  % 返回 ok，不等待操作完成
  ok.

% 异步批量删除键，参数为键列表
async_delete_batch(Keys) ->
  % 使用 spawn 创建新进程异步执行 delete_batch 操作
  spawn(fun() -> delete_batch(Keys) end),
  % 返回 ok，不等待操作完成
  ok.



