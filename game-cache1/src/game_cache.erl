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
    % 调用 game_cache_mapping:get_pid 获取键对应的存储进程 PID
    case game_cache_mapping:get_pid(Key) of
      % 如果找到 PID，则调用 game_cache_store:get 获取值
      {ok, Pid} ->
        game_cache_store:get(Pid);
      % 如果键不存在，返回错误
      {error, not_found} ->
        {error, not_found}
    end
  catch
    Error:Reason ->
      % 使用 logger 记录获取操作失败的错误信息
      logger:error("获取键 ~p 失败: ~p:~p", [Key, Error, Reason]),
      {error, Reason}
  end.

% 批量获取多个键的值，参数为键列表
get_batch(Keys) ->
  try
    % 调用 game_cache_mapping:get_pids 批量获取键对应的 PID
    KeyPidList = game_cache_mapping:get_pids(Keys),
    % 对每个键和 PID 调用 game_cache_store:get 获取值
    Results = [{Key, case PidResult of
                       {ok, Pid} -> game_cache_store:get(Pid);
                       {error, not_found} -> {error, not_found}
                     end} || {Key, PidResult} <- KeyPidList],
    % 返回批量获取结果，格式为 [{Key, {ok, Value} | {error, not_found}}]
    {ok, Results}
  catch
    Error:Reason ->
      logger:error("批量获取键 ~p 失败: ~p:~p", [Keys, Error, Reason]),
      {error, Reason}
  end.

% 设置键值对，参数为键和值
set(Key, Value) ->
  try
    % 调用 game_cache_mapping:get_or_create_pid 获取或创建存储进程 PID
    {ok, Pid} = game_cache_mapping:get_or_create_pid(Key),
    % 调用 game_cache_store:set 设置值
    game_cache_store:set(Pid, Value)
  catch
    Error:Reason ->
      logger:error("设置键 ~p 失败: ~p:~p", [Key, Error, Reason]),
      {error, Reason}
  end.

% 批量设置多个键值对，参数为键值对列表 [{Key, Value}]
set_batch(KeyValueList) ->
  try
    % 提取键列表
    Keys = [Key || {Key, _Value} <- KeyValueList],
    % 调用 game_cache_mapping:get_or_create_pids 批量获取或创建各Keys的 PID，KeyPidList的格式为[{Key, {ok, Pid}}]
    {ok, KeyPidList} = game_cache_mapping:get_or_create_pids(Keys),
    % 对每个键和 PID 设置值
    Results = [{Key, game_cache_store:set(Pid, Value)} || {{Key, Value}, {Key, {ok, Pid}}} <- lists:zip(KeyValueList, KeyPidList)],
    % 返回批量设置结果，格式为 [{Key, ok | {error, Reason}}]
    {ok, Results}
  catch
    Error:Reason ->
      logger:error("批量设置键 ~p 失败: ~p:~p", [[Key || {Key, _} <- KeyValueList], Error, Reason]),
      {error, Reason}
  end.

% 删除指定键，参数为键
delete(Key) ->
  try
    % 调用 game_cache_mapping:get_pid 获取键对应的存储进程 PID
    case game_cache_mapping:get_pid(Key) of
      % 如果找到 PID，删除存储进程和映射
      {ok, Pid} ->
        % 调用 game_cache_store:delete 停止存储进程
        game_cache_store:delete(Pid),
        % 调用 game_cache_mapping:delete 删除键的映射
        game_cache_mapping:delete(Key);
      % 如果键不存在，直接返回 ok
      {error, not_found} ->
        ok
    end
  catch
    Error:Reason ->
      logger:error("删除键 ~p 失败: ~p:~p", [Key, Error, Reason]),
      {error, Reason}
  end.

% 批量删除多个键，参数为键列表
delete_batch(Keys) ->
  try
    % 调用 game_cache_mapping:get_pids 批量获取键对应的 PID
    KeyPidList = game_cache_mapping:get_pids(Keys),
    % 对每个存在的键删除存储进程和映射
    Results = [{Key, case PidResult of
                       {ok, Pid} ->
                         game_cache_store:delete(Pid),
                         game_cache_mapping:delete(Key);
                       {error, not_found} ->
                         ok
                     end} || {Key, PidResult} <- KeyPidList],
    % 返回批量删除结果，格式为 [{Key, ok}]
    {ok, Results}
  catch
    Error:Reason ->
      logger:error("批量删除键 ~p 失败: ~p:~p", [Keys, Error, Reason]),
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



