%%%-------------------------------------------------------------------
%%% @author luoruiyu
%%% @copyright (C) 2025, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 6月 2025 17:06
%%%-------------------------------------------------------------------

% 此模块为 键 和 值存储进程 之间映射关系的存储模块
-module(game_cache_mapping).
-author("luoruiyu").

-behaviour(gen_server).
%% API
-export([start_link/0, get_pid/1, get_pids/1, get_or_create_pid/1, get_or_create_pids/1, delete/1, delete_batch/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(ETS_TABLE, game_cache_mapping).

% 启动映射进程
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% 获取键对应的存储进程 PID，参数为键
get_pid(Key) ->
  try
    % 查询 ETS 表以获取键对应的 PID
    case ets:lookup(?ETS_TABLE, Key) of
      % 如果找到，返回 PID
      [{Key, Pid}] -> {ok, Pid};
      % 如果未找到，返回错误
      [] -> {error, not_found}
    end
  catch
    Error:Reason ->
      logger:error("获取键 ~p 的PID失败: ~p:~p", [Key, Error, Reason]),
      {error, Reason}
  end.

% 批量获取键对应的 PID，参数为键列表
get_pids(Keys) ->
  try
    % 对每个键查询 ETS 表，返回键和 PID 或错误
    Results = [{Key, case ets:lookup(?ETS_TABLE, Key) of
                       [{Key, Pid}] -> {ok, Pid};
                       [] -> {error, not_found}
                     end} || Key <- Keys],
    % 返回批量结果，格式为 [{Key, {ok, Pid} | {error, not_found}}]
    Results
  catch
    Error:Reason ->
      logger:error("批量获取键 ~p 失败: ~p:~p", [Keys, Error, Reason]),
      {error, Reason}
  end.

% 获取或创建键对应的存储进程 PID，参数为键
get_or_create_pid(Key) ->
  % 调用 gen_server:call 同步请求获取或创建 PID
  gen_server:call(?MODULE, {get_or_create_pid, Key}).

% 批量获取或创建键对应的 PID，参数为键列表
get_or_create_pids(Keys) ->
  % 调用 gen_server:call 同步请求批量获取或创建 PID
  gen_server:call(?MODULE, {get_or_create_pids, Keys}).

% 删除键的映射，参数为键
delete(Key) ->
  % 调用 gen_server:call 同步请求删除键的映射
  gen_server:call(?MODULE, {delete, Key}).

% 批量删除键的映射，参数为键列表
delete_batch(Keys) ->
  % 调用 gen_server:call 同步请求批量删除键的映射
  gen_server:call(?MODULE, {delete_batch, Keys}).



% 初始化 gen_server
init([]) ->
  try
    % 创建 ETS 表，设置属性为 set、protected、named_table，并启用写并发
    ?ETS_TABLE = ets:new(?ETS_TABLE, [set, protected, named_table, {write_concurrency, true}]),
    % 模拟加载初始映射数据
    ok = load_initial_mappings(),
    % 返回初始化状态
    {ok, #{}}
  catch
    Error:Reason ->
      logger:error("映射的gen_server进程初始化失败: ~p:~p", [Error, Reason]),
      {stop, {init_failed, Reason}}
  end.

% 处理同步调用：获取或创建 PID
handle_call({get_or_create_pid, Key}, _From, State) ->
  try
    % 查询 ETS 表以获取键对应的 PID
    case ets:lookup(?ETS_TABLE, Key) of
      % 如果找到，返回现有 PID
      [{Key, Pid}] ->
        {reply, {ok, Pid}, State};
      % 如果未找到，创建新存储进程
      [] ->
        % 启动 game_cache_store 进程
        {ok, Pid} = game_cache_store:start_link(Key),
        % 将键和 PID 插入 ETS 表
        ets:insert(?ETS_TABLE, {Key, Pid}),
        % 返回新创建的 PID 和状态
        {reply, {ok, Pid}, State}
    end
  catch
    Error:Reason ->
      logger:error("获取或创建键 ~p 的PID失败: ~p:~p", [Key, Error, Reason]),
      {reply, {error, Reason}, State}
  end;
% 处理同步调用：批量获取或创建 PID
handle_call({get_or_create_pids, Keys}, _From, State) ->
  try
    % 对每个键获取或创建 PID
    Results = [{Key, case ets:lookup(?ETS_TABLE, Key) of
                       [{Key, Pid}] ->
                         {ok, Pid};
                       [] ->
                         {ok, Pid} = game_cache_store:start_link(Key),
                         ets:insert(?ETS_TABLE, {Key, Pid}),
                         {ok, Pid}
                     end} || Key <- Keys],
    % 返回批量结果和状态，格式为 {ok, [{Key, {ok, Pid}}]}
    {reply, {ok, Results}, State}
  catch
    Error:Reason ->
      logger:error("批量获取或创建键 ~p 的PID失败: ~p:~p", [Keys, Error, Reason]),
      {reply, {error, Reason}, State}
  end;
% 处理同步调用：删除映射
handle_call({delete, Key}, _From, State) ->
  try
    % 查询 ETS 表以检查键是否存在
    case ets:lookup(?ETS_TABLE, Key) of
      % 如果找到，删除键的映射
      [{Key, _Pid}] ->
        % 从 ETS 表中删除键
        ets:delete(?ETS_TABLE, Key),
        % 返回 ok 和状态
        {reply, ok, State};
      % 如果键不存在，直接返回 ok
      [] ->
        {reply, ok, State}
    end
  catch
    Error:Reason ->
      logger:error("删除键 ~p 的映射失败: ~p:~p", [Key, Error, Reason]),
      {reply, {error, Reason}, State}
  end;
% 处理同步调用：批量删除映射
handle_call({delete_batch, Keys}, _From, State) ->
  try
    % 对每个键删除 ETS 表中的映射
    [ets:delete(?ETS_TABLE, Key) || Key <- Keys, ets:member(?ETS_TABLE, Key)],
    % 返回 ok 和状态
    {reply, ok, State}
  catch
    Error:Reason ->
      logger:error("批量删除键 ~p 的映射失败: ~p:~p", [Keys, Error, Reason]),
      {reply, {error, Reason}, State}
  end.

% 处理异步消息，当前未使用
handle_cast(_Msg, State) ->
  {noreply, State}.

% 处理其他消息，当前未使用
handle_info(_Info, State) ->
  {noreply, State}.

% 进程终止时执行，参数为终止原因和状态
terminate(_Reason, _State) ->
  % 删除 ETS 表以清理资源
  ets:delete(?ETS_TABLE),
  ok.

% 处理代码热更新，参数为旧版本、状态和额外数据
code_change(_OldVsn, State, _Extra) ->
  % 返回更新后的状态
  {ok, State}.

% 模拟加载初始映射数据
load_initial_mappings() ->
  try
    % 定义初始键值对列表
    InitialData = [
      {user1, 100},
      {user2, 200}
    ],
    % 遍历初始数据，为每个键值对创建存储进程并设置值
    lists:foreach(
      fun({Key, Value}) ->
        % 启动 game_cache_store 进程
        {ok, Pid} = game_cache_store:start_link(Key),
        % 将键和 PID 插入 ETS 表
        ets:insert(?ETS_TABLE, {Key, Pid}),
        % 设置初始值
        ok = game_cache_store:set(Pid, Value),
        % 记录加载日志
        logger:info("加载初始映射数据: key ~p, value ~p", [Key, Value])
      end,
      InitialData
    ),
    % 返回 ok 表示加载成功
    ok
  catch
    Error:Reason ->
      logger:error("加载初始映射数据失败: ~p:~p", [Error, Reason]),
      {error, Reason}
  end.


