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
-export([start_link/0, get_pid/1, get_pids/1, set/2, set_batch/1, delete/1, delete_batch/1, check_consistency/0, check_consistency/1, handle_store_termination/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(ETS_TABLE, game_cache_mapping).
-define(CONSISTENCY_CHECK_INTERVAL, 60000). % 每60秒做一次一致性检查

-record(state, {monitors = #{}}). % 键到 Monitor 的映射



% ======================================================================================================================
%      以下为公共接口
% ======================================================================================================================



% 启动映射进程
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% 获取键对应的存储进程 PID，参数为键
get_pid(Key) ->
  try
    % 查询 ETS 表以获取键对应的 PID
    case ets:lookup(?ETS_TABLE, Key) of
      % 如果找到，且Pid存在则返回 PID，否则移除ets表中对应的PID
      [{Key, Pid}] ->
        case is_process_alive(Pid) of
          true -> {ok, Pid};
          false ->
            ets:delete(?ETS_TABLE, Key),
            logger:warning("移除了键 ~p 的无效PID ~p", [Key, Pid]),
            {error, not_found}
        end;
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
                       [{Key, Pid}] ->
                         case is_process_alive(Pid) of
                           true -> {ok, Pid};
                           false ->
                             ets:delete(?ETS_TABLE, Key),
                             logger:warning("移除了键 ~p 的无效PID ~p", [Key, Pid]),
                             {error, not_found}
                         end;
                       [] -> {error, not_found}
                     end} || Key <- Keys],
    % 返回批量结果，格式为 [{Key, {ok, Pid} | {error, not_found}}]
    Results
  catch
    Error:Reason ->
      logger:error("批量获取键 ~p 失败: ~p:~p", [Keys, Error, Reason]),
      {error, Reason}
  end.

% 设置键值
set(Key, Value) ->
  gen_server:call(?MODULE, {set, Key, Value}).

% 批量设置键值
set_batch(KeyValueList) ->
  gen_server:call(?MODULE, {set_batch, KeyValueList}).

% 删除键的映射，参数为键
delete(Key) ->
  % 调用 gen_server:call 同步请求删除键的映射
  gen_server:call(?MODULE, {delete, Key}).

% 批量删除键的映射，参数为键列表
delete_batch(Keys) ->
  % 调用 gen_server:call 同步请求批量删除键的映射
  gen_server:call(?MODULE, {delete_batch, Keys}).

% 进行一致性检查（全表检查）
check_consistency() ->
  gen_server:cast(?MODULE, {check_consistency, all}).

% 进行一致性检查（针对性检查）
check_consistency(Keys) when is_list(Keys) ->
  gen_server:cast(?MODULE, {check_consistency, Keys}).

handle_store_termination(Key) ->
  gen_server:cast(?MODULE, {store_terminated, Key}).



% ======================================================================================================================
%      以下为gen_server回调函数
% ======================================================================================================================



% 初始化 gen_server
init(Args) ->
  try
    % 调用 do_init 处理初始化逻辑
    do_init(Args)
  catch
    throw:Value:Stacktrace ->
      % 记录 throw 异常日志
      logger:error("Init throw: ~p, stacktrace: ~p", [Value, Stacktrace]),
      {stop, {init_failed, {thrown, Value}}};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("Init error: ~p, stacktrace: ~p", [Reason, Stacktrace]),
      {stop, {init_failed, Reason}};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("Init exited: ~p, stacktrace: ~p", [Reason, Stacktrace]),
      {stop, {init_failed, {exit, Reason}}}
  end.

% 核心逻辑处理初始化
do_init([]) ->
  % 创建 ETS 表，设置属性为 set、protected、named_table，并启用写并发
  ?ETS_TABLE = ets:new(?ETS_TABLE, [set, protected, named_table, {write_concurrency, true}]),
  % 加载初始映射数据
  ok = load_initial_mappings(),
  % 启动定时器，定期检查一致性（每60秒）
  timer:send_interval(?CONSISTENCY_CHECK_INTERVAL, check_consistency),
  % 返回初始化状态
  {ok, #state{}}.


% ======================================================================================================================

% 处理同步调用
handle_call(Request, From, State) ->
  try
    % 调用 do_handle_call 处理具体逻辑
    do_handle_call(Request, From, State)
  catch
    throw:Value:Stacktrace ->
      % 记录 throw 异常日志
      logger:error("处理调用异常: ~p, state ~p: ~p, stacktrace: ~p", [Request, State, Value, Stacktrace]),
      {reply, {error, {thrown, Value}}, State};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("处理调用错误: ~p, state ~p: ~p, stacktrace: ~p", [Request, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Request)]),
      {reply, {error, Reason}, NewState};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理调用退出: ~p, state ~p: ~p, stacktrace: ~p", [Request, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Request)]),
      {reply, {error, {exit, Reason}}, NewState}
  end.

% 处理同步调用：设置映射
do_handle_call({set, Key, Value}, _From, #state{monitors = Monitors} = State) ->
  case ets:lookup(?ETS_TABLE, Key) of
    % 如果ets表中有对应的pid，则为修改value操作
    [{Key, Pid}] ->
      try
        ok = game_cache_store:set(Pid, Value),
        {reply, ok, State}
      catch
        Error:Reason:Stacktrace ->
          logger:error("设置键 ~p 失败, pid ~p: ~p:~p, stacktrace: ~p", [Key, Pid, Error, Reason, Stacktrace]),
          NewState = do_check_consistency(State,[Key]),
          {reply, {error, {set_failed, Reason}}, NewState}
      end;
    % 如果ets表中没有对应的pid，则为创建新的pid并设置value操作
    [] ->
      try
        {ok, NewPid} = game_cache_store:start_link(Key),
        % 使用 erlang:monitor 来监控新创建的NewPid，当NewPid退出时向当前gen_server进程发送{'DOWN', Ref, process, Pid, Reason}消息
        Ref = erlang:monitor(process, NewPid),
        true = ets:insert(?ETS_TABLE, {Key, NewPid}),
        ok = game_cache_store:set(NewPid, Value),
        % 将对应的 Key => Ref 映射保存在状态中
        {reply, ok, State#state{monitors = Monitors#{Key => Ref}}}
      catch
        Error:Reason:Stacktrace ->
          case ets:lookup(?ETS_TABLE, Key) of
            [{Key, Pid}] ->
              game_cache_store:delete(Pid),
              ets:delete(?ETS_TABLE, Key),
              logger:info("回滚: 删除键 ~p 的进程和ETS映射", [Key]);
            [] -> ok
          end,
          logger:error("原子设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
          % 进行一致性检查（针对性检查）
          NewState = do_check_consistency(State, [Key]),
          {reply, {error, {set_failed, Reason}}, NewState}
      end
  end;
do_handle_call({set_batch, KeyValueList}, _From, #state{monitors = Monitors} = State) ->
  % 批量设置键值对，逐键处理，失败不影响其他键
  {Results, NewMonitors} = lists:foldl(
    fun({Key, Value}, {Acc, MonAcc}) ->
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, Pid}] ->
          try
            ok = game_cache_store:set(Pid, Value),
            {[{Key, ok} | Acc], MonAcc}
          catch
            Error:Reason:Stacktrace ->
              logger:error("批量设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
              {[{Key, {error, {set_failed, Reason}}} | Acc], MonAcc}
          end;
        [] ->
          try
            {ok, NewPid} = game_cache_store:start_link(Key),
            Ref = erlang:monitor(process, NewPid),
            true = ets:insert(?ETS_TABLE, {Key, NewPid}),
            ok = game_cache_store:set(NewPid, Value),
            {[{Key, ok} | Acc], MonAcc#{Key => Ref}}
          catch
            Error:Reason:Stacktrace ->
              % 回滚：清理可能创建的进程和 ETS 映射
              case ets:lookup(?ETS_TABLE, Key) of
                [{Key, Pid}] ->
                  game_cache_store:delete(Pid),
                  ets:delete(?ETS_TABLE, Key),
                  logger:info("回滚: 删除键 ~p 的进程和ETS映射", [Key]);
                [] -> ok
              end,
              logger:error("原子批量设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
              {[{Key, {error, {set_failed, Reason}}} | Acc], MonAcc}
          end
      end
    end,
    {[], Monitors},
    KeyValueList
  ),
  Keys = [Key || {Key, _} <- KeyValueList],
  % 更新状态：使用 NewMonitors，并执行一致性检查（针对性检查）
  NewState = do_check_consistency(State#state{monitors = NewMonitors}, Keys),
  {reply, {ok, Results}, NewState};
% 处理同步调用：删除映射
do_handle_call({delete, Key}, _From, #state{monitors = Monitors} = State) ->
  case ets:lookup(?ETS_TABLE, Key) of
    [{Key, Pid}] ->
      try
        ok = game_cache_store:delete(Pid),
        true = ets:delete(?ETS_TABLE, Key),
        % 删除对应的 Key => Ref 映射并返回新映射
        NewMonitors = maps:remove(Key, Monitors),
        {reply, ok, State#state{monitors = NewMonitors}}
      catch
        Error:Reason:Stacktrace ->
          logger:error("原子删除键 ~p 失败, pid ~p: ~p:~p, stacktrace: ~p", [Key, Pid, Error, Reason, Stacktrace]),
          % 进行一致性检查（针对性检查）
          NewState = do_check_consistency(State, [Key]),
          {reply, {error, {delete_failed, Reason}}, NewState}
      end;
    [] ->
      {reply, ok, State}
  end;
% 处理同步调用：批量删除映射
do_handle_call({delete_batch, Keys}, _From, #state{monitors = Monitors} = State) ->
  % 批量删除键及其进程
  {Results, NewMonitors} = lists:foldl(
    fun(Key, {Acc, MonAcc}) ->
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, Pid}] ->
          try
            ok = game_cache_store:delete(Pid),
            true = ets:delete(?ETS_TABLE, Key),
            {[{Key, ok} | Acc], maps:remove(Key, MonAcc)}
          catch
            Error:Reason:Stacktrace ->
              logger:error("原子批量删除键 ~p 失败, pid ~p: ~p:~p, stacktrace: ~p", [Key, Pid, Error, Reason, Stacktrace]),
              {[{Key, {error, {delete_failed, Reason}}} | Acc], MonAcc}
          end;
        [] ->
          {[{Key, ok} | Acc], MonAcc}
      end
    end,
    {[], Monitors},
    Keys
  ),
  % 更新状态：使用 NewMonitors，并执行一致性检查（针对性检查）
  NewState = do_check_consistency(State#state{monitors = NewMonitors}, Keys),
  {reply, {ok, Results}, NewState};
do_handle_call(Request, _From, State) ->
  % 处理未知请求，记录警告
  logger:warning("未知 call 请求 ~p, state ~p", [Request, State]),
  {reply, {error, unknown_request}, State}.


% ======================================================================================================================

% 处理异步消息
handle_cast(Msg, State) ->
  try
    % 调用 do_handle_cast 处理具体逻辑
    do_handle_cast(Msg, State)
  catch
    throw:Value:Stacktrace ->
      % 记录 throw 异常日志
      logger:error("处理异步消息异常: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Value, Stacktrace]),
      {noreply, State};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("处理异步消息错误: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Msg)]),
      {noreply, NewState};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理异步消息退出: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Msg)]),
      {noreply, NewState}
  end.

% 处理一致性检查,在 do_check_consistency 中实现具体逻辑
do_handle_cast({check_consistency, all}, State) ->
  {noreply, do_check_consistency(State, all)};
do_handle_cast({check_consistency, Keys}, State) when is_list(Keys) ->
  {noreply, do_check_consistency(State, Keys)};
% 当 game_cache_store 的 gen_server 进程终止时,需要移除对应的ets映射
do_handle_cast({store_terminated, Key}, #state{monitors = Monitors} = State) ->
  case ets:lookup(?ETS_TABLE, Key) of
    [{Key, _Pid}] ->
      ets:delete(?ETS_TABLE, Key),
      logger:info("移除终止进程的键 ~p 的ETS映射", [Key]),
      {noreply, State#state{monitors = maps:remove(Key, Monitors)}};
    [] ->
      {noreply, State#state{monitors = maps:remove(Key, Monitors)}}
  end;
% 核心逻辑处理异步消息
do_handle_cast(Msg, State) ->
  % 处理未知异步消息，记录警告
  logger:warning("未知 cast 消息 ~p, state ~p", [Msg, State]),
  {noreply, State}.


% ======================================================================================================================

% 处理其他消息
handle_info(Msg, State) ->
  try
    % 调用 do_handle_info 处理具体逻辑
    do_handle_info(Msg, State)
  catch
    throw:Value:Stacktrace ->
      % 记录 throw 异常日志
      logger:error("处理消息异常: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Value, Stacktrace]),
      {noreply, State};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("处理消息错误: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Msg)]),
      {noreply, NewState};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理消息退出: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      NewState = do_check_consistency(State, [extract_key(Msg)]),
      {noreply, NewState}
  end.

% 处理定时一致性检查（全表检查）
do_handle_info(check_consistency, State) ->
  gen_server:cast(?MODULE, {check_consistency,all}),
  {noreply, State};
% 当有 Pid 退出时执行的逻辑
do_handle_info({'DOWN', Ref, process, Pid, _Reason}, #state{monitors = Monitors} = State) ->
  % 查询当前的 monitors 映射中是否有退出的 pid（Ref）
  case lists:keyfind(Ref, 2, maps:to_list(Monitors)) of
    {Key, Ref} ->
      ets:delete(?ETS_TABLE, Key),
      logger:info("移除崩溃监控的键 ~p 的ETS映射, pid ~p", [Key, Pid]),
      {noreply, State#state{monitors = maps:remove(Key, Monitors)}};
    false ->
      {noreply, State}
  end;
% 核心逻辑处理其他消息
do_handle_info(Msg, State) ->
  % 处理未知消息，记录警告
  logger:warning("未知 info 消息: ~p, state ~p", [Msg, State]),
  {noreply, State}.


% ======================================================================================================================

% 进程终止时执行，参数为终止原因和状态
terminate(_Reason, _State) ->
  % 删除 ETS 表以清理资源
  ets:delete(?ETS_TABLE),
  ok.


% 处理代码热更新，参数为旧版本、状态和额外数据
code_change(_OldVsn, State, _Extra) ->
  % 返回更新后的状态
  {ok, State}.


% ======================================================================================================================
%      以下为内部函数
% ======================================================================================================================


% 提取请求中的键
extract_key({set, Key, _}) -> Key;
extract_key({delete, Key}) -> Key;
extract_key({set_batch, KeyValueList}) -> [Key || {Key, _} <- KeyValueList];
extract_key({delete_batch, Keys}) -> Keys;
extract_key({check_consistency, Keys}) -> Keys;
extract_key({store_terminated, Key}) -> Key;
extract_key(_) -> undefined.

% 一致性检查具体逻辑(全表检查)
do_check_consistency(#state{monitors = Monitors} = State,all) ->
  % 首先移除所有无效的 Pid 并将移除的所有对应的键记录为RemovedKeys
  RemovedKeys = ets:foldl(
    fun({Key, Pid}, Acc) ->
      case is_process_alive(Pid) of
        true -> Acc;
        false ->
          ets:delete(?ETS_TABLE, Key),
          logger:warning("全表检查: 移除了键 ~p 的无效PID映射, pid ~p", [Key, Pid]),
          [Key | Acc]
      end
    end,
    [],
    ?ETS_TABLE
  ),
  % 从 Monitors 中移除所有 RemovedKeys 的映射，然后返回新的状态
  NewMonitors = maps:without(RemovedKeys, Monitors),
  State#state{monitors = NewMonitors};
% 一致性检查具体逻辑(针对性检查)
do_check_consistency(#state{monitors = Monitors} = State, Keys) when is_list(Keys) ->
  RemovedKeys = lists:foldl(
    fun(Key, Acc) ->
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, Pid}] ->
          case is_process_alive(Pid) of
            true -> Acc;
            false ->
              ets:delete(?ETS_TABLE, Key),
              logger:warning("针对性检查: 移除了键 ~p 的无效PID映射, pid ~p", [Key, Pid]),
              [Key | Acc]
          end;
        [] ->
          Acc
      end
    end,
    [],
    Keys
  ),
  NewMonitors = maps:without(RemovedKeys, Monitors),
  State#state{monitors = NewMonitors};
% 处理无效参数的一致性检查
do_check_consistency(State, _Invalid) ->
  logger:warning("无效一致性检查参数，跳过"),
  State.



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


