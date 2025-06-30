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
%% 公共接口
-export([start_link/0, get_pid/1, get_pids/1, set/2, set_batch/1, delete/1, delete_batch/1, check_consistency/0, check_consistency/1, handle_file_cleanup/1]).
%% gen_server 回调函数
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(ETS_TABLE, game_cache_mapping).
-define(CONSISTENCY_CHECK_INTERVAL, 60000). % 每60秒做一次一致性检查
-define(DATA_DIR, "data/cache"). % 数据文件存储目录

-record(state, {monitors = #{}}). % 键到 Monitor 的映射



% ======================================================================================================================
%      以下为公共接口
% ======================================================================================================================



% 启动映射进程
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% 获取指定键对应的数据文件路径
get_pid(Key) ->
  try
    case ets:lookup(?ETS_TABLE, Key) of
      [{Key, FilePath}] ->
        case filelib:is_file(FilePath) of
          true -> {ok, FilePath};
          false ->
            ets:delete(?ETS_TABLE, Key),
            logger:warning("移除了键 ~p 的无效文件路径 ~p", [Key, FilePath]),
            {error, not_found}
        end;
      [] ->
        {error, not_found}
    end
  catch
    Error:Reason:Stacktrace ->
      logger:error("获取键 ~p 的文件路径失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
      {error, Reason}
  end.

% 批量获取多个键对应的文件路径
get_pids(Keys) ->
  try
    Results = [{Key, case ets:lookup(?ETS_TABLE, Key) of
                       [{Key, FilePath}] ->
                         case filelib:is_file(FilePath) of
                           true -> {ok, FilePath};
                           false ->
                             ets:delete(?ETS_TABLE, Key),
                             logger:warning("移除了键 ~p 的无效文件路径 ~p", [Key, FilePath]),
                             {error, not_found}
                         end;
                       [] ->
                         {error, not_found}
                     end} || Key <- Keys],
    Results
  catch
    Error:Reason:Stacktrace ->
      logger:error("批量获取键 ~p 的文件路径失败: ~p:~p, stacktrace: ~p", [Keys, Error, Reason, Stacktrace]),
      {error, Reason}
  end.

% 设置键值，直接写入文件
set(Key, Value) ->
  gen_server:call(?MODULE, {set, Key, Value}).

% 批量设置键值，直接写入文件
set_batch(KeyValueList) ->
  gen_server:call(?MODULE, {set_batch, KeyValueList}).

% 删除单个键及其数据文件
delete(Key) ->
  % 调用 gen_server:call 同步请求删除键的映射
  gen_server:call(?MODULE, {delete, Key}).

% 批量删除多个键及其数据文件
delete_batch(Keys) ->
  % 调用 gen_server:call 同步请求批量删除键的映射
  gen_server:call(?MODULE, {delete_batch, Keys}).

% 进行一致性检查（全表检查）
check_consistency() ->
  gen_server:cast(?MODULE, {check_consistency, all}).

% 进行一致性检查（针对性检查）
check_consistency(Keys) when is_list(Keys) ->
  gen_server:cast(?MODULE, {check_consistency, Keys}).

% 清理无效文件
handle_file_cleanup(Key) ->
  gen_server:cast(?MODULE, {file_cleanup, Key}).


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
      logger:error("初始化 throw: ~p, stacktrace: ~p", [Value, Stacktrace]),
      {stop, {init_failed, {thrown, Value}}};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("初始化 error: ~p, stacktrace: ~p", [Reason, Stacktrace]),
      {stop, {init_failed, Reason}};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("初始化 exited: ~p, stacktrace: ~p", [Reason, Stacktrace]),
      {stop, {init_failed, {exit, Reason}}}
  end.

% 核心逻辑处理初始化
do_init([]) ->
  % 创建数据目录
  ok = filelib:ensure_dir(?DATA_DIR ++ "/"),
  logger:info("ETS 表状态: ~p", [ets:info(?ETS_TABLE)]),
  % 确保 ETS 表不存在，或者重新创建
  case ets:info(?ETS_TABLE) of
    undefined ->
      _ = ets:new(?ETS_TABLE, [set, protected, named_table, {write_concurrency, true}]);
    _ ->
      ets:delete(?ETS_TABLE),
      _ = ets:new(?ETS_TABLE, [set, protected, named_table, {write_concurrency, true}])
  end,
  % 加载初始映射数据
  ok = load_initial_mappings(),
  % 启动定时器，定期检查一致性（每60秒）
  timer:send_interval(?CONSISTENCY_CHECK_INTERVAL, trigger_check_consistency),
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
  % 生成文件路径
  FilePath = get_file_path(Key),
  case ets:lookup(?ETS_TABLE, Key) of
    [{Key, FilePath}] ->
      try
        ok = write_file(FilePath, Value),
        {reply, ok, State}
      catch
        Error:Reason:Stacktrace ->
          logger:error("设置键 ~p 失败, 文件 ~p: ~p:~p, stacktrace: ~p", [Key, FilePath, Error, Reason, Stacktrace]),
          NewState = do_check_consistency(State, [Key]),
          {reply, {error, {set_failed, Reason}}, NewState}
      end;
    [] ->
      try
        ok = write_file(FilePath, Value),
        true = ets:insert(?ETS_TABLE, {Key, FilePath}),
        {reply, ok, State}
      catch
        Error:Reason:Stacktrace ->
          case ets:lookup(?ETS_TABLE, Key) of
            % 如果出现异常且此时存在ets表的映射以及键对应的文件，则执行回滚操作
            [{Key, FilePath}] ->
              file:delete(FilePath),
              ets:delete(?ETS_TABLE, Key),
              logger:info("回滚: 删除键 ~p 的文件和ETS映射", [Key]);
            [] -> ok
          end,
          logger:error("原子设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
          NewState = do_check_consistency(State, [Key]),
          {reply, {error, {set_failed, Reason}}, NewState}
      end
  end;
% 处理同步调用：批量设置映射
do_handle_call({set_batch, KeyValueList}, _From, #state{monitors = Monitors} = State) ->
  % 批量设置键值对，逐键处理，失败不影响其他键
  {Results, NewMonitors} = lists:foldl(
    fun({Key, Value}, {Acc, MonAcc}) ->
      FilePath = get_file_path(Key),
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, FilePath}] ->
          try
            ok = write_file(FilePath, Value),
            {[{Key, ok} | Acc], MonAcc}
          catch
            Error:Reason:Stacktrace ->
              logger:error("批量设置键 ~p 失败: ~p:~p, stacktrace: ~p", [Key, Error, Reason, Stacktrace]),
              {[{Key, {error, {set_failed, Reason}}} | Acc], MonAcc}
          end;
        [] ->
          try
            ok = write_file(FilePath, Value),
            true = ets:insert(?ETS_TABLE, {Key, FilePath}),
            {[{Key, ok} | Acc], MonAcc}
          catch
            Error:Reason:Stacktrace ->
              case ets:lookup(?ETS_TABLE, Key) of
                [{Key, FilePath}] ->
                  file:delete(FilePath),
                  ets:delete(?ETS_TABLE, Key),
                  logger:info("回滚: 删除键 ~p 的文件和ETS映射", [Key]);
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
  {reply, {ok, lists:reverse(Results)}, NewState};
% 处理同步调用：删除映射
do_handle_call({delete, Key}, _From, #state{monitors = Monitors} = State) ->
  case ets:lookup(?ETS_TABLE, Key) of
    [{Key, FilePath}] ->
      try
        ok = file:delete(FilePath),
        true = ets:delete(?ETS_TABLE, Key),
        NewMonitors = maps:remove(Key, Monitors),
        {reply, ok, State#state{monitors = NewMonitors}}
      catch
        Error:Reason:Stacktrace ->
          logger:error("原子删除键 ~p 失败, 文件 ~p: ~p:~p, stacktrace: ~p", [Key, FilePath, Error, Reason, Stacktrace]),
          NewState = do_check_consistency(State, [Key]),
          {reply, {error, {delete_failed, Reason}}, NewState}
      end;
    [] ->
      {reply, ok, State}
  end;
% 处理同步调用：批量删除映射
do_handle_call({delete_batch, Keys}, _From, #state{monitors = Monitors} = State) ->
  % 批量删除键及其文件
  {Results, NewMonitors} = lists:foldl(
    fun(Key, {Acc, MonAcc}) ->
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, FilePath}] ->
          try
            ok = file:delete(FilePath),
            true = ets:delete(?ETS_TABLE, Key),
            {[{Key, ok} | Acc], maps:remove(Key, MonAcc)}
          catch
            Error:Reason:Stacktrace ->
              logger:error("原子批量删除键 ~p 失败, 文件 ~p: ~p:~p, stacktrace: ~p", [Key, FilePath, Error, Reason, Stacktrace]),
              {[{Key, {error, {delete_failed, Reason}}} | Acc], MonAcc}
          end;
        [] ->
          {[{Key, ok} | Acc], MonAcc}
      end
    end,
    {[], Monitors},
    Keys
  ),
  NewState = do_check_consistency(State#state{monitors = NewMonitors}, Keys),
  {reply, {ok, lists:reverse(Results)}, NewState};
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
% 处理文件清理
do_handle_cast({file_cleanup, Key}, #state{monitors = Monitors} = State) ->
  case ets:lookup(?ETS_TABLE, Key) of
    [{Key, FilePath}] ->
      file:delete(FilePath),
      ets:delete(?ETS_TABLE, Key),
      logger:info("移除键 ~p 的文件和ETS映射", [Key]),
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
do_handle_info(trigger_check_consistency, State) ->
  gen_server:cast(?MODULE, {check_consistency, all}),
  {noreply, State};
% 处理其他消息
do_handle_info(Msg, State) ->
  % 处理未知消息，记录警告
  logger:warning("未知 info 消息: ~p, state ~p", [Msg, State]),
  {noreply, State}.


% ======================================================================================================================

% 进程终止时执行，参数为终止原因和状态
terminate(_Reason, _State) ->
  % 删除 ETS 表以清理资源
  ets:delete(?ETS_TABLE),
  % 清理数据目录
  case filelib:is_dir(?DATA_DIR) of
    true -> file:del_dir_r(?DATA_DIR);
    false -> ok
  end,
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
extract_key({file_cleanup, Key}) -> Key;
extract_key(_) -> undefined.

% 生成文件路径
get_file_path(Key) ->
  FileName = io_lib:format("~p.dat", [Key]),
  FileNameStr = lists:flatten(FileName),
  filename:join(?DATA_DIR, FileNameStr).

% 写入文件
write_file(FilePath, Value) ->
  Binary = term_to_binary(Value),
  % exclusive：仅在文件不存在时写入，若文件已存在则报错
  file:write_file(FilePath, Binary).

% 一致性检查具体逻辑(全表检查)
do_check_consistency(#state{monitors = Monitors} = State,all) ->
  RemovedKeys = ets:foldl(
    fun({Key, FilePath}, Acc) ->
      case filelib:is_file(FilePath) of
        true -> Acc;
        false ->
          ets:delete(?ETS_TABLE, Key),
          logger:warning("全表检查: 移除了键 ~p 的无效文件映射 ~p", [Key, FilePath]),
          [Key | Acc]
      end
    end,
    [],
    ?ETS_TABLE
  ),
  NewMonitors = maps:without(RemovedKeys, Monitors),
  State#state{monitors = NewMonitors};
% 一致性检查具体逻辑(针对性检查)
do_check_consistency(#state{monitors = Monitors} = State, Keys) when is_list(Keys) ->
  RemovedKeys = lists:foldl(
    fun(Key, Acc) ->
      case ets:lookup(?ETS_TABLE, Key) of
        [{Key, FilePath}] ->
          case filelib:is_file(FilePath) of
            true -> Acc;
            false ->
              ets:delete(?ETS_TABLE, Key),
              logger:warning("针对性检查: 移除了键 ~p 的无效文件映射 ~p", [Key, FilePath]),
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
        % 获取文件路径
        FilePath = get_file_path(Key),
        % 将对应的值写入文件
        ok = write_file(FilePath, Value),
        % 在ets表中加入对应的映射
        true = ets:insert(?ETS_TABLE, {Key, FilePath}),
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


