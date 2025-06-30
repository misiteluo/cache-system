%%%-------------------------------------------------------------------
%%% @author luoruiyu
%%% @copyright (C) 2025, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 6月 2025 17:06
%%%-------------------------------------------------------------------

% 此模块为值存储进程模块
-module(game_cache_store).
-author("luoruiyu").

-behaviour(gen_server).

%% API
-export([start_link/1, get/1, set/2, delete/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
% 定义状态记录，包含键和值
-record(state, {key, value}).

% 启动值存储进程，参数为键
start_link(Key) ->
  Name = list_to_atom("game_cache_store_" ++ atom_to_list(Key)),
  gen_server:start_link({local, Name}, ?MODULE, [Key], []).

% 获取值，参数为存储进程 PID
get(Pid) ->
  % 调用 gen_server:call 向存储进程请求值
  gen_server:call(Pid, get).

% 设置值，参数为存储进程 PID 和值
set(Pid, Value) ->
  % 调用 gen_server:call 向存储进程设置值
  gen_server:call(Pid, {set, Value}).

% 删除存储进程，参数为存储进程 PID
delete(Pid) ->
  % 调用 gen_server:stop 停止存储进程
  gen_server:stop(Pid).



% 初始化 gen_server，参数为键
init([Key]) ->
  % 返回初始化状态，值初始为 undefined
  {ok, #state{key = Key, value = undefined}}.

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
      {reply, {error, Reason}, State};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理调用退出: ~p, state ~p: ~p, stacktrace: ~p", [Request, State, Reason, Stacktrace]),
      {reply, {error, {exit, Reason}}, State}
  end.

% 核心逻辑处理同步调用
do_handle_call(get, _From, #state{value = Value} = State) ->
  % 处理 get 请求，返回当前值
  {reply, {ok, Value}, State};
do_handle_call({set, Value}, _From, State) ->
  % 处理 set 请求，异步保存到数据库
  spawn(fun() -> save_to_db(State#state.key, Value) end),
  {reply, ok, State#state{value = Value}};
do_handle_call(Request, _From, State) ->
  % 处理未知请求，记录警告并返回错误
  logger:warning("未知 call 请求 ~p, state ~p", [Request, State]),
  {reply, {error, unknown_request}, State}.


% 处理异步消息，此处未使用
handle_cast(Msg, State) ->
  try
    % 调用 do_handle_cast 处理具体逻辑
    do_handle_cast(Msg, State)
  catch
    throw:Value:Stacktrace ->
      % 记录 throw 异常日志
      logger:error("H处理异步消息异常: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Value, Stacktrace]),
      {noreply, State};
    error:Reason:Stacktrace ->
      % 记录 error 异常日志
      logger:error("处理异步消息错误: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      {noreply, State};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理异步消息退出: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      {noreply, State}
  end.

% 核心逻辑处理异步消息
do_handle_cast(Msg, State) ->
  % 处理未知异步消息，记录警告
  logger:warning("未知 cast 消息 ~p, state ~p", [Msg, State]),
  {noreply, State}.

% 处理其他消息，此处未使用
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
      {noreply, State};
    exit:Reason:Stacktrace ->
      % 记录 exit 异常日志
      logger:error("处理消息退出: ~p, state ~p: ~p, stacktrace: ~p", [Msg, State, Reason, Stacktrace]),
      {noreply, State}
  end.

% 核心逻辑处理其他消息
do_handle_info(Msg, State) ->
  % 处理未知消息，记录警告
  logger:warning("未知 info 消息 ~p, state ~p", [Msg, State]),
  {noreply, State}.

% 进程终止时执行，参数为终止原因和状态
terminate(_Reason, #state{key = Key, value = Value}) ->
  % 进程终止时，需要移除 game_cache_mapping 中的ets映射
  game_cache_mapping:handle_store_termination(Key),
  % 模拟将键值对保存到数据库
  save_to_db(Key, Value),
  ok.

% 处理代码热更新，当前未使用
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.



% 模拟数据库保存操作，参数为键和值
save_to_db(Key, Value) ->
  % 使用 logger 记录保存操作日志
  logger:info("已将键：~p 值：~p 存储到数据库中", [Key,Value]),
  ok.
