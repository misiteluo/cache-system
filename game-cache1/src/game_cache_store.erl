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
  gen_server:start_link(?MODULE, [Key], []).

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

% 处理同步调用：获取值
handle_call(get, _From, #state{value = Value} = State) ->
  % 返回值和当前状态
  {reply, {ok, Value}, State};
% 处理同步调用：设置值
handle_call({set, Value}, _From, State) ->
  % 异步执行数据库保存操作
  spawn(fun() -> save_to_db(State#state.key, Value) end),
  {reply, ok, State#state{value = Value}}.

% 处理异步消息，当前未使用
handle_cast(_Msg, State) ->
  {noreply, State}.

% 处理其他消息，当前未使用
handle_info(_Info, State) ->
  {noreply, State}.

% 进程终止时执行，参数为终止原因和状态
terminate(_Reason, #state{key = Key, value = Value}) ->
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
