%%%-------------------------------------------------------------------
%% @doc game_cache top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(game_cache_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  % 定义监督策略：one_for_all，重启强度为 5 次，周期为 10 秒
  SupFlags = #{
    strategy => one_for_all,
    intensity => 5,
    period => 10
  },
  ChildSpecs = [
    #{
      % 子进程 ID
      id => game_cache_mapping,
      % 启动函数
      start => {game_cache_mapping, start_link, []},
      % 重启策略为 permanent，进程总是重启
      restart => permanent,
      % 关闭超时时间为 5 秒
      shutdown => 5000,
      % 进程类型为 worker
      type => worker,
      % 相关模块
      modules => [game_cache_mapping]
    }
    % game_cache_store 进程由 game_cache_mapping 动态创建，无需在此定义
  ],
  {ok, {SupFlags, ChildSpecs}}.

