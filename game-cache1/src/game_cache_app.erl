%%%-------------------------------------------------------------------
%% @doc game_cache public API
%% @end
%%%-------------------------------------------------------------------

-module(game_cache_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    game_cache_sup:start_link().

stop(_State) ->
    ok.

