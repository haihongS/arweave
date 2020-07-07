%%%-------------------------------------------------------------------
%%% @author sherlock
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Jul 2020 17:45
%%%-------------------------------------------------------------------
-module(ar_stratum_mine).
-author("sherlock").

%% API
-export([]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TCP_HOST, "matpoo.io").
-define(TCP_PORT, 12345).
-define(TCP_OPTIONS, [binary, {packet, raw}, {active, true}]).

-record(miner_state, {
  data_segment,
  modified_diff,
  height,
  timestamp,
  max_miners = ?NUM_MINING_PROCESSES, % max mining process to start
  miners = [] % miner worker processes
}).

main() ->
  SockPid = spawn(?MODULE, get_sock_id, [self()]),
  mainloop(),
  ok.

mainloop() ->
  receive
    {got_sock, SockPid} ->
      io:format("haha"),
      ok
  end,
  ok.

get_sock_id(Supervisor) ->
  case gen_tcp:connect(?TCP_HOST, ?TCP_PORT, ?TCP_OPTIONS, 5000) of
    {ok, Sock} ->
      sock_loop(Sock, Supervisor),
      {ok, Sock};
    {error, Reason} ->
      io:format("Client connect error: ~p~n", [Reason]),
      {error, nil}
  end.

sock_loop(Sock, Supervisor) ->
  receive
    {tcp, Sock, Data} ->
      io:format("Client received: ~s~n", [Data]),
      Supervisor ! {yummy, "yyy"},
      sock_loop(Sock, Supervisor);
    {tcp_closed, Sock} ->
      io:format("Client socket closed~n");
    {tcp_error, Sock, Reason} ->
      io:format("Client socket error: ~p~n", [Reason]);
    {sol, Data} ->
      % todo: handle send
      gen_tcp:send(Sock, Data),
      sock_loop(Sock, Supervisor);
    Other ->
      io:format("Client unexpected: ~p", [Other])
  end.

start_server(S) ->
  spawn(fun() ->
    server(start_miners(S))
  end).

server(
  S = #miner_state{
    miners = Miners
  }
) ->
  receive
    stop ->
      stop_miners(Miners),
      ok;
    {solution, Hash, Nonce, Timestamp} ->
      % todo, convey to parent process
      ok
  end.

%% @doc Start the workers and return the new state.
start_miners(
  S = #miner_state{
    data_segment = BDS,
    modified_diff = MDF,
    timestamp = TS,
    height = H
  }
) ->
  WorkerState = #{
    data_segment => BDS,
    diff => MDF,
    timestamp => TS,
    height => H
  },
  Miners = [spawn(?MODULE, mine, [WorkerState, self()]) || _ <- lists:seq(1, S#miner_state.max_miners)],
  S#miner_state {miners = Miners}.

%% @doc Stop all workers.
stop_miners(Miners) ->
  lists:foreach(
    fun(PID) ->
      exit(PID, stop)
    end,
    Miners
  ).

%% @doc Stop a running mining server.
stop(PID) ->
  PID ! stop.

%% @doc A worker process to hash the data segment searching for a solution
%% for the given diff.
mine(
  #{
    data_segment := BDS,
    diff := Diff,
    timestamp := Timestamp,
    height := Height
  },
  Supervisor
) ->
  process_flag(priority, low),
  {Nonce, Hash} = find_nonce(BDS, Diff, Height, Supervisor),
  Supervisor ! {solution, Hash, Nonce, Timestamp}.

find_nonce(BDS, Diff, Height, Supervisor) ->
  case randomx_hasher(Height) of
    {ok, Hasher} ->
      StartNonce =
        {crypto:strong_rand_bytes(256 div 8), crypto:strong_rand_bytes(256 div 8)},
      find_nonce(BDS, Diff, Height, StartNonce, Hasher, Supervisor);
    not_found ->
      ar:info("Mining is waiting on RandomX initialization"),
      timer:sleep(30 * 1000),
      find_nonce(BDS, Diff, Height, Supervisor)
  end.

%% Use RandomX fast-mode, where hashing is fast but initialization is slow.
randomx_hasher(Height) ->
  case ar_randomx_state:randomx_state_by_height(Height) of
    {state, {fast, FastState}} ->
      Hasher = fun(Nonce, BDS, Diff) ->
        ar_mine_randomx:bulk_hash_fast(FastState, Nonce, BDS, Diff)
               end,
      {ok, Hasher};
    {state, {light, _}} ->
      not_found;
    {key, _} ->
      not_found
  end.

find_nonce(BDS, Diff, Height, Nonce, Hasher, Supervisor) ->
  {BDSHash, HashNonce, ExtraNonce, HashesTried} = Hasher(Nonce, BDS, Diff),
  Supervisor ! {hashes_tried, HashesTried},
  case validate(BDSHash, Diff, Height) of
    false ->
      %% Re-use the hash as the next nonce, since we get it for free.
      find_nonce(BDS, Diff, Height, {BDSHash, ExtraNonce}, Hasher, Supervisor);
    true ->
      {HashNonce, BDSHash}
  end.

%% @doc Validate that a given hash/nonce satisfy the difficulty requirement.
validate(BDS, Nonce, Diff, Height) ->
  BDSHash = ar_weave:hash(BDS, Nonce, Height),
  case validate(BDSHash, Diff, Height) of
    true ->
      {valid, BDSHash};
    false ->
      {invalid, BDSHash}
  end.

%% @doc Validate that a given block data segment hash satisfies the difficulty requirement.
validate(BDSHash, Diff, Height) ->
  case ar_fork:height_1_8() of
    H when Height >= H ->
      binary:decode_unsigned(BDSHash, big) > Diff;
    _ ->
      case BDSHash of
        << 0:Diff, _/bitstring >> ->
          true;
        _ ->
          false
      end
  end.
