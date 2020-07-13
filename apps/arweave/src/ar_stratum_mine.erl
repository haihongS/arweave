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

-export([run/0, get_sock/1]).
-export([mine/2, stop/1, validate/3, validate/4]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TCP_OPTIONS, [binary, {packet, raw}, {active, true}]).

-record(miner_state, {
  data_segment,
  modified_diff,
  height,
  timestamp,
  max_miners = ?NUM_MINING_PROCESSES, % max mining process to start
  miners = [] % miner worker processes
}).

miner_test() ->
  io:format("qqqqqq~n"),

  ar_randomx_state:init(
    whereis(ar_randomx_state),
    ar_randomx_state:swap_height(485406),
    crypto:strong_rand_bytes(32),
    erlang:system_info(schedulers_online)
  ),

  start_miner_server(
    #miner_state{
      data_segment = <<127,68,21,83,156,14,14,112,69,189,228,133,76,20,179,145,128,92,224,27,
        16,234,80,129,30,86,247,60,211,65,107,141,165,206,219,180,222,200,41,33,
        38,228,19,202,233,251,131,88>>,
      modified_diff =   115792089224058742650682137424559985932804201871617767750832240943213492305920,
      timestamp = 1594621188,
      height = 485406
    },
    self()
  ),

%%  WorkerState = #{
%%    data_segment => <<127,68,21,83,156,14,14,112,69,189,228,133,76,20,179,145,128,92,224,27,
%%      16,234,80,129,30,86,247,60,211,65,107,141,165,206,219,180,222,200,41,33,
%%      38,228,19,202,233,251,131,88>>,
%%    diff =>   115792089224058742650682137424559985932804201871617767750832240943213492305920,
%%    timestamp => 1594621188,
%%    height =>485406
%%  },
%%
%%  io:format("height: ~p~n", [ar_randomx_state:swap_height(482780)]),
%%  ar_randomx_state:init(
%%    whereis(ar_randomx_state),
%%    ar_randomx_state:swap_height(485406),
%%    crypto:strong_rand_bytes(32),
%%    erlang:system_info(schedulers_online)
%%  ),

%%  mine(WorkerState, self()),

  log_listener(),
  ok.

log_listener() ->
  receive
    {log, Msg} ->
      io:format("log: ~p~n", [Msg]),
      log_listener();
    {solution, Hash, Nonce, Timestamp} ->
      io:format("solution: ~p ~p ~p~n", [Hash, Nonce, Timestamp]),
      log_listener();
    {hashes_tried, T} ->
      io:format("hashes tried ~p~n", [T]),
      log_listener()
  end.

run() ->
  miner_test(),
  ok.

run(M) ->
  Sock = spawn(?MODULE, get_sock, [self()]),
  io:format("Sock~p~n", [Sock]),
  mainloop().

mainloop() ->
  receive
    {got_sock, Sock} ->
      io:format("haha~n"),
      gen_tcp:send(Sock, "xxxxxxxx"),
      %% start miner
      x(Sock)
  end.

x(Sock) ->
  receive
    {pool_job, Job} ->
      io:format("pool job~p~n", [Job]),
      timer:sleep(1000),
      gen_tcp:send(Sock, Job),
      x(Sock);
    {miner_sol, Sol} ->
      x(Sock)
  end.

get_sock(Parent) ->
  TcpHost = ar_meta_db:get(pool_tcp_host),
  TcpPort = ar_meta_db:get(pool_tcp_port),
  io:format("h: ~p~p~n", [TcpHost, TcpPort]),
  case gen_tcp:connect(TcpHost, list_to_integer(TcpPort), ?TCP_OPTIONS, 5000) of
    {ok, Sock} ->
      Parent ! {got_sock, Sock},
      sock_loop(Sock, Parent);
    {error, Reason} ->
      io:format("Client connect error: ~p~n", [Reason]),
      {error, nil}
  end.

sock_loop(Sock, Parent) ->
  receive
    {tcp, Sock, Data} ->
      io:format("Client received: ~s~n", [Data]),
      Parent ! {pool_job, Data},
      sock_loop(Sock, Parent);
    {tcp_closed, Sock} ->
      io:format("Client socket closed~n");
    {tcp_error, Sock, Reason} ->
      io:format("Client socket error: ~p~n", [Reason]);
    {sol, Data} ->
      % todo: handle send
      io:format("coool~n"),
      gen_tcp:send(Sock, Data),
      sock_loop(Sock, Parent);
    Other ->
      io:format("Client unexpected: ~p", [Other])
  end.

start_miner_server(S, LogPID) ->
  io:format("sss: ~p~n", [S]),
  spawn(fun() ->
    server(start_miners(S, LogPID))
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
      io:format("Soluuuuuution"),
      ok
  end.

%% @doc Start the workers and return the new state.
start_miners(
    S = #miner_state{
      data_segment = BDS,
      modified_diff = MDF,
      timestamp = TS,
      height = H
    },
    LogPID
) ->
  WorkerState = #{
    data_segment => BDS,
    diff => MDF,
    timestamp => TS,
    height => H
  },
  LogPID ! {log, WorkerState},
  Miners = [spawn(?MODULE, mine, [WorkerState, LogPID]) || _ <- lists:seq(1, S#miner_state.max_miners)],
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
  Supervisor ! {log, "XIXI~n"},
  {Nonce, Hash} = find_nonce(BDS, Diff, Height, Supervisor),
  Supervisor ! {log, "HAHA~n"},
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
      io:format("faststate: ~p~n", [FastState]),
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
  io:format("p1~n"),
  {BDSHash, HashNonce, ExtraNonce, HashesTried} = Hasher(Nonce, BDS, Diff),
  io:format("p2~n"),
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
