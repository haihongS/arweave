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
-export([tcp_mgr/2, tcp_dial/1]).
-export([mining_mgr/2]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TCP_OPTIONS, [binary, {packet, raw}, {active, true}]).

-record(miner_state, {
  job_id,
  data_segment,
  modified_diff,
  height,
  timestamp,
  max_miners = ?NUM_MINING_PROCESSES, % max mining process to start
  miners = [], % miner worker processes
  randomx_state_init = false
}).

main_mgr() ->
  TcpMgrPid = spawn(?MODULE, tcp_mgr, [self(), nil]),
  MiningMgrPid = spawn(?MODULE, mining_mgr, [nil, self()]),

  main_listener(TcpMgrPid, MiningMgrPid),
  ok.

main_listener(TcpMgrPid, MiningMgrPid) ->
  receive
    {tcp_mgr_newdata, Data} ->
      State = job_json_to_state(Data),
      io:format("State: ~p~n", [State]),
      MiningMgrPid ! {new_job, State},
      main_listener(TcpMgrPid, MiningMgrPid);

    {mining_mgr_solution, JobId, Hash, Nonce, Timestamp} ->
      io:format("Solution: ~p | ~p~n", [JobId, Hash]),
      SubmitJson = solution_to_submit_json(JobId, Hash, Nonce),
      io:format("SubmitJson: ~p~n", [SubmitJson]),
      TcpMgrPid ! {to_sock, SubmitJson},
      main_listener(TcpMgrPid, MiningMgrPid)
  end.

solution_to_submit_json(JobId, Hash, Nonce) ->
  Json =
    {[
        {id, 2},
        {jsonrpc, <<"2.0">>},
        {method, <<"submit">>},
        {params, {[
          {job_id, JobId},
          {hash, list_to_binary(av_utils:binary_to_hex(Hash))},
          {nonce, list_to_binary(av_utils:binary_to_hex(Nonce))}
        ]}}
    ]},
  ar_serialize:jsonify(Json).

job_json_to_state(JobJson) ->
  Job = jiffy:decode(JobJson, [return_maps]),
  Params = maps:get(<<"params">>, Job),

  JobId = maps:get(<<"job_id">>, Params),
  BDS = av_utils:hex_to_binary(binary_to_list(maps:get(<<"bds">>, Params))),
  Diff = list_to_integer(binary_to_list(maps:get(<<"diff">>, Params)), 10),
  Timestamp = maps:get(<<"timestamp">>, Params),
  Height = maps:get(<<"height">>, Params),

  Ans = #miner_state{
    job_id = JobId,
    data_segment = BDS,
    modified_diff = Diff,
    timestamp = Timestamp,
    height = Height
  },
  Ans.

mining_mgr(State, MainPid) ->
  receive
    {start_miners} ->
      io:format("ss~n"),
      UpdatedState = restart_miners_with_state(State, self()),
      mining_mgr(UpdatedState, MainPid);

    {new_job, NewState} ->
      io:format("hh~n"),
      RandomXState =
        case State of
          nil -> false;
          _Else -> true
        end,

      UpdatedState = restart_miners_with_state(
        NewState#miner_state{randomx_state_init = RandomXState},
        self()
      ),

      io:format("UpdatedState: ~p~n", [UpdatedState]),
      mining_mgr(UpdatedState, MainPid);
    {new_randomx_state, Height} ->
      % todo: update new randomx state
      mining_mgr(State, MainPid);
    {solution, JobId, Hash, Nonce, Timestamp} ->
      MainPid ! {mining_mgr_solution, JobId, Hash, Nonce, Timestamp},
      mining_mgr(State, MainPid)
  end.

restart_miners_with_state(S, MiningMgrPid) ->
  stop_miners(S#miner_state.miners),
  NewS = mining_start_miners(S, MiningMgrPid),
  NewS.

mining_start_miners(
    S = #miner_state{
      job_id = JobId,
      data_segment = BDS,
      modified_diff = MDF,
      timestamp = TS,
      height = H
    },
    MiningMgrPid
) ->
  case S#miner_state.randomx_state_init of
    false -> mining_init_randomx_state(H);
    true -> {}
  end,

  WorkerState = #{
    job_id => JobId,
    data_segment => BDS,
    diff => MDF,
    timestamp => TS,
    height => H
  },
  Miners = [spawn(?MODULE, mine, [WorkerState, MiningMgrPid]) || _ <- lists:seq(1, S#miner_state.max_miners)],
  S#miner_state {miners = Miners, randomx_state_init = true}.

mining_init_randomx_state(Height) ->
  ar_randomx_state:init(
    whereis(ar_randomx_state),
    ar_randomx_state:swap_height(Height),
    crypto:strong_rand_bytes(32), % todo: add key bytes here
    erlang:system_info(schedulers_online)
  ).

tcp_mgr(MainPid, Sock) ->
  case Sock of
    nil ->
      spawn(?MODULE, tcp_dial, [self()]);
    _Else -> {}
  end,

  receive
    {got_sock, NewSock} ->
      tcp_mgr(MainPid, NewSock);
    {error_sock, ErrorReason} ->
      % todo: log error
      timer:sleep(3000),
      tcp_mgr(MainPid, nil);
    {reset_sock} ->
      tcp_mgr(MainPid, nil);
    {from_sock, Data} ->
      MainPid ! {tcp_mgr_newdata, Data},
      tcp_mgr(MainPid, Sock);
    {to_sock, Data} ->
      if
        Sock == nil -> ok;
        true -> gen_tcp:send(Sock, Data)
      end,
      tcp_mgr(MainPid, Sock)
  end.

tcp_dial(TcpMgrPid) ->
  TcpHost = ar_meta_db:get(pool_tcp_host),
  TcpPort = ar_meta_db:get(pool_tcp_port),

  case gen_tcp:connect(TcpHost, list_to_integer(TcpPort), ?TCP_OPTIONS, 5000) of
    {ok, Sock} ->
      TcpMgrPid ! {got_sock, Sock},
      tcp_sock_loop(Sock, TcpMgrPid);
    {error, Reason} ->
      io:format("Client connect error: ~p~n", [Reason]),
      TcpMgrPid ! {error_sock, Reason},
      {error, nil}
  end.

tcp_sock_loop(Sock, TcpMgrPid) ->
  receive
    {tcp, Sock, Data} ->
      io:format("Client received: ~s~n", [Data]),
      TcpMgrPid ! {from_sock, Data},
      tcp_sock_loop(Sock, TcpMgrPid);
    {tcp_closed, Sock} ->
      io:format("Client socket closed~n"),
      TcpMgrPid ! {reset_sock};
    {tcp_error, Sock, Reason} ->
      io:format("Client socket error: ~p~n", [Reason]),
      TcpMgrPid ! {error_sock, Reason};
    Other ->
      io:format("Client unexpected: ~p", [Other]),
      TcpMgrPid ! {reset_sock}
  end.

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
  main_mgr().
%%  Sock = spawn(?MODULE, get_sock, [self()]),
%%  io:format("Sock~p~n", [Sock]),
%%  mainloop().

mainloop() ->
  receive
    {got_sock, Sock} ->
      io:format("haha~n"),
      Tmp = {[{<<"method">>, <<"bar">>}, {<<"params">>, {[{<<"bds">>, <<"7F4415539C0E0E7045BDE4854C14B391805CE01B10EA50811E56F73CD3416B8DA5CEDBB4DEC8292126E413CAE9FB8358">>}]}}]},
      TData = jiffy:encode(Tmp),
%%      gen_tcp:send(Sock, TData),
      %% start miner
      x(Sock)
  end.

x(Sock) ->
  receive
    {pool_job, Job} ->
      io:format("pool job: ~p~n", [Job]),
      Resp = jiffy:decode(Job, [return_maps]),

      Params = maps:get(<<"params">>, Resp),
      io:format("Params: ~p~n", [Params]),

      Bds = av_utils:hex_to_binary(binary_to_list(maps:get(<<"bds">>, Params))),
      io:format("Bds: ~p~n", [Bds]),

      Diff = list_to_integer(binary_to_list(maps:get(<<"diff">>, Params)), 10),
      io:format("diff: ~p~n", [Diff]),

      Timestamp = maps:get(<<"timestamp">>, Params),
      io:format("ts: ~p~n", [Timestamp]),

      Height = maps:get(<<"height">>, Params),
      io:format("height: ~p~n", [Height]),

      io:format("resp: | ~p~n", [Resp]),
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
  S = #{
    job_id := JobId,
    data_segment := BDS,
    diff := Diff,
    timestamp := Timestamp,
    height := Height
  },
  Supervisor
) ->
  process_flag(priority, low),
  io:format("XIXI~n"),
  {Nonce, Hash} = find_nonce(BDS, Diff, Height, Supervisor),
  Supervisor ! {solution, JobId, Hash, Nonce, Timestamp},
  mine(S, Supervisor).

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
