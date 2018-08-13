%%%
%%% @doc Server to queue ar_node state-changing tasks.
%%%

-module(ar_node_worker).

-export([start/1, stop/1, cast/2, call/2, call/3]).

-include("ar.hrl").

%%%
%%% Public API.
%%%

%% @doc Start a node worker.
start(SPid) ->
	Pid = spawn(fun() -> server(SPid) end),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Send an asynchronous task to a node worker. The answer
%% will be sent to the caller.
cast(Pid, Task) ->
	Pid ! {Task, self()},
	ok.

%% @doc Send a synchronous task to a node worker. The timeout
%% can be passed, default is 5000 ms.
call(Pid, Task) ->
	call(Pid, Task, 5000).

call(Pid, Task, Timeout) ->
	cast(Pid, Task),
	receive
		Reply ->
			Reply
	after
		Timeout ->
			{error, timeout}
	end.

%%%
%%% Server functions.
%%%

%% @doc Main server loop.
server(SPid) ->
	receive
		{Task, Sender} ->
			try handle(SPid, Task, Sender) of
				Reply ->
					Sender ! Reply,
					server(SPid)
			catch
				throw:Term ->
					ar:report( [ {'NodeWorkerEXCEPTION', {Term} } ]),
					server(SPid);
				exit:Term ->
					ar:report( [ {'NodeWorkerEXIT', Term} ] ),
					server(SPid);
				error:Term ->
					ar:report( [ {'NodeWorkerERROR', {Term, erlang:get_stacktrace()} } ]),
					server(SPid)
			end;
		stop ->
			ok
	end.

%% @doc Handle the server tasks. Return values a sent to the caller. Simple tasks
%% can be done directy, more complex ones with dependencies from arguments are
%% handled as private API functions. SPid allows to access the state server,
%% inserts have to be atomic.
handle(SPid, {add_tx, TX}, Sender) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case add_tx(StateIn, TX, Sender) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {add_tx, TX, NewGS}, Sender) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case add_tx(StateIn, TX, NewGS, Sender) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {process_new_block, NewGS, NewB, RecallB, Peer, HashList}, _Sender) ->
	% TODO mue: Reduce to only needed values later, but seem to be pretty much here.
	{ok, StateIn} = ar_node_state:all(SPid),
	case process_new_block(StateIn, NewGS, NewB, RecallB, Peer, HashList) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, process_new_block};
handle(SPid, {replace_block_list, NewBL}, _Sender) ->
	case replace_block_list(NewBL) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, replace_block_list};
handle(SPid, {work_complete, MinedTXs, _Hash, Diff, Nonce, Timestamp}, _Sender) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, work_complete};
handle(_SPid, Msg, _Sender) ->
	{error, {unknown_node_worker_message, Msg}}.

%%%
%%% Private API functions.
%%%

%% @doc Add new transaction to a server state.
add_tx(StateIn, TX, Sender) ->
	#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs} = StateIn,
	case ar_node_utils:get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			{ok , [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])}
			]};
		_ ->
			% TODO mue: Space in string atom correct?
			ar_tx_db:put(TX#tx.id, ["last_tx_not_valid "]),
			{ok, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])}
			]}
	end.

add_tx(StateIn, TX, NewGS, Sender) ->
	#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs} = StateIn,
	case ar_node_utils:get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			{ok, [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])},
				{gossip, NewGS}
			]};
		_ ->
			{ok, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])},
				{gossip, NewGS}
			]}
	end.

%% @doc Validate whether a new block is legitimate, then handle it, optionally
%% dropping or starting a fork recoverer as appropriate.
process_new_block(_StateIn, NewGS, NewB, _, _Peer, not_joined) ->
	ar_join:start(ar_gossip:peers(NewGS, NewB)),
	none;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, unavailable, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	% This block is at the correct height.
	RecallHash = ar_node_utils:find_recall_hash(NewB, HashList),
	FullBlock = ar_node_utils:get_full_block(Peer, RecallHash),
	case ?IS_BLOCK(FullBlock) of
		true ->
			% TODO: Cleanup full block -> shadow generation.
			RecallShadow = FullBlock#block { txs = [
													T#tx.id
													||
													T <- FullBlock#block.txs] },
			ar_storage:write_full_block(FullBlock),
			StateNext = StateIn#{ gossip => NewGS },
			process_new_block(StateNext, NewGS, NewB, RecallShadow, Peer, HashList);
		false ->
			ar:d(failed_to_get_recall_block),
			none
	end;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, RecallB, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	% This block is at the correct height.
	StateNext = StateIn#{ gossip => NewGS },
	#{
		txs := TXs,
		waiting_txs := WaitingTXs,
		potential_txs := PotentialTXs,
		rewart_pool := RewardPool,
		wallet_list := WalletList
	} = StateNext,
	% If transaction not found in state or storage, txlist built will be
	% incomplete and will fail in validate
	TXs = lists:foldr(
		fun(T, Acc) ->
			%state contains it
			case [ TX || TX <- (TXs ++ WaitingTXs ++ PotentialTXs), TX#tx.id == T ] of
				[] ->
					case ar_storage:read_tx(T) of
						unavailable -> Acc;
						TX			-> [TX | Acc]
					end;
				[TX | _] ->
					[TX | Acc]
			end
		end,
		[],
		NewB#block.txs
	),
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				NewB#block.height
			)
		),
	NewWalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(WalletList, TXs),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		),
	StateNew = StateNext#{ wallet_list => NewWalletList },
	% TODO mue: ar_node:validate() has to be moved to ar_node_worker. Also
	% check what values of state are needed. Also setting the state gossip
	% for fork_recover/3 has to be checked. The gossip is already set to
	% NewGS in first function statement. Compare to pre-refactoring.
	case ar_node:validate(
			StateNew,
			NewB,
			TXs,
			ar_util:get_head_block(HashList), RecallB
	) of
		true ->
			% The block is legit. Accept it.
			case whereis(fork_recovery_server) of
				undefined -> ar_node_utils:integrate_new_block(StateNew, NewB);
				_		  -> ar_node_utils:fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
			end;
		false ->
			ar:d({could_not_validate_new_block, ar_util:encode(NewB#block.indep_hash)}),
			ar_node_utils:fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
	end;
process_new_block(# {height := Height }, NewGS, NewB, _RecallB, _Peer, _HashList)
		when NewB#block.height =< Height ->
	% Block is lower than us, ignore it.
	ar:report(
		[
			{ignoring_block_below_current, ar_util:encode(NewB#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, NewB#block.height}
		]
	),
	{ok, [{gossip, NewGS}]};
% process_new_block(S, NewGS, NewB, _RecallB, _Peer, _Hashlist)
%		when (NewB#block.height == S#state.height + 1) ->
	% Block is lower than fork recovery height, ignore it.
	% server(S#state { gossip = NewGS });
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, _RecallB, Peer, _HashList)
		when (NewB#block.height > Height + 1) ->
	ar_node_utils:fork_recover(StateIn#{ gossip => NewGS }, Peer, NewB).

%% @doc Replace the entire stored block list, regenerating the hash list.
replace_block_list([Block | _]) ->
	{ok, [
		{hash_list, [Block#block.indep_hash | Block#block.hash_list]},
		{wallet_list, Block#block.wallet_list},
		{height, Block#block.height}
	]}.

%% @doc Verify a new block found by a miner, integrate it.
integrate_block_from_miner(#{ hash_list := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) ->
	#{
		hash_list     := HashList,
		wallet_list   := RawWalletList,
		txs           := TXs,
		gossip        := GS,
		reward_addr   := RewardAddr,
		tags          := Tags,
		reward_pool   := OldPool,
		weave_size    := OldWeaveSize,
		potential_txs := PotentialTXs
	} = StateIn,
	% Calculate the new wallet list (applying TXs and mining rewards).
	RecallB = ar_node_utils:find_recall_block(HashList),
	WeaveSize = OldWeaveSize +
		lists:foldl(
			fun(TX, Acc) ->
				Acc + byte_size(TX#tx.data)
			end,
			0,
			TXs
		),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			OldPool,
			MinedTXs,
			RewardAddr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				WeaveSize,
				length(HashList)
			)
		),
	ar:report(
		[
			calculated_reward_for_mined_block,
			{finder_reward, FinderReward},
			{new_reward_pool, RewardPool},
			{reward_address, RewardAddr},
			{old_reward_pool, OldPool},
			{txs, length(MinedTXs)},
			{recall_block_size, RecallB#block.block_size},
			{weave_size, WeaveSize},
			{length, length(HashList)}
		]
	),
	WalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(RawWalletList, MinedTXs),
			RewardAddr,
			FinderReward,
			length(HashList)
		),
	% Store the transactions that we know about, but were not mined in
	% this block.
	NotMinedTXs =
		lists:filter(
			fun(T) -> ar_tx:verify(T, Diff, WalletList) end,
			ar_node_utils:filter_all_out_of_order_txs(WalletList, TXs -- MinedTXs)
		),
	StateNew = StateIn#{ wallet_list => WalletList },
	% Build the block record, verify it, and gossip it to the other nodes.
	[NextB | _] = ar_weave:add(
		HashList, MinedTXs, HashList, RewardAddr, RewardPool,
		WalletList, Tags, RecallB, Diff, Nonce, Timestamp),
	case validate(StateNew, NextB, MinedTXs, ar_util:get_head_block(HashList), RecallB = find_recall_block(HashList)) of
		false ->
			ar:report_console(miner_produced_invalid_block),
			case rand:uniform(5) of
				1 ->
					#{ gossip := StateInGS } = StateIn,
					ar_node_utils:reset_miner(StateIn#{
						gossip		  => StateInGS,
						txs			  => [], % TXs not included in the block
						potential_txs => []
					});
				_ ->
					ar_node_utils:reset_miner(StateIn)
			end;
		true ->
			ar_storage:write_tx(MinedTXs),
			ar_storage:write_block(NextB),
			app_search:update_tag_table(NextB),
			{NewGS, _} =
				ar_gossip:send(
					GS,
					{
						new_block,
						self(),
						NextB#block.height,
						NextB,
						RecallB
					}
				),
			ar:report_console(
				[
					{node, self()},
					{accepted_block, NextB#block.height},
					{indep_hash, ar_util:encode(NextB#block.indep_hash)},
					{recall_block, RecallB#block.height},
					{recall_hash, RecallB#block.indep_hash},
					{txs, length(MinedTXs)},
					case is_atom(RewardAddr) of
						true -> {reward_address, unclaimed};
						false -> {reward_address, ar_util:encode(RewardAddr)}
					end
				]
			),
			lists:foreach(
				fun(MinedTX) ->
					ar:report(
						{successfully_mined_tx_into_block, ar_util:encode(MinedTX#tx.id)}
					)
				end,
				MinedTXs
			),
			lists:foreach(
				fun(T) ->
					ar_tx_db:maybe_add(T#tx.id)
				end,
				PotentialTXs
			),
			ar_node_utils:reset_miner(
				StateNew#{
					gossip => NewGS,
					hash_list => [NextB#block.indep_hash | HashList],
					txs => ar_track_tx_db:remove_bad_txs(NotMinedTXs), % TXs not included in the block
					height => NextB#block.height,
					floating_wallet_list => ar_node_utils:apply_txs(WalletList, NotMinedTXs),
					reward_pool => RewardPool,
					potential_txs => [],
					diff => NextB#block.diff,
					last_retarget => NextB#block.last_retarget,
					weave_size => NextB#block.weave_size
				}
			)
	end.

%%%
%%% EOF
%%%
