%%%
%%% @doc Different utility functions for node and node worker.
%%%

-module(ar_node_utils).

-export([get_conflicting_txs/2, get_full_block/3]).
-export([find_recall_hash/2, find_recall_block/1, find_block/1]).
-export([calculate_reward/2, calculate_reward_pool/4, calculate_proportion/3]).
-export([start_mining/1, reset_miner/1]).
-export([integrate_new_block/2]).
-export([fork_recover/3]).
-export([validate/5, validate/8]).

-include("ar.hrl").

%%%
%%% Public API.
%%%

%% @doc Get the conflicting transaction between state and passed ones.
get_conflicting_txs(StateTXs, TX) ->
	[
		TXOut
	||
		TXOut <-
			StateTXs,
			(
				(TXOut#tx.last_tx == TX#tx.last_tx) and
				(TXOut#tx.owner == TX#tx.owner)
			)
	].

%% @doc Get a specific full block (a block containing full txs) via
%% blocks indep_hash.
get_full_block(Peers, ID, BHL) when is_list(Peers) ->
	% check locally first, if not found ask list of external peers for block
	case ar_storage:read_block(ID, BHL) of
		unavailable ->
			get_full_block_from_remote_peers(ar_util:unique(Peers), ID, BHL);
		Block ->
			case make_full_block(Block) of
				unavailable ->
					ar_storage:invalidate_block(Block),
					get_full_block(Peers, ID, BHL);
				FinalB ->
					FinalB
			end
	end;
get_full_block(Pid, ID, BHL) when is_pid(Pid) ->
	% Attempt to get block from local storage and add transactions.
	make_full_block(ID, BHL);
get_full_block(Host, ID, BHL) ->
	% Handle external peer request.
	ar_http_iface_client:get_full_block(Host, ID, BHL).

%% @doc Attempt to get a full block from a HTTP peer, picking the node to query
%% randomly until the block is retreived.
get_full_block_from_remote_peers([], _ID, _BHL) ->
	unavailable;
get_full_block_from_remote_peers(Peers, ID, BHL) ->
	Peer = ar_util:pick_random(Peers),
	{Time, B} = timer:tc(fun() -> get_full_block(Peer, ID, BHL) end),
	case ?IS_BLOCK(B) of
		true ->
			case ar_meta_db:get(http_logging) of
				true ->
					ar:report(
						[
							{downloaded_block, ar_util:encode(ID)},
							{peer, Peer},
							{time, Time}
						]
					);
				_ -> do_nothing
			end,
			B;
		false ->
			get_full_block_from_remote_peers(Peers -- [Peer], ID, BHL)
	end.

%% @doc Return the hash of the next recall block.
find_recall_hash(Block, []) ->
	Block#block.indep_hash;
find_recall_hash(Block, HashList) ->
	lists:nth(1 + ar_weave:calculate_recall_block(Block, HashList), lists:reverse(HashList)).

%% @doc Search a block list for the next recall block.
find_recall_block(BHL = [Hash]) ->
	ar_storage:read_block(Hash, BHL);
find_recall_block(HashList) ->
	Block = ar_storage:read_block(hd(HashList), HashList),
	RecallHash = find_recall_hash(Block, HashList),
	ar_storage:read_block(RecallHash, HashList).

%% @doc Find a block from an ordered block list.
find_block(Hash) when is_binary(Hash) ->
	ar_storage:read_block(Hash).

%% @doc Calculate the reward.
calculate_reward_pool(OldPool, TXs, unclaimed, _Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	{0, Pool};
calculate_reward_pool(OldPool, TXs, _RewardAddr, Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	FinderReward = erlang:trunc(Pool * Proportion),
	{FinderReward, Pool - FinderReward}.

%% @doc Calculates the portion of the rewardpool that the miner is entitled
%% to for mining a block with a given recall. The proportion is based on the
%% size of the recall block and the average data stored within the weave.
calculate_proportion(RecallSize, WeaveSize, Height) when (Height == 0)->
	% Genesis block.
	calculate_proportion(
		RecallSize,
		WeaveSize,
		1
	);
calculate_proportion(RecallSize, WeaveSize, Height) when (WeaveSize == 0)->
	% No data stored in the weave.
	calculate_proportion(
		RecallSize,
		1,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize >= (WeaveSize/Height) ->
	% Recall size is larger than the average data stored per block.
	XRaw = ((Height * RecallSize) / WeaveSize) - 1,
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X) / (math:pow(2, X) + 2))
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize == 0 ->
	% Recall block has no data txs, hence size of zero.
	calculate_proportion(
		1,
		WeaveSize,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) ->
	% Standard recall block, 0 < Recall size < Average block.
	XRaw = -(((Height * WeaveSize) / RecallSize) -1),
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X)/(math:pow(2, X) + 2))
	).

%% @doc Force a node to start mining, update state.
start_mining(StateIn) ->
	start_mining(StateIn, unforced).

start_mining(#{hash_list := not_joined} = StateIn, _) ->
	% We don't have a block list. Wait until we have one before
	% starting to mine.
	StateIn;
start_mining(#{ 
		node := Node,
		hash_list := BHL,
		txs := TXs,
		reward_addr := RewardAddr, 
		tags := Tags } = StateIn, ForceDiff) ->
	case find_recall_block(BHL) of
		unavailable ->
			B = ar_storage:read_block(hd(BHL), BHL),
			RecallHash = find_recall_hash(B, BHL),
			% TODO: Cleanup.
			% FullBlock = get_encrypted_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash),
			FullBlock = get_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash, BHL),
			case FullBlock of
				X when (X == unavailable) or (X == not_found) ->
					ar:report(
						[
							could_not_start_mining,
							could_not_retrieve_recall_block
						]
					);
				_ ->
					case ar_weave:verify_indep(FullBlock, BHL) of
						true ->
							ar_storage:write_full_block(FullBlock),
							ar:report(
								[
									could_not_start_mining,
									stored_recall_block_for_foreign_verification
								]
							);
						false ->
								ar:report(
									[
										could_not_start_mining,
										{received_invalid_recall_block, FullBlock#block.indep_hash}
									]
								)
					end
			end,
			StateIn;
		RecallB ->
			if not is_record(RecallB, block) ->
				ar:report_console([{erroneous_recall_block, RecallB}]);
			true ->
				ar_miner_log:started_hashing(),
				ar:report([{node_starting_miner, Node}, {recall_block, RecallB#block.height}])
			end,
			RecallBFull = make_full_block(
				RecallB#block.indep_hash,
				BHL
			),
			ar_key_db:put(
				RecallB#block.indep_hash,
				[
					{
						ar_block:generate_block_key(RecallBFull, hd(BHL)),
						binary:part(hd(BHL), 0, 16)
					}
				]
			),
			B = ar_storage:read_block(hd(BHL), BHL),
			case ForceDiff of
				unforced ->
					Miner = ar_mine:start(
						B,
						RecallB,
						TXs,
						RewardAddr,
						Tags,
						Node
					),
					ar:report([{node, Node}, {started_miner, Miner}]),
					StateIn#{ miner => Miner };
				ForceDiff ->
					Miner = ar_mine:start(
						B,
						RecallB,
						TXs,
						RewardAddr,
						Tags,
						ForceDiff,
						Node
					),
					ar:report([{node, Node}, {started_miner, Miner}, {forced_diff, ForceDiff}]),
					StateIn#{ miner => Miner, diff => ForceDiff }
			end
	end.

%% @doc Kill the old miner, optionally start a new miner, depending on the automine setting.
reset_miner(#{ miner := undefined, automine := false } = StateIn) ->
	StateIn;
reset_miner(#{ miner := undefined, automine := true } = StateIn) ->
	start_mining(StateIn);
reset_miner(#{ miner := Pid, automine := false } = StateIn) ->
	ar_mine:stop(Pid),
	StateIn#{ miner => undefined };
reset_miner(#{ miner := Pid, automine := true } = StateIn) ->
	ar_mine:stop(Pid),
	start_mining(StateIn#{ miner => undefined }).

%% @doc We have received a new valid block. Update the node state accordingly.
integrate_new_block(
		#{
			txs := TXs,
			hash_list := HashList,
			wallet_list := WalletList,
			waiting_txs := WaitingTXs,
			potential_txs := PotentialTXs
		} = StateIn,
		NewB) ->
	% Filter completed TXs from the pending list.
	Diff = ar_mine:next_diff(NewB),
	RawKeepNotMinedTXs =
		lists:filter(
			fun(T) ->
				(not ar_weave:is_tx_on_block_list([NewB], T#tx.id)) and
				ar_tx:verify(T, Diff, WalletList)
			end,
			TXs
		),
	NotMinedTXs =
		lists:filter(
			fun(T) ->
				(not ar_weave:is_tx_on_block_list([NewB], T#tx.id))
			end,
			TXs ++ WaitingTXs ++ PotentialTXs
		),
	KeepNotMinedTXs = ar_wallet_list:filter_all_out_of_order_txs(
							NewB#block.wallet_list,
							RawKeepNotMinedTXs),
	BlockTXs = (TXs ++ WaitingTXs ++ PotentialTXs) -- NotMinedTXs,
	% Write new block and included TXs to local storage.
	ar_storage:write_tx(BlockTXs),
	ar_storage:write_block(NewB),
	% Recurse over the new block.
	ar_miner_log:foreign_block(NewB#block.indep_hash),
	ar:report_console(
		[
			{accepted_foreign_block, ar_util:encode(NewB#block.indep_hash)},
			{height, NewB#block.height}
		]
	),
	case whereis(fork_recovery_server) of
		undefined -> do_nothing;
		PID       -> PID ! {parent_accepted_block, NewB}
	end,
	% ar:d({new_hash_list, [NewB#block.indep_hash | HashList]}),
	app_search:update_tag_table(NewB),
	lists:foreach(
		fun(T) ->
			ar_tx_db:maybe_add(T#tx.id)
		end,
		PotentialTXs
	),
	RecallHash = find_recall_hash(NewB, BHL = [NewB#block.indep_hash | HashList]),
	RawRecallB = ar_storage:read_block(RecallHash, BHL),
	case ?IS_BLOCK(RawRecallB) of
		true ->
			RecallB =
				RawRecallB#block {
					txs = lists:map(fun ar_storage:read_tx/1, RawRecallB#block.txs)
				},
			ar_key_db:put(
				RecallB#block.indep_hash,
				[
					{
						ar_block:generate_block_key(
							RecallB,
							NewB#block.previous_block),
						binary:part(NewB#block.indep_hash, 0, 16)
					}
				]
			);
		false ->
			ok
	end,
	reset_miner(StateIn#{
		hash_list			 => [NewB#block.indep_hash | HashList],
		txs					 => ar_track_tx_db:remove_bad_txs(KeepNotMinedTXs),
		height				 => NewB#block.height,
		floating_wallet_list => ar_wallet_list:apply_txs(WalletList, TXs),
		reward_pool			 => NewB#block.reward_pool,
		potential_txs		 => [],
		diff				 => NewB#block.diff,
		last_retarget		 => NewB#block.last_retarget,
		weave_size			 => NewB#block.weave_size
	}).

%% @doc Recovery from a fork.
fork_recover(#{ node := Node, hash_list := HashList } = StateIn, Peer, NewB) ->
	case {whereis(fork_recovery_server), whereis(join_server)} of
		{undefined, undefined} ->
			PrioritisedPeers = ar_util:unique(Peer),
			erlang:monitor(
				process,
				PID = ar_fork_recovery:start(
					PrioritisedPeers,
					NewB,
					HashList,
					Node
				)
			),
			case PID of
				undefined -> ok;
				_         -> erlang:register(fork_recovery_server, PID)
			end;
		{undefined, _} ->
			ok;
		_ ->
			whereis(fork_recovery_server) ! {update_target_block, NewB, ar_util:unique(Peer)}
	end,
	% TODO: Check how an unchanged state has to be returned in
	% program flow.
	StateIn.

%% @doc Validate a block, given a node state and the dependencies.
validate(#{ hash_list := HashList, wallet_list := WalletList }, B, TXs, OldB, RecallB) ->
	validate(HashList, WalletList, B, TXs, OldB, RecallB, B#block.reward_addr, B#block.tags).

%% @doc Validate a new block, given a server state, a claimed new block, the last block,
%% and the recall block.
validate(_, _, _, _, _, _RecallB = unavailable, _, _) ->
	false;
validate(
		HashList,
		WalletList,
		NewB =
			#block {
				hash_list = HashList,
				wallet_list = WalletList,
				nonce = Nonce,
				diff = Diff,
				timestamp = Timestamp
			},
		TXs,
		OldB,
		RecallB,
		RewardAddr,
		Tags) ->
	% TODO: Fix names.
	{_, BDSHash} = timer:tc(ar_weave,hash,[
		ar_block:generate_block_data_segment(OldB, RecallB, TXs, RewardAddr, Timestamp, Tags),
		Nonce]),
	{T1,Mine} = timer:tc(ar_mine,validate_by_hash,[BDSHash, Diff]),
	{T2,IndepRecall} = timer:tc(ar_weave,verify_indep,[RecallB, HashList]),
	{T3,Txs} = timer:tc(ar_tx,verify_txs,[TXs, Diff, OldB#block.wallet_list]),
	{T4,Retarget} = timer:tc(ar_retarget,validate,[NewB, OldB]),
	{T5,IndepHash} = timer:tc(ar_block,verify_indep_hash,[NewB]),
	{T6,Hash} = timer:tc(ar_block,verify_dep_hash,[NewB, BDSHash]),
	{T7,WeaveSize} = timer:tc(ar_block,verify_weave_size,[NewB, OldB, TXs]),
	{T8,Size} = timer:tc(ar_block,block_field_size_limit,[NewB]),
	{T9,TimeCheck} = timer:tc(ar_block,verify_timestamp_diff,[NewB, OldB]),
	{Ta,HeightCheck} = timer:tc(ar_block,verify_height,[NewB, OldB]),
	{Tb,RetargetCheck} = timer:tc(ar_block,verify_last_retarget,[NewB]),
	{Tc,PreviousBCheck} = timer:tc(ar_block,verify_previous_block,[NewB, OldB]),
	{Td,HashlistCheck} = timer:tc(ar_block,verify_block_hash_list,[NewB, OldB]),
	{Te,WalletListCheck} = timer:tc(ar_block,verify_wallet_list,[NewB, OldB, RecallB, TXs]),
	{Tf,CumulativeDiffCheck} = timer:tc(ar_block,verify_cumulative_diff,[NewB, OldB]),
	{Tg,BHLMerkleCheck} = timer:tc(ar_block,verify_block_hash_list_merkle,[NewB, OldB]),

	ar:report(
		[
			{block_validation_results, ar_util:encode(NewB#block.indep_hash)},
			{height, NewB#block.height},
			{block_mine_validate, Mine, T1},
			{block_indep_validate, IndepRecall, T2},
			{block_txs_validate, Txs, T3},
			{block_diff_validate, Retarget, T4},
			{block_indep, IndepHash, T5},
			{block_hash, Hash, T6},
			{weave_size, WeaveSize, T7},
			{block_size, Size, T8},
			{block_timestamp_diff, TimeCheck, T9},
			{block_height, HeightCheck, Ta},
			{block_retarget_time, RetargetCheck, Tb},
			{block_previous_check, PreviousBCheck, Tc},
			{block_hash_list, HashlistCheck, Td},
			{block_wallet_list, WalletListCheck, Te},
			{block_cumulative_diff, CumulativeDiffCheck, Tf},
			{hash_list_merkle, BHLMerkleCheck, Tg}
		]
	),

	case IndepRecall of
		false ->
			ar:report(
				[
					{encountered_invalid_recall_block, ar_util:encode(RecallB#block.indep_hash)},
					moving_to_invalid_block_directory
				]
			),
			ar_storage:invalidate_block(RecallB);
		_ ->
			ok
	end,

	(Mine =/= false)
		andalso IndepRecall
		andalso Txs
		andalso Retarget
		andalso IndepHash
		andalso Hash
		andalso WeaveSize
		andalso Size
		andalso TimeCheck
		andalso HeightCheck
		andalso RetargetCheck
		andalso PreviousBCheck
		andalso HashlistCheck
		andalso WalletListCheck
		andalso CumulativeDiffCheck
		andalso BHLMerkleCheck;
validate(_HL, WL, NewB = #block { hash_list = unset }, TXs, OldB, RecallB, _, _) ->
	validate(unset, WL, NewB, TXs, OldB, RecallB, unclaimed, []);
validate(HL, _WL, NewB = #block { wallet_list = undefined }, TXs,OldB, RecallB, _, _) ->
	validate(HL, undefined, NewB, TXs, OldB, RecallB, unclaimed, []);
validate(_HL, _WL, NewB, _TXs, _OldB, _RecallB, _, _) ->
	ar:report([{block_not_accepted, ar_util:encode(NewB#block.indep_hash)}]),
	false.

%%%
%%% Private functions.
%%%

%% @doc Convert a block with tx references into a full block, that is a block
%% containing the entirety of all its referenced txs.
make_full_block(ID, BHL) ->
	make_full_block(ar_storage:read_block(ID, BHL)).
make_full_block(unavailable) -> unavailable;
make_full_block(BlockHeader) ->
	FullB =
		BlockHeader#block{
			txs =
				get_tx(
					whereis(http_entrypoint_node),
					BlockHeader#block.txs
				)
		},
	case [ NotTX || NotTX <- FullB#block.txs, is_atom(NotTX) ] of
		[] -> FullB;
		_  -> unavailable
	end.

%% @doc Return a specific tx from a node, if it has it.
%% TODO: Should catch case return empty list or not_found atom.
get_tx(_, []) ->
	[];
get_tx(Peers, ID) when is_list(Peers) ->
	% check locally first, if not found in storage nor nodes state ask
	% list of external peers for tx
	ar:report([{getting_tx, ID}, {peers, Peers}]),
	case
		{
			ar_storage:read_tx(ID),
			lists:keyfind(ID, 2, ar_node:get_full_pending_txs(whereis(http_entrypoint_node)))
		} of
		{unavailable, false} ->
			lists:foldl(
				fun(Peer, Acc) ->
					case is_atom(Acc) of
						false -> Acc;
						true ->
							T = get_tx(Peer, ID),
							case is_atom(T) of
								true -> Acc;
								false -> T
							end
					end
				end,
				unavailable,
				Peers
			);
		{TX, false} -> TX;
		{unavailable, TX} -> TX;
		{TX, _} -> TX
	end;
get_tx(Proc, ID) when is_pid(Proc) ->
	% attempt to get tx from local storage
	%ar:d({pending, get_full_pending_txs(whereis(http_entrypoint_node))}),
	ar_storage:read_tx(ID);
get_tx(Host, ID) ->
	% handle external peer request
	ar_http_iface_client:get_tx(Host, ID).

%% @doc Calculate the total mining reward for the a block and its associated TXs.
calculate_reward(Height, Quantity) ->
	erlang:trunc(ar_inflation:calculate(Height) + Quantity).

%% @doc Given a TX, calculate an appropriate reward.
calculate_tx_reward(#tx { reward = Reward }) ->
	% TDOD mue: Calculation is not calculated, only returned.
	Reward.

%%%
%%% Unreferenced!
%%%

% @doc Return the last block to include both a wallet and hash list.
% NOTE: For now, all blocks are sync blocks.
%find_sync_block([]) ->
%	not_found;
%find_sync_block([Hash | Rest]) when is_binary(Hash) ->
%	find_sync_block([ar_storage:read_block(Hash) | Rest]);
%find_sync_block([B = #block { hash_list = HashList, wallet_list = WalletList } | _])
%		when HashList =/= undefined, WalletList =/= undefined ->
%	B;
%find_sync_block([_ | Xs]) ->
%	find_sync_block(Xs).

%% @doc Given a wallet list and set of txs will try to apply the txs
%% iteratively to the wallet list and return the result.
%% Txs that cannot be applied are disregarded.
%generate_floating_wallet_list(WalletList, []) ->
%	WalletList;
%generate_floating_wallet_list(WalletList, [T | TXs]) ->
%	case ar_tx:check_last_tx(WalletList, T) of
%		true ->
%			UpdatedWalletList = ar_wallet_list:apply_tx(WalletList, T),
%			generate_floating_wallet_list(UpdatedWalletList, TXs);
%		false -> false
%	end.
