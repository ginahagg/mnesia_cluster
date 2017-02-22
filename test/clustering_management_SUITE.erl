-module(clustering_management_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(LOOP_RECURSION_DELAY, 100).

-record(test_tab_item, {id, name, description}).

all() ->
    [
     {group, cluster}].

groups() ->
    [     
    {cluster, [sequence],
     [
      %test_empty_slave 
      %start_slaves
      %,auto_cluster
      cluster_test
      %,cluster_join
     %,cluster_leave
      %,cluster_remove
      %,cluster_remove2
      %,cluster_node_down
     ]}
     ].

init_per_suite(Config) ->
    %application:start(lager),
    application:set_env(mnesia_cluster, cluster_nodes, proplists:get_value(cluster_nodes,localconfig())),
    application:set_env(mnesia_cluster, table_definition_mod, proplists:get_value(table_definition_mod,localconfig())),
    application:set_env(mnesia_cluster,app_process, proplists:get_value(app_process,localconfig())),
    erlang:set_cookie(node(),gina),
    %application:ensure_started(mnesia),
    
    [{config, localconfig()} | Config].

end_per_suite(_Config) ->
    ct:pal("ending suite").
    %mnesia:stop().

localconfig() ->
    [
        {table_definition_mod, {mnesia_cluster_table, test_defs, []}},
        {app_process, mnesia_cluster},
        {cluster_nodes, {[
        'swan',
        'kitri',
        'giselle'], disc}},
        %{cluster_partition_handling, ignore}
        %{cluster_partition_handling, autoheal}
        {cluster_partition_handling, pause_minority},
       %% Retries when waiting for Mnesia tables in the cluster startup. Note that
        %% this setting is not applied to Mnesia upgrades or node deletions.
       {mnesia_table_loading_retry_limit, 10}
    ].
vmconfig() ->
    [
        {table_definition_mod, {mnesia_cluster_table, test_defs, []}},
        {app_process, mnesia_cluster},
        {cluster_nodes, {[
        'cluster@ginacontroller01.dev1.core.astra.rocks',
        'cluster@ginacontroller02.dev1.core.astra.rocks',
        'cluster@ginacontroller03.dev1.core.astra.rocks',
        'cluster@ginacontroller04.dev1.core.astra.rocks',
        'cluster@ginacontroller05.dev1.core.astra.rocks'], disc}},
        %{cluster_partition_handling, ignore}
        %{cluster_partition_handling, autoheal}
        {cluster_partition_handling, pause_minority},
        %% Timeout used when waiting for Mnesia tables in a cluster to become available.
        {mnesia_table_loading_retry_timeout, 30000},

       %% Retries when waiting for Mnesia tables in the cluster startup. Note that
        %% this setting is not applied to Mnesia upgrades or node deletions.
       {mnesia_table_loading_retry_limit, 10}
    ].
%%--------------------------------------------------------------------
%% cluster group
%%--------------------------------------------------------------------
start_slaves(_Config)->
    start_slave_connect([swan, kitri]),

    {ok, [mnesia,mnesia_cluster]} = application:ensure_all_started(mnesia_cluster),
    ok.

%%1. Start two mnesia nodes, they cluster
%%2. start mnesia in ct node
%%3. Add a node

cluster_test() ->
    %%1
    start_slave_connect([swan, kitri]),
    {ok, [mnesia,mnesia_cluster]} = application:ensure_all_started(mnesia_cluster),
    ct:pal("cluster_test:starting Z"),
    Z = start_slave(cluster_test_z),
    %wait_running(Z),
    %connect_node(Z),
    %true = rpc:call(Z, mnesia_cluster_utils, ensure_mnesia_running, []),
    %Node = node(),
    %ok = rpc:call(Z, mnesia_cluster_utils, join_cluster, [Node, ram]),
    Nodes = lists:sort(mnesia:system_info(running_db_nodes)),
    ct:pal("Running nodes should be 3 :~p~n", [Nodes]),
    {ok, _} = ct_slave:stop(cluster_test_z),
    Rest = lists:sort(mnesia:system_info(running_db_nodes)),
    ct:pal("Running nodes should be 2:~p~n", [Rest]),    
    ok.

cluster_join(_) ->
    Z = start_slave(cluster_join_z),
    N = start_slave(cluster_join_n),
    true = rpc:call(Z, mnesia_cluster_utils, ensure_mnesia_running, []),
    Node = node(),
    {error, {cannot_join_with_self, Node}} = mnesia_cluster_utils:join_cluster(Node),
    {error, {node_not_running, N}} = mnesia_cluster_utils:join_cluster(N),
    ok = mnesia_cluster_utils:join_cluster(Z),
    slave:stop(Z),
    slave:stop(N).
 
cluster_leave(_) ->
    Z = start_slave(cluster_leave_z),
    wait_running(Z),
    {error, node_not_in_cluster} = mnesia_cluster_utils:leave_cluster(Z),
    ok = mnesia_cluster_utils:join_cluster(Z, ram),
    Node = node(),
    [Z, Node] = mnesia_cluster_utils:running_nodes(),
    ok = mnesia_cluster_utils:leave_cluster(Z),
    [Node] = mnesia:system_info(running_db_nodes),
    slave:stop(Z).

cluster_remove(_) ->
    Z = start_slave(cluster_remove_z),
    wait_running(Z),
    Node = node(),
    {error, {cannot_remove_self, Node}} = mnesia_cluster_utils:leave_cluster(Node),
    ok = mnesia_cluster_utils:join_cluster(Z, ram),
    [Z, Node] = mnesia:system_info(running_db_nodes),
    ok = mnesia_cluster_utils:leave_cluster(Z),
    [Node] = mnesia:system_info(running_db_nodes),
    slave:stop(Z).

cluster_remove2(_) ->
    Z = start_slave(cluster_remove2_z),
    wait_running(Z),
    connect_node(Z),
    ok = mnesia_cluster_utils:join_cluster(Z, ram),
    Node = node(),
    [Z, Node] = mnesia:system_info(running_db_nodes),
    ok = rpc:call(Z, mnesia_cluster_utils, ensure_mnesia_running, []),
    ok = mnesia_cluster_utils:leave_cluster(Z),
    [Node] = mnesia:system_info(running_db_nodes),
    slave:stop(Z).

cluster_node_down(_) ->
    Z = start_slave(cluster_node_down),
    timer:sleep(1000),
    ok = mnesia_cluster_utils:join_cluster(Z, ram),
    slave:stop(Z),
    timer:sleep(1000),
    %%does schema still exits below?
    io:format("there should be no schema now").

forget_cluster_node(Config) ->
    [Swan, Kitri, Giselle] = cluster_members(Config),

    %% Trying to remove a node not in the cluster should fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Kitri, Swan) end),

    stop_join_start(Swan, Kitri),
    assert_clustered([Swan, Kitri]),

    %% Trying to remove an online node should fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Swan, Kitri) end),

    %ok = stop_app(Swan),
    %% We're passing the --offline flag, but Kitri is online
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Kitri, Swan, true) end),
    %% Removing some non-existant node will fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Kitri, non@existant) end),
    ok = mnesia_cluster_utils:forget_cluster_node(Kitri, Swan),
    assert_not_clustered(Kitri),
    assert_cluster_status({[Swan, Kitri], [Swan, Kitri], [Kitri]},
                          [Swan]),

    %% Now we can't start Swan since it thinks that it's still in the cluster
    %% with Kitri, while Kitri disagrees.
    %%GINA REVISIT THIS
    %assert_failure(fun () -> start_app(Swan) end),

    ok = mnesia_cluster_utils:reset(Swan),
     %%GINA REVISIT THIS
    %ok = start_app(Swan),
    assert_not_clustered(Swan),

    %% Now we remove Swan from an offline node.
    stop_join_start(Giselle, Kitri),
    stop_join_start(Swan, Kitri),
    assert_clustered([Swan, Kitri, Giselle]),
    %ok = stop_app(Kitri),
    %ok = stop_app(Swan),
    %ok = stop_app(Giselle),
    %% This is fine but we need the flag
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Kitri, Giselle) end),
    %% Also fails because kitri node is still running
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(Kitri, Giselle, true) end),
    %% But this works
    ok = rabbit_ct_broker_helpers:stop_node(Config, Kitri),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Kitri,
      ["forget_cluster_node", "--offline", Giselle]),
    ok = rabbit_ct_broker_helpers:start_node(Config, Kitri),
    %%GINA REVISIT
    %ok = start_app(Swan),
    %% Giselle still thinks its clustered with Swan and Kitri
    %%GINA REVISIT
    %%assert_failure(fun () -> start_app(Giselle) end),
    ok = mnesia_cluster_utils:reset(Giselle),
    %ok = start_app(Giselle),
    assert_not_clustered(Giselle),
    assert_clustered([Swan, Kitri]).

force_reset_node(Config) ->
    %[Swan, Kitri, _Giselle] = cluster_members(Config),
    [Swan, Kitri, _Giselle, _Juliet] = cluster_members(Config),

    stop_join_start(Swan, Kitri),
    %stop_app(Swan),
    mnesia_cluster_utils:force_reset(Swan),
    %% Kitri thinks that Swan is still clustered
    assert_cluster_status({[Swan, Kitri], [Swan, Kitri], [Kitri]},
                          [Kitri]),
    %% %% ...but it isn't
    assert_cluster_status({[Swan], [Swan], []}, [Swan]),
    %% We can rejoin Swan and Kitri
    mnesia_cluster_utils:update_cluster_nodes(Swan, Kitri),
    %start_app(Swan),
    assert_clustered([Swan, Kitri]).

ensure_ok(ok) -> ok;
ensure_ok({error, {already_started, _}}) -> ok.

host() -> [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

wait_running(Node) ->
    io:format("wait running"),
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    connect_node(Node),
    case rpc:call(Node, mnesia, system_info, [is_running]) of
        yes  -> ok;
        no -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

assert_cluster_status(Status0, Nodes) ->
    Status = {AllNodes, _, _} = sort_cluster_status(Status0),
    wait_for_cluster_status(Status, AllNodes, Nodes).

assert_clustered(Nodes) ->
    assert_cluster_status({Nodes, Nodes, Nodes}, Nodes).

assert_not_clustered(Node) ->
    assert_cluster_status({[Node], [Node], [Node]}, [Node]).

cluster_status(Node) ->
    {rpc:call(Node, mnesia_cluster_utils, cluster_nodes, [all]),
     rpc:call(Node, mnesia_cluster_utils, cluster_nodes, [disc]),
     rpc:call(Node, mnesia_cluster_utils, cluster_nodes, [running])}.

sort_cluster_status({All, Disc, Running}) ->
    {lists:sort(All), lists:sort(Disc), lists:sort(Running)}.

wait_for_cluster_status(Status, AllNodes, Nodes) ->
    Max = 10000 / ?LOOP_RECURSION_DELAY,
    wait_for_cluster_status(0, Max, Status, AllNodes, Nodes).

wait_for_cluster_status(N, Max, Status, _AllNodes, Nodes) when N >= Max ->
    erlang:error({cluster_status_max_tries_failed,
                  [{nodes, Nodes},
                   {expected_status, Status},
                   {max_tried, Max}]});
wait_for_cluster_status(N, Max, Status, AllNodes, Nodes) ->
    case lists:all(fun (Node) ->
                            verify_status_equal(Node, Status, AllNodes)

                   end, Nodes) of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_cluster_status(N + 1, Max, Status, AllNodes, Nodes)
    end.

cluster_members(_Config) ->
    [swan, kitri, giselle].

stop_join_start(Node, ClusterTo, Ram) ->
    %ok = stop_app(Node),
    ok = mnesia_cluster_utils:join_cluster(Node, ClusterTo, Ram).
    %ok = start_app(Node).

stop_join_start(Node, ClusterTo) ->
    stop_join_start(Node, ClusterTo, false). 

verify_status_equal(Node, Status, AllNodes) ->
    NodeStatus = sort_cluster_status(cluster_status(Node)),
    (AllNodes =/= [Node]) =:= rpc:call(Node, mnesia_cluster_utils, is_clustered, [])
        andalso NodeStatus =:= Status.

assert_failure(Fun) ->
    case catch Fun() of
        {error, _Code, Reason}         -> Reason;
        {error, Reason}                -> Reason;
        {error_string, Reason}         -> Reason;
        {badrpc, {'EXIT', Reason}}     -> Reason;
        {badrpc_multi, Reason, _Nodes} -> Reason;
        Other                          -> exit({expected_failure, Other})
    end.


%start_slave(NodeName) ->
%    EbinFilePath = filename:join([filename:dirname(code:lib_dir(mnesia_cluster, ebin)), "ebin"]),
%    TestFilePath = filename:join([filename:dirname(code:lib_dir(mnesia_cluster, ebin)), "test"]),
%    io:format("fpath ~p, tpath:~p~n",[EbinFilePath,TestFilePath]),
    %% start slave
%    {ok, Node} = ct_slave:start(NodeName, [
        %{boot_timeout, 10},
       %{erl_flags, lists:concat(["-pa ", EbinFilePath, " ", TestFilePath, " -setcookie gina"])}
%        {erl_flags, lists:concat([" -setcookie gina"])}
%    ]).

start_slave(Node) ->
    Host = gethostname(),
    EbinFilePath = filename:join([filename:dirname(code:lib_dir(mnesia_cluster, ebin)), "ebin"]),
    ct:pal("~p ebinfilepath: ~p~n",[Node,EbinFilePath]),
    Opts = [
             {boot_timeout, 30}, 
             {monitor_master, true}, 
             {startup_functions, []},
             {env, []},
             {erl_flags, lists:concat([" -pa ", EbinFilePath])}
            ],
    {ok, TestNode} = ct_slave:start(Host, Node, Opts),
    Res1 = rpc:call(TestNode,mnesia_cluster, start, []),
    ct:pal("Res1: ~p~n",[Res1]),
    Res3 = rpc:call(TestNode,mnesia, system_info, [is_running]),
    ct:pal("Res3  ~p~n",[Res3]),
    %Stop = ct_slave:stop(Node),
    TestNode.

start_slave_connect(Nodes) ->
    Host = gethostname(),
    EbinFilePath = filename:join([filename:dirname(code:lib_dir(mnesia_cluster, ebin)), "ebin"]),
    ct:pal("ebinfilepath: ~p~n",[EbinFilePath]),
    Opts = [
             {boot_timeout, 30}, 
             {monitor_master, true}, 
             {startup_functions, []},
             {env, []},
             {erl_flags, lists:concat([" -pa ", EbinFilePath])}
            ],
    TestNodes = lists:map(fun(Node) -> 
        {ok,TestNode} = ct_slave:start(Host, Node, Opts),
        C = net_kernel:connect_node(TestNode),
        ct:pal("for node ~p net_connect returned: ~p~n",[Node, C]),
        Res1 = rpc:call(TestNode,mnesia_cluster, start, []),
        ct:pal("for node ~p Res1: ~p~n",[Node, Res1]),
        Res3 = rpc:call(TestNode,mnesia, system_info, [is_running]),
        ct:pal("Res3  ~p~n",[Res3]),
        TestNode
    end, Nodes),
    ct:pal("TestNodes: ~p~n", [TestNodes]),
    %Stop = ct_slave:stop(Node),
    TestNodes.

test_empty_slave(Config) ->
     Host = gethostname(),
     ct:pal("hostname: ~p~n",[Host]),
     Node = test_node,
     Opts = [
             {boot_timeout, 30}, {monitor_master, true}, 
             {startup_functions, []},
             {env, []},
             {erl_flags, ""}
            ],
     {ok, TestNode} = ct_slave:start(Host, Node, Opts),
     {_, _, _} = rpc:call(TestNode, erlang, now, []),
     {ok, _} = ct_slave:stop(Node),
     ok.


stop_slave(NodeName) ->
    {ok, _} = ct_slave:stop(NodeName).

connect_node(Node) ->   
    net_kernel:connect_node(Node).
disconnect_node(Node) ->
    erlang:disconnect_node(Node).

gethostname() ->
     Hostname = case net_kernel:longnames() of
                    true->
                        net_adm:localhost();
                    _-> 
                        {ok, Name} = inet:gethostname(),
                        Name
                end,
     list_to_atom(Hostname).