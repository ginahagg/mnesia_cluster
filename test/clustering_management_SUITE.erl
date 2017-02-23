-module(clustering_management_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(LOOP_RECURSION_DELAY, 100).

-define(Nodes,[kitri,swan]).

%-record(test_tab_item, {id, name, description}).

init_per_suite(Config) ->
    ct:pal("==== TEST SUITE: ~s ====", [?MODULE_STRING]),
    erlang:set_cookie(node(),gina),
    NodeNames = start_slave_connect(?Nodes),
    {ok, [mnesia,mnesia_cluster]} = application:ensure_all_started(mnesia_cluster),
    
    [{nodes, NodeNames}|Config].

end_per_suite(_Config) ->
    ct:pal("==== /TEST SUITE: ~s ===", [?MODULE_STRING]),
    %Nodes = ct:get_config(nodes, ?Nodes),
    %Host = gethostname(),
    %lists:map(fun(Node)-> ct_slave:stop(Node) end, Nodes),
    %application:ensure_all_stopped(mnesia_cluster),   
    ok.

%% Case
init_per_testcase(TestCase, Config) ->
    ct:pal("==== TEST CASE: ~s ====",[TestCase]),
    Config.

end_per_testcase(TestCase, Config) ->
    ct:pal("==== /TEST CASE: ~s ===",[TestCase]),
    Config.

all() ->
    [
     cluster_test, forget_cluster_node].

suite() ->
    [].

localconfig() ->
    [
        {table_definition_mod, {mnesia_cluster_table, test_defs, []}},
        {app_process, mnesia_cluster},
        {cluster_nodes, {[
        'swan',
        'kitri',
        'giselle'], disc}},
        %{cluster_partition_handling, ignore}
        {cluster_partition_handling, autoheal},
        %{cluster_partition_handling, pause_minority},
       %% Retries when waiting for Mnesia tables in the cluster startup. Note that
        %% this setting is not applied to Mnesia upgrades or node deletions.
       {mnesia_table_loading_retry_limit, 10}
    ].
%%--------------------------------------------------------------------
%% cluster group
%%--------------------------------------------------------------------
%%1. Start two mnesia nodes, they cluster
%%2. start mnesia in ct node
%%3. Add a node and let it join the cluster

cluster_test(_Config) ->
    %%1   
    ct:pal("cluster_test:starting Z"),
    Z = start_slave(cluster_test_z),
    Nodes = lists:sort(mnesia:system_info(running_db_nodes)),
    ?assertEqual(4, length(Nodes)),
    ct:pal("Running nodes should be 4 :~p~n", [Nodes]),
    {ok, _} = ct_slave:stop(cluster_test_z),
    Rest = lists:sort(mnesia:system_info(running_db_nodes)),
    ct:pal("Running nodes should be 3 since we stopped one node ::~p~n", [Rest]),
    ?assertEqual(3, length(Rest)), 
    {comment, ""}.

%% 1. Start a node, but don't make it join the cluster yet
%% 2. Try to remove it without it being in the cluster
%% 3. Then let it join the cluster
%% 4. Let cluster forget it 
        %% a) while it is online and mnesia running
        %% b) while saying it is offline but in reality node is online
        %% c) try to remove a non-existant node from the cluster
        %% d) stop mnesia, and try to remove it.
forget_cluster_node(_Config) ->
    
    %% Start the node, but don't make it join yet. Not in cluster
    ForgetZ = start_slave_not_joined(forget_z),
    %% Trying to remove a node not in the cluster should fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(ForgetZ, true) end),

    %%Now start mnesia_cluster in the node
    stop_join_start(ForgetZ),
    Nodes = lists:sort(mnesia:system_info(running_db_nodes)),
    ?assertEqual(4, length(Nodes)),

    ct:pal("next is forgetting ForgetZ while mnesia is running and node is online"),
    %% Trying to remove an online node should fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(ForgetZ, true) end),

    ct:pal("next is forgetting ForgetZ while by saying node is offline, but node is online in actuality"),
    %% We're passing the --offline flag, but ForgetZ is online
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(ForgetZ, false) end),

    ct:pal("next is forgetting some non-existant node"),
    %% We're passing the --offline flag, but ForgetZ is online
    %% Removing some non-existant node will fail
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(non@existant, true) end),
    
    %% Turn of mnesia, so that forget_cluster will do its job.
    ct:pal("next is turn off mnesia on ForgetZ so that we can maybe forget this node."),
    %% We're passing the --offline flag, but ForgetZ is online
    rpc:call(ForgetZ, mnesia, stop,[]),
    assert_failure(fun () -> mnesia_cluster_utils:forget_cluster_node(ForgetZ, true) end),
    {comment,""}.
    

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
    TestNode.

start_slave_not_joined(Node) ->
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
    ct:pal("~p  started ~n",[TestNode]),
    TestNode.

stop_join_start(Node) ->
    rpc:call(Node, mnesia_cluster, start,[]),
    ct:pal("restarted mnesia_cluster for Node: ~p~n",[Node]),
    ok.

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


assert_failure(Fun) ->
    case catch Fun() of
        {error, _Code, Reason}         -> Reason;
        {error, Reason}                -> Reason;
        {error_string, Reason}         -> Reason;
        {badrpc, {'EXIT', Reason}}     -> Reason;
        {badrpc_multi, Reason, _Nodes} -> Reason;
        Other                          -> exit({expected_failure, Other})
    end.