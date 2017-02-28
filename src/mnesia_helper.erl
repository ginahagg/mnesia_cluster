-module(mnesia_helper).

-compile(export_all).

-record(test_tab_item, {id, name, description}).

write(Table, Records)->
    Trans = fun()->
                    lists:foreach(fun(Record)->
                                          mnesia:write(Table,Record, write)
                                  end, Records)
            end,
    mnesia:transaction(Trans).


add(Num) ->
    LastNum = list_max(mnesia:dirty_all_keys(test_tab)),	
	Recs = [
	 begin
		Id = LastNum + N,
		Name = integer_to_list(N),
		Desc = lists:concat(["this is ", N, "th"]),
		#test_tab_item{id=Id, name=Name, description=Desc}		
	 end 
	|| N <- lists:seq(1, Num)
	],
	write(test_tab, Recs).

list_max([]) -> none;
list_max([H | T]) ->
    lists:foldl(fun erlang:max/2, H, T);
list_max(_) -> badarg.
