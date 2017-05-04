PROJECT = mnesia_cluster
DEPS = lager 
dep_lager = git https://github.com/basho/lager.git

ERLC_OPTS = +debug_info +'{parse_transform,lager_transform}'
include erlang.mk
