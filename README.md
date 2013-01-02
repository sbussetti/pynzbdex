
Service Dependencies

Riak: document storage db for handling NZBs, header info.
    Arch: https://aur.archlinux.org/packages/riak/

Binary Dependencies
Protobuf: riak client requires protobuf is installed
    Arch: protobuf-2.4.1-2 or newer

Config Dependencies:   
    riakkit:    search must be enabled on bitcask,
                indexes must be installed for each bucket:
                search-cmd install group
                search-cmd install article
    beam modules:
            https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapreduce.erl
