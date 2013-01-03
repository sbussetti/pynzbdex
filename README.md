pynzbdex
========

Service Dependencies
--------------------

* Riak: document storage db for handling NZBs, header info.
* Redis: highspeed messaging passing/queing system for notifying population script
* MySQL: stores article relational data (Files, Reports, etc. )

* Binary Library Dependencies
 * Protobuf: riak client requires protobuf is installed
  * Arch: protobuf-2.4.1-2 or newer

* Python Library Dependencies
 * See requirements.txt

* Config Dependencies:   
 * riakkit:    search must be enabled on bitcask,
               indexes must be installed for each bucket:
               search-cmd install group
               search-cmd install article
 * beam modules:    beam modules must be compiled in the regular way and installed for Riak
                    https://github.com/basho/riak_kv/blob/master/src/riak_kv_mapreduce.erl
                    https://github.com/basho/riak_function_contrib/blob/master/mapreduce/erlang/delete_keys.erl
 * mysql:   expects all schemas to be UTF8
 * redis:   runs in cache mode (implicit TTL)
            maxmemory 32mb
            maxmemory-policy allkeys-lru


Service Architecture
--------------------

    [usenet provider] ---> [pynntpprox] ---> [pynzbdex/cmddex] ---> [riak]   (full document)
                                                               ---> [redis]  (subset of document:  mesg_id:{ from, subject, date, newsgroups })

            ...

    [redis] ---> [pynzbdex/cmdpop] ---> [mysql] (decompose redis msg [json string] into standard relational model, [or mark as deleted])
