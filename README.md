pynzbdex
========

Service Dependencies
--------------------

* Riak: document storage db for handling NZBs, header info.
* Redis: highspeed messaging passing/queing system for notifying population script
* MySQL: stores article relational data (Files, Reports, etc. )

* Binary Library Dependencies
 * Protobuf: riak client requires protobuf is installed
  * Arch: protobuf-2.4.1-2 or newer (community/protobuf)

* Python Library Dependencies
 * For Arch, better to install dist for these:
   * extra/python2-lxml
 * Until SQL alchemy bumps their official version to 0.8: http://pypi.python.org/pypi/SQLAlchemy/0.8.0b2
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
            #maxmemory 32mb
            #maxmemory-policy allkeys-lru
            maxmemory 256mb
            maxmemor-policy never-expire


Service Architecture
--------------------

    [usenet provider] ---> [pynntpprox] ---> [pynzbdex/cmddex] ---> [riak]   (full document)
                                                               ---> [redis]  (subset of document:  mesg_id:{ from, subject, date, newsgroups })

            ...

    [redis] ---> [pynzbdex/cmdpop] ---> [mysql] (decompose redis msg [pickle string] into standard relational model, [or mark as deleted])

Assembly Strategies and Notes
-----------------------------
pynzbdex necessarily groups articles together that comprise uploaded files.
it follow's Perl's aub's strategies for indirectly identifying alike files.

Skipped if:
* null subject or subject begins with Re:
* any subject with .htm or more than one !(exclaimation point)

Included if:
* IFF subject matches:
     ( $subject =~ m/^(.*)[^\d](1)\/(1)[^\d]/i ) ||
     ( $subject =~ m/^(.*\D)(\d+)\s*\/\s*(\d+)/ ) ||
     ( $subject =~ m/^(.*\D)(\d+)\s*\|\s*(\d+)/ ) ||
     ( $subject =~ m/^(.*\D)(\d+)\s*\\\s*(\d+)/ ) ||
     ( $subject =~ m/^(.*\D)(\d+)\s*o\s*f\s*(\d+)/i ) ||
     ( $subject =~ m/^(.*\D)(\d+)\s*f\s*o\s*(\d+)/i )
* subject includes filename extensions: these "hints" are used to help identify, not required like the previous item.
    (".gif", ".jpg", ".jpeg", ".gl", ".zip", ".au", ".zoo",
    ".exe", ".dl", ".snd", ".mpg", ".mpeg", ".tiff", ".lzh", ".wav" )



#CREATE DATABASE `pynzbdex` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci */

#`subj_key` varchar(767) CHARACTER SET latin1 DEFAULT NULL AFTER from_

# UNIQUE KEY `subj_key` (`subj_key`),

from pynzbdex import storage
from sqlalchemy.schema import CreateTable
print CreateTable(storage.sql.File.__table__)


-> need new scanner that just sees if there is a riak article backing the sql article.. simple loop over all articles...

-> factor out article deletion cascade/count

-> consider refactoring aggregator to make each scanner/processor class-based

-> fix "index completion" graphic
 -> check math, etc..
 -> files -> # of file records wrt # of articles grouped by association
 -> reports -> same but for report -> file



/etc/profile
# addl custom
PATH="$PATH:/opt/riak/bin"
export PATH 

search-cmd install article
search-cmd install group

search-cmd set-schema article schema/article.erl
search-cmd set-schema group schema/group.erl
