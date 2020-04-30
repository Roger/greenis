Greenis - green is the new red 
==============================

Very basic WIP implementation of redis to learn some rust.

Features
--------

* RESP protocol parsing using combine (any redis client can be connected)
* Async server using tokio
* Basic commands: get, set, delete, ping, append, keys, exists, etc

Goals
-----

* Support some redis commands using standard clients
* Try rust-evmap
