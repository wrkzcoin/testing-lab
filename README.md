#### daemon-dego-redis-cache.py
* Working in progress. Inspired by https://docs.turtlepay.io/blockapi/
* Rely on blockchain-data-collection-agent: https://github.com/TurtlePay/blockchain-data-collection-agent
* Currently testing with one new table, and read from redis.
* SQL:
```
CREATE TABLE `cache_blocks` (
`hash` varchar(64) NOT NULL,
`height` bigint(20) unsigned NOT NULL,
`timestamp` bigint(20) unsigned NOT NULL,
`txnum` smallint(5) unsigned NOT NULL,
`blockinfo` longtext CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL CHECK (json_valid(`blockinfo`)),
PRIMARY KEY (`height`),
KEY `timestamp` (`timestamp`),
KEY `hash` (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED;

```
#### Sorry, no guide yet :)

At least some tips:
```
export MYSQL_HOST_DEGO=localhost
export MYSQL_PORT_DEGO=3306
export MYSQL_USERNAME_DEGO=blockchain_user
export MYSQL_PASSWORD_DEGO=blockchain_password
export MYSQL_DATABASE_DEGO=blockchain_db
export DEGO_DAEMON_RPC=http://daemon-IP:daemon-rpc-PORT
```

#### zlib_vs_zstd.py output test
```originalString: 55639 bytes.
zlib level 9 compress for 50000 times.
completed compression zlib 50000 steps. Took 102s
new size from zlib: 24652 bytes.
zstd level 9 compress for 50000 times.
completed compression zstd 50000 steps. Took 60s
new size from zstd: 23283 bytes.
zlib decompress for 50000 times.
completed decompress zlib 50000 steps. Took 13s
zstd decompress for 50000 times.
completed decompress zstd 50000 steps. Took 3s```

```
