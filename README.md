#### daemon-dego-redis-cache.py
* Working in progress. Inspired by https://turtlecoin.github.io/wallet-api-docs/
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
