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
