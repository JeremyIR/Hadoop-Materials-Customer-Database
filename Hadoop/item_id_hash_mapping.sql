
CREATE TABLE `item_id_hash_mapping` 
(
  `hash_key` varchar(32) DEFAULT NULL,
  `item_id` bigint(20) NOT NULL DEFAULT '0',
  `line_desc` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`item_id`)
);
