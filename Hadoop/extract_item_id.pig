

REGISTER /home/cloudera/workspace/pigudf/target/pigudf-1.0.jar;

load_data = LOAD '/home/cloudera/Downloads/IDLinesAndItemDescriptions.tsv' USING PigStorage('\t') as (line_desc:chararray,item_id:chararray,item_desc:chararray);

item_id_hash_mapping = LOAD 'db_work.item_id_hash_mapping' using org.apache.hive.hcatalog.pig.HCatLoader(); 

clean_data = FOREACH load_data GENERATE line_desc,REPLACE(REPLACE(line_desc,'  *',' '),'__*','_') as clean_line_desc;

hash_map_data = FOREACH clean_data GENERATE com.dezyre.pig.pigudf.ExtractItemId(clean_line_desc) as hashmap,clean_line_desc;

join_hash_map = FILTER hash_map_data BY hashmap is not null OR hashmap == '';

join_hash_map = JOIN join_hash_map BY hashmap LEFT OUTER, item_id_hash_mapping BY hash_key;

join_hash_map_filter = FILTER join_hash_map BY item_id_hash_mapping::hash_key is null;

join_hash_map_filter = DISTINCT join_hash_map_filter;

rank_join_hash_map_filter = RANK join_hash_map_filter BY join_hash_map::hashmap DENSE;

item_id_gen = FOREACH rank_join_hash_map_filter GENERATE
		join_hash_map::hashmap as hash_key,
		($0 + $COUNT) as item_id,
		join_hash_map::clean_line_desc as line_desc;

STORE item_id_gen INTO 'db_work.item_id_hash_mapping' USING org.apache.hive.hcatalog.pig.HCatStorer();
