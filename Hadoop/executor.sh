

RESULT=`hive -e "use db_work; select max(item_id) from item_id_hash_mapping;"`
echo "RESULT is $RESULT"

COUNT=`echo $RESULT | cut -d' ' -f1`
echo $COUNT | grep "^[0-9]\+"
if [ $? -ne 0 ] ; then
	COUNT=10000000;
fi

echo "COUNT IS $COUNT"

pig -useHCatalog -x local -param COUNT=${COUNT} -f ../pig/extract_item_id.pig
if [ $? -eq 0 ]; then
	echo "Pig execution successful."
else
	echo "Failed to complete the Pig job. Please check."
	exit 100;
fi

sqoop export --connect jdbc:mysql://localhost/store --username root --password cloudera --table item_id_hash_mapping --export-dir /user/hive/warehouse/db_work.db/item_id_hash_mapping --input-fields-terminated-by '\001';
if [ $? -eq 0 ]; then
        echo "Sqoop execution successful."
else
        echo "Failed to complete the Sqoop job. Please check."
        exit 100;
fi

