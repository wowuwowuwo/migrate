
rm migrate.log

mkdir work

nohup cos_migrate_tool_for_restore -c oss-2-cos.conf > migrate.log &
