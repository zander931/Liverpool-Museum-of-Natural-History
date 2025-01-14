source .env
export PGPASSWORD=$DB_PASS
psql -h $DB_HOST -d $DB_NAME -U $DB_USER -p $DB_PORT
