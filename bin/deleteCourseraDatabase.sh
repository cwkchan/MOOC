#!/bin/bash

source importCourseraDatabase.config
DatabaseList=/home/purvak/DataImport/DatabaseList

usage () {
  echo "usage: $0 -d DatabaseName - to delete a single database/schema	       
	$0 -a - to delete all databases/schemas
	$0 -h - for help
	"
}

all=""
dbname=""
while getopts ":d:ha" option; do
  case "$option" in
    d)  dbname="$OPTARG" 
	echo $dbname ;;
    a)  all="1" ;;
    h)  usage
	echo "In case of further information contact Purva Kulkarni - purvak@umich.edu"
        exit 0 
        ;;
    :)  echo "ERROR: -$OPTARG requires an argument" 
        usage
        exit 1
        ;;
    ?)  echo "ERROR: Invalid option -$OPTARG" 
        usage
        exit 1
        ;;
  esac
done    

if [[ -z "$dbname" && -z "$all" ]]
then
	echo $dbname
	echo $all
  echo "ERROR: you must either specify a database name to delete using -d or use option -a to delete all databases"
  usage
  exit 1
fi

echo "INFO: Constructing the names of the databases/schemas"
ls -1 "$homeDir" |awk -F "[()]" '{ for (i=2; i<NF; i+=2) print $i }'|sort |uniq > $DatabaseList

if [ -n "$dbname" ]
then
wc=`grep "$dbname" ${DatabaseList} | wc -l`
	
	if [ $wc -ne 1 ]
	then 
	echo "ERROR: The database $dbname is not a valid database. Please choose from options below"
	cat ${DatabaseList}
	exit 1
	fi
echo "INFO: Deleting database $dbname"  
        mysql -u "$user" -p"$password"  -e "drop database if exists \`$dbname\` " 
else
echo "Deleting all databases" 
while read database; do
	echo "INFO: Deleting database $database"  
  	mysql -u "$user" -p"$password"  -e "drop database if exists \`$database\` " 

done < $DatabaseList 
fi

exit 0

