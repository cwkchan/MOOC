from sqlconnection import *
import glob

connection = 'uselab_mooc'
print 'Connecting to: '+connection
uselabconn = get_connection(connection)
uselabcursor = uselabconn.cursor()

print 'Creating table: coursera_pii'
print ''
sql_drop_coursera_pii = '''DROP TABLE IF EXISTS `coursera_pii`;'''
uselabcursor.execute(sql_drop_coursera_pii)

sql_create_coursera_pii = '''
  CREATE TABLE IF NOT EXISTS `coursera_pii` (
  `user_id` INT NOT NULL,
  `session_id` VARCHAR(255) NOT NULL,
  `name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
  `email` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`user_id`, `session_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
  '''
uselabcursor.execute(sql_create_coursera_pii)

print 'Copying information to coursera_pii...'
input_files = glob.glob('*.csv')   # Get all .csv files in current directory
input_files[:] = [x for x in input_files if x != 'coursera_pii.csv']   # Don't count coursera_pii.csv if it already exists

total_row_num = 0
for input_file in input_files:
  print '  Reading file: '+input_file
  print '  Processing...'
  with open(input_file,'rU') as IN:
    sql_insert_into_coursera_pii = '''INSERT INTO coursera_pii (user_id, session_id, name, email) VALUES (%s,%s,%s,%s);'''
    for row_num, row in enumerate(IN):
      row = row.strip()            # get rid of trailing \n
      row = row.replace('"',"'")   # change " to '
      row = row.split(';')
      # Error checking
      if len(row) != 3:
        print '    WARNING (len(row)!=3): '+row
      values = (row[0],input_file.replace('.csv',''),row[1],row[2])
      uselabcursor.execute(sql_insert_into_coursera_pii, values)
      uselabconn.commit()
      
    total_row_num += row_num + 1

  print '  Done. ('+str(row_num+1)+' rows)'
  print ''

print 'Done. ('+str(total_row_num)+' total rows)'
print ''

uselabconn.close()
print 'Connection closed.'
