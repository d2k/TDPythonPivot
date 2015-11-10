#!/yourPath/python

##########################################################
### Author: Ulrich Arndt
### Company: data2knowledge Gmbh
### website: www.data2knowledge.de
### First creation date: 29.10.2015
### License:
# The MIT License (MIT)
#
# Copyright (c) 2015 by data2knowledge GmbH, Germany
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
##########################################################

import teradata
import argparse
import logging

logger = logging.getLogger(__name__)

###########################################################
# Process the command line information
###########################################################

parser = argparse.ArgumentParser(description='Teradata Pivot SQL generator')
parser.add_argument('-l','--logonfile', nargs=1,
                   help='logon file for the pivot operation')
parser.add_argument('-c','--configfile', nargs=1,
                   help='config file for the pivot generation')

args = parser.parse_args()

###########################################################
# Set Default config and logon info files in case they are not given at programm call
###########################################################
configfile = ''
if args.configfile:
	configfile = args.configfile[0]
else:
	configfile = './appini/demo.ini'

logonfile = ''
if args.logonfile:
	logonfile = args.logonfile[0]
else:
	logonfile = './dwl/demo.dwl'


###########################################################################
# Function Definition
###########################################################################

#######
# generate a column list string for a given database and tablename
####### 
def genTableColumnList(dbName, tableName,session,runAlways):
	rc = 0
	colList = ''
	sql = '''
select cast(columnname as varchar(128)) as columnname
from dbc.columnsV 
where databasename = \''''+dbName+'''\'
	  and tablename = \''''+tableName+'''\'
order by columnid
'''
	for row in session.execute(sql, runAlways=runAlways):
		rc += 1
		if rc == 1:
			colList = '\t'+row[0]
		else:
			colList = colList + '\n\t,' + row[0] 
	return colList	


############################################################
# Main
###########################################################

############# 
#init udaExec
############# 
udaExec = teradata.UdaExec (userConfigFile=[configfile,logonfile])

session = udaExec.connect(
	method		='rest', 
	system		='${system}',
	username	='${user}', 
	password	='${password}',
	host		='${host}',
	port		='${port}'
	)
	
with session: 

	############# 
	#init vars
	#############	
	rangeList = []
	rangeString = ''
	
	quoteString = ''
	if ( udaExec.config['rangeType'] == 'String'):
		quoteString = '\''
	
	ct = 0
	############# 
	#create range string with rangeQuery result set
	#############	
	for row in session.execute('${rangeQuery}'):
		ct += 1
		rangeList.append(row[0])
		if (ct == 1): 
			rangeString = quoteString+str(row[0])+quoteString
		else: 
			rangeString = rangeString + ',' + quoteString+str(row[0])+quoteString
	
	
	denormColInList = udaExec.config['denormVarInList'].split(',')
	denormColOutList = []

	if (udaExec.config['denormVarOutList'] == ''):
		denormColOutList = denormColInList
	else:
		denormColOutList = udaExec.config['denormVarOutList'].split(',')

	idList = [] 
	for id in range(1,ct+1):
		idList.append(id)
	
	############# 
	#get conf file parameter values
	#############	
	rangeVar 		= udaExec.config['rangeVar']
	DB 				= udaExec.config['DB']
	objectName 		= udaExec.config['objectName']
	whereCondition  = udaExec.config['whereCondition']
	
	denormAggFunctionList = udaExec.config['denormAggFunctionList'].split(',')
	replaceNullValueList = udaExec.config['replaceNullValueList'].split(',')
	
	if (len(denormAggFunctionList) == 1):
		denormAggFunctionList = [denormAggFunctionList[0]] * len(denormColInList)
	if (len(replaceNullValueList) == 1):
		replaceNullValueList = [replaceNullValueList[0]] * len(denormColInList)

    ############# 
	#add range condition to where condition in case where condition is defined 
	#############	
	if (whereCondition != ''):
		whereCondition = whereCondition + '\n\tAND ' + rangeVar + ' in (' + rangeString + ')\n'
	else: 
		whereCondition = 'WHERE ' + rangeVar + ' in (' + rangeString + ')\n'
	
    ############# 
	#create aggregate function
	#############		
	aggString = ''
	for id1 in range(len(denormColInList)):
		colNameIn = denormColInList[id1]
		colNameOut = denormColOutList[id1]
		for id2 in range(len(idList)):
			rowAgg = '	, coalesce(' + str(denormAggFunctionList[id1]) + '( case '\
	           	      + 'when '\
			       	  + rangeVar + ' = ' + quoteString + str(rangeList[id2]) + quoteString  \
		        	  + ' then ' + colNameIn + ' else null end), ' + str(replaceNullValueList[id1]) + ')' \
		        	  + ' (Title \'Column Info Source:' + colNameIn + ' - Row Condition : '\
		              + str(rangeList[id2]) +'\') as ' + colNameOut + '_' \
		          	  + str(id2+1).zfill(4) + '\n'			
			aggString = aggString + rowAgg

	groupByList = udaExec.config['groupbyVarList'].split(',')
	groupByString = ''
	ct = 0
	for col in groupByList:
		ct += 1
		if(ct == 1):
			groupByString = '	' + col + '\n'
		else:
			groupByString = groupByString + '	,' + col + '\n'
	
	############# 
	#concat final SQL 
	#############	
	run_id = udaExec.config['runNumber']
	
	pivotSql = 'select \n' + groupByString + aggString + ',\' ${appName} \' as application_name, \'${runNumber}\' as run_id FROM ' + DB + '.' + objectName + '\n' \
                + whereCondition + 'Group By ' +  groupByString + '\n'
	
    ############# 
	#materialize if needed else only print the generated SQL
	#############	

	if (  udaExec.config['materializeFlag'] ==  'True'):
	
		
		if (  udaExec.config['createTableFlag'] ==  'True'):
			
			if (  udaExec.config['replaceExistingTableFlag'] ==  'True'):
				sql = 'drop table ${materializeDB}.${materializeTable}';
				session.execute(sql,ignoreErrors=[3807])

			sql = 'create table ${materializeDB}.${materializeTable} as (\n' + pivotSql + ') with data ${materializePI}; ' 
			session.execute(sql)
			udaExec.checkpoint("Table created")			    
		
			sql = 'comment table ${materializeDB}.${materializeTable} \'${materializeComment}\''
			session.execute(sql)
			udaExec.checkpoint("Table comment set")
		
			for id1 in range(len(denormColInList)):
				colNameIn = denormColInList[id1]
				colNameOut = denormColOutList[id1]
				for id2 in range(len(idList)):
					sql = 'COMMENT ON COLUMN ${materializeDB}.${materializeTable}.' + colNameOut + '_' \
						  + str(id2+1).zfill(4) + ' AS  \'Column Info Source:' + str(denormAggFunctionList[id1]) + '(' + colNameIn + ') - Row Condition : '\
						  + str(rangeList[id2]) +'\';' + '\n'
					session.execute(sql)
			udaExec.checkpoint("Column comment set")
			
		else:
			sql = 'insert into ${materializeDB}.${materializeTable} \n' + pivotSql  
			session.execute(sql)
			udaExec.checkpoint("Insert / Select")			
		        
		

	else:
		print(pivotSql)

# Script completed successfully, clear checkpoint
# so it executes from the beginning next time
udaExec.checkpoint()
print('Success')
exit(0)