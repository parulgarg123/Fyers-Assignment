# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 00:48:37 2019

@author: parulg014@gmail.com
"""

import pandas as pd
import pymysql
import os
import logging
import datetime

'''
Basic Logging 
'''
print("Task Started")
logging.basicConfig(filename="LogFile.log", level=logging.INFO)


def __log(level, message):
    """
    Used to log logs in LogFile.log file
    """
    if level == 1:
        logging.info(" " + str(datetime.datetime.now()) + " " + message)
    if level == 2:
        logging.error(" " + str(datetime.datetime.now()) + " " + message)
    if level == 3:
        logging.critical(" " + str(datetime.datetime.now()) + " " + message)


__log(1, 'Task Started')

try:	
    def cleaning(string):
        """
        Utility function for cleaning DataFrame data. 
        Removing special characters and numerical data
        
        Return clean Data 
        """

        if type(string) == float or type(string) == int:
            return string
        res = ''
        if string != string:
            return string
        string = string.replace("\\r", "")
        string = string.replace("\\n", "")
        string = string.replace("\\b", "")
        string = string.replace("\\t", "")
        for i in string:
            if i.isalpha():
                res = res + i
        return res.lower()


    def db_conection():
        """
        Used for creating database connection
        
        Return Connection
        """
        try:
	        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='test', db='EMP')
	        __log(1,"DB Connection Established")
	        return conn
        except Exception as e:
#			print (e)
             __log(3,"Database Connection Establishment Error, Fatal Error, Exiting from Code")
             exit()

    def table_create(name):
        """
		It will create table if not exists in EMP database
		"""
        query = """CREATE TABLE  if not exists `emp`.`"""+name+"""` (`Id` int(11) NOT NULL,`First name` varchar(45) NOT NULL,`Last Name` varchar(45) NOT NULL,`deparment` varchar(45) NOT NULL,`salary` int(11) NOT NULL,PRIMARY KEY (`Id`))"""
        try:
            conn = db_conection()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
#            print ("table Success")
            __log(1, 'Table Create if not exists')
        except Exception as e:
            print (e)
            __log(2,'Table creation check and craete Error')
            
			
    def insert_update(record,name):
        """
        Establish connection with database.
        try to insert record in database.table
        if error then try to update that record in database with id
        if error again logging unknown error with description
        """
        try:
            conn = db_conection()
            cur = conn.cursor()
        except Exception as db:
            __log(2, "DataBase Connection Error" + db.args[1])

        try:
            query_insert = "INSERT into "+name+" VALUES ('" + str(record['Id']) + "', '" + record[
                'First name'] + "', '" + record['Last Name'] + "', '" + record['deparment'] + "', '" + str(
                int(record['salary'])) + "')"
            cur.execute(query_insert)
            conn.commit()
            # print ("Insert Success",record['Id'])
            __log(1, 'Insert Success. ID: ' + str(record['Id']))

        except pymysql.Error:
            # print ('Duplicate Error Found ID: ',record['Id'])
            __log(2, 'Duplicate Error Found ID: ' + str(record['Id']))

            query_update = "UPDATE "+name+" SET `First name` = %s , `Last Name`= %s,`deparment`= %s,`salary` = " \
                           "'%s' WHERE `Id` = '%s' "
            val = (record[1], record[2], record[3], record[4], record[0])
            cur.execute(query_update, val)
            conn.commit()
            # print ("Update Success, ID",record['Id'])
            __log(1, 'Duplicate Error Updated with New Values, ID: ' + str(record['Id']))

        except Exception as e:
            # print (e)
            __log(2, 'Unknown Error, Skipping Record having id' + str(record['Id']))


    def generate_DataFrame(file_path):
        """
        Generating Dataframe and cleaning it
        
        Returns cleaned dataframe
        """
        # print ("Generating DataFrame")
        __log(1, 'Generating DataFrame....')

        df = pd.read_csv(file_path)
        df = df.rename(columns=lambda x: x.strip())
        df = df.dropna()

        for i in list(df.keys()):
            df[i] = df[i].apply(cleaning)

        # print ("DataFrame Generated Successfully")
        __log(1, 'DataFrame Generated Sucessfully.')
        return df


    def get_path(file_name):
        """
        Crawl in current directory to find text file
        Return : if Success, File path
                 if Failure, -1
        """
        path_project = os.getcwd()
        list_files = os.listdir(path_project)
        # print ("Getting File in your Current Directory", path_project)
        __log(1, 'Gatering Files from current Directory.')

        #        for i in list_files:
        if file_name in list_files:
            file_path = path_project + '\\' + file_name
            __log(1, 'Relevant File Found.')
            return file_path
        else:
            __log(2, 'File Not Found.')
            return -1
        # print ("File Found",file_path)

    def main():
        """
        main function to call the sub functions
        """
#    file_name = input("Please Provide File Name (e.g: sample_data.txt): ")
        file_name = "sample_data.txt"
        file_path = get_path(file_name)
        if file_path != -1:
            df = generate_DataFrame(file_path=file_path)
#            table_name = input("Please Enter table Name(emp_details)")
            table_name = "emp_details"
            table_create(table_name)
            for x, i in df.iterrows():
                insert_update(i,table_name)
                print('processing id:', x)

    #    Calling main function
    if __name__ == "__main__":
        main()

except Exception as rootError:
    print(rootError)
    __log(3, 'Fatal Error')

__log(1, 'Task Ended \n')
print(
    "Task Ended \nRefer Log File for InDepth details \nUncomment all the print statement(s) to view details in console.")
