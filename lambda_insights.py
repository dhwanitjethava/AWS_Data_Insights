# Import required libraries
import json
import boto3
import sys
import pymysql
import pymysql.cursors

# Boto3 Client
s3_client = boto3.client('s3')

# Lambda Function
def lambda_handler(event, context):
    
    # TODO implement
    
     #1 - S3 bucket name
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    print('Data dumping to the Bucket - {}'.format(bucket_name))
    
    #2 - Object filename
    object_file = event['Records'][0]['s3']['object']['key']
    print('Object File Name - {}'.format(object_file))
    
    #3 - Objects(metadata) of object_file
    file_metadata = s3_client.get_object(Bucket=bucket_name, Key=object_file)
    print('File Metadata - {}'.format(file_metadata))
    
    #4 - File content
    data = file_metadata['Body'].read().decode('utf-8')
    print('JSON file contain data: {}'.format(data))
    
    #5 - RDS Database details
    db_host  = "<RDS Endpoint>"
    db_username = "<username>"
    db_password = "<password>"
    db_name = "<database name>"
    connection = None
    
    #6 - Connect to RDS database instance using PyMySQL
    try:
        connection = pymysql.connect(host = db_host, user = db_username, password = db_password, database = db_name, cursorclass=pymysql.cursors.DictCursor)
        print("RDS connection established!")
    except pymysql.MySQLError as e:
        print("ERROR: Could not connect to MySQL instance.")

    #7 - Create table in RDS Database (condition: if table not exist)
    try:
        cur = connection.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS employees.xyz (EmployeeId INT NOT NULL, FirstName VARCHAR(45) NOT NULL, LastName VARCHAR(45) NOT NULL, Age INT NOT NULL, JobTitle VARCHAR(45) NOT NULL, Location VARCHAR(45) NOT NULL, JoiningMonth VARCHAR(45) NOT NULL, JoiningYear VARCHAR(45) NOT NULL, SalaryAccount VARCHAR(45) NOT NULL, Sex VARCHAR(45) NOT NULL, MaritalStatus VARCHAR(45) NOT NULL, Education VARCHAR(45) NOT NULL, YearsOfExperience INT NOT NULL, PRIMARY KEY (EmployeeId))")
        connection.commit()
        print("Table created!")
    except:
        print("ERROR: Could not create table in MySQL instance.")
    
    #8 - Fatch data from S3 bucket and dump into RDS database
    empList = json.loads(data)
    print(empList,type(empList))
    
    with connection.cursor() as cur:
        # Iterate over S3 json file content and insert into MySQL database
        for emp in empList:
            try:
                empDict = emp
                sql = 'INSERT INTO employees.xyz (EmployeeId,FirstName,LastName,Age,JobTitle,Location,JoiningMonth,JoiningYear,SalaryAccount,Sex,MaritalStatus,Education,YearsOfExperience) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                val = (int(empDict["EmployeeId"]),empDict["FirstName"],empDict["LastName"],empDict["Age"],empDict["JobTitle"],empDict["Location"],empDict["JoiningMonth"],empDict["JoiningYear"],empDict["SalaryAccount"],empDict["Sex"],empDict["MaritalStatus"],empDict["Education"],empDict["YearsOfExperience"])
                cur.execute(sql,val)
                connection.commit()
            except:
                print("ERROR: Could not dump data in RDS")

    #9 - Delete After dumping data in RDS from S3 
    print("Deleting the data file from S3 bucket")
    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_file)
    except:
        print("ERROR: Deletion is not successful!")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
