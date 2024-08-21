# PROJECT1:Census Data Standardization and Analysis Pipeline

The project is to clean, process, and analyze census data from a given source, including data renaming, missing data handling, state/UT name standardization, new state/UT formation handling, data storage, database connection, and querying. The goal is to ensure uniformity, accuracy, and accessibility of the census data for further analysis and visualization.

### Approaches: 
1:Data was given in an Excel sheet using Pandas in Python to convert the data into dataframe.
2:Performed the given tasks like renaming the column names,renaming State/Ut into State_UT. 
3:New State_UT formation(changed some of the state from Andhra pradesh to telangana,changed jammu and kashmir to ladakh for given districts).
4:Found the missing data by using the information from other columns.
5:Saved the data in MongoDB using pymongo
6:Then fetched data from MongoDB and uploaded it to MYSQL as a table using     mysql.connector.
7:Solved queries were given in the task:7 and output were produced in streamlit.
8:Displayed queries answers with Tables, Bar charts and Pie charts.

### Problems Faced: 
Faced some error in converting data from MongoDb collection into MYSQL table format:
Null values are not read by mysql so change null values to ‘0’.
Column lengths were too long in mysql so truncated the column length before uploading in mysql.
Got some errors in importing and connecting mysql.connector in python.


