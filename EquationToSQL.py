# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

# Access the secrets using dbutils.secrets.get
AZURE_OPENAI_API_KEY = dbutils.secrets.get(scope="openaiAccess", key="openaiKey")
AZURE_OPENAI_ENDPOINT = dbutils.secrets.get(scope="openaiAccess", key="openaiendpoint")

# COMMAND ----------

#Steps TO DO:
#1. CSV LOAD functionality with aggregate category validate
    #a. Each Aggregate category must have different prompting 
    #b. Once equation is uploded into the transpiler, the tool have to categorise the simple, medium by definitions
    #c. IF it is simple then current notebook will run from cell-10 (Main Functions)
    #d. Before initalising, dedup and equation clean up process have to be in place
#2. 

# COMMAND ----------

AZURE_OPENAI_API_KEY = '56c25056db95494ab740150f4c591ec4'
AZURE_OPENAI_ENDPOINT = 'https://oai-fds-eastus2.openai.azure.com/'

# COMMAND ----------

import os
from openai import AzureOpenAI
client = AzureOpenAI(
  api_key = AZURE_OPENAI_API_KEY,  
  api_version = "2024-02-01",
  azure_endpoint = AZURE_OPENAI_ENDPOINT
)

# COMMAND ----------

# DBTITLE 1,Aggregate Preprocessing / Simple equation format
#####  NEED MORE REFINED SOLUTION TO READ THE EQUATION ############
from pyspark.sql import functions as F
import re

# Read CSV file into a Spark DataFrame
file_path = 'dbfs:/FileStore/tables/for_demo/simple_aggregate_eq-1.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Collect the DataFrame rows into a list of dictionaries
rows_list = df.collect()
row_dict_list = [row.asDict() for row in rows_list]

# Create a dictionary with key-value pairs where the value is a box codes
result_dict = {row['aggregate_names']: re.findall(r'[a-zA-Z0-9]+', row['equations']) for row in row_dict_list}

display(result_dict)
#comments:
#Show the original equation
#Error Handling mechanisms
#repo structure


# COMMAND ----------

# DBTITLE 1,Dedup Check
# Check if equation exists in the transpiler table - 
    # If exists Need design solution how to process the existing equation 1. Eaither replace it with new SQL 2. else use the same sql from privous run
from pyspark.sql.functions import col

df = spark.sql(f"SELECT * FROM fdscatalog.fds.Aggregate_Transpiler_script")
filtered_df = df.filter(
    (col("Equation") == result_dict['aggregate_names'])
)
if filtered_df.count() > 0:
    print("The specific values exist in the SQL table.")
else:
    print("The specific values do not exist in the SQL table.")

# COMMAND ----------

# DBTITLE 1,Schema Reterival
# Load the table schema data into a Spark DataFrame
schema_df = spark.sql("DESCRIBE fdscatalog.fds.dq_mapping") ### Try this with entire catalog 

# Convert the DataFrame to a JSON string
schema_json = schema_df.toPandas().to_json(orient='records')

# indexing

# Prepare the content for Azure OpenAI
schema_content = "Table schema data: " + str(schema_json)


# COMMAND ----------

# DBTITLE 1,Prompting Cell

# Function to generate Box code SQL queries using GPT-4o
def box_to_mapping(input_data):

    # Convert the dictionary to a string format for the prompt
    input_data_str = str(input_data)
    
    # Formulate the prompt for GPT-4
    prompt = f"""
    You are an assistant that generates SQL queries based on dictionary input. The input dictionary contains keys that represent some identifier, and each key has a list of values. These values correspond to specific rows in a database.

    For each list in the dictionary, generate a single SQL query using the `IN` clause to retrieve all relevant rows from a table.

    Assume the following:
    - The table to query from is `fdscatalog.fds.dq_mapping`
    - The column to match against is `boxc`.

    The input dictionary is: {input_data_str}

    Generate the SQL queries for each list:
    """
    
    response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "You are an AI model that helps with SQL and data processing."},
            {"role": "user", "content": prompt}
        ],
        model="gpt-4o",
        max_tokens=1000,
        temperature=0.5
    )
    # Extract the text content from the response
    response_text = response.choices[0].message.content.strip()
    
    # Use regular expression to extract SQL statements from the response text
    sql_statements = re.findall(r"SELECT.*?;", response_text, re.DOTALL)
    
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(input_data.items())], indent=4)

    return input_json

# Function to generate Final SQL queries for Aggregate equation using GPT-4o
def generate_sql_select_statements(sql_queries_dict,finalquery_dict):
    # Prepare the prompt
    prompt = f"""
    You are an SQL assistant. I have a list of SQL results stored in a dictionary. 
    For each result set, generate a SQL SELECT statement using 'flash_report_boxcode_aggregates' as the table name.
    The SELECT statement should include a WHERE clause that combines common column names with an OR condition if a column has multiple values.

    Here is the dictionary:

    {{{sql_queries_dict}}}

    For each 'result' entry in the dictionary, generate a SELECT statement that could be used to filter for these values in 'table1'.
    Please output the full SELECT statement for each dictionary entry separately.
    
    Here is the example to learn from :
    Input dict:
    'result': [
           {{'column1': 'value1', 'column2': 'value2'}},
            {{'column1': 'value3', 'column2': 'value4'}}
        ]
    Expected sql output for the result set:
    SELECT * FROM table1 WHERE (column1 = 'value1' OR column1 = 'value3') AND (column2 = 'value2' OR column2 = 'value4');

    """
    
    # Make the API call to GPT-4
    response =  client.chat.completions.create(
      model="gpt-4o",
      messages=[
            {"role": "system", "content": "You are an expert in SQL query construction."},
            {"role": "user", "content": prompt}
        ]
    )
    
    # Extract the SQL SELECT statements from the response
    response_text = response.choices[0].message.content.strip()
    sql_statements = re.findall(r"SELECT \*.*?;", response_text, re.DOTALL)
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(finalquery_dict.items())], indent=4)


    return input_json



# COMMAND ----------

# DBTITLE 1,Prompt 2 input
def prompt2_input_clean(sql_queries):
    # Convert the sql_queries string to a dictionary
    sql_queries_dict = json.loads(sql_queries)

    # Iterate over each dictionary in the list
    for entry in sql_queries_dict:
        sql = entry['SQL_Query']
        key = entry['aggregate_name']
        
        try:
            # Execute the SQL statement using Databricks' spark.sql()
            result_df = spark.sql(sql)
            
            # Collect the result as a list of dictionaries (or any other format you prefer)
            result_data = result_df.collect()
            
            # Store the result back in the dictionary
            entry['result'] = [row.asDict() for row in result_data]
            entry['error'] = None
            
            print(f"Execution successful for key '{key}'")
        except Exception as e:
            # Handle any SQL execution errors
            entry['result'] = None
            entry['error'] = str(e)
            print(f"Error executing SQL for key '{key}': {e}")
    return (sql_queries_dict)

# COMMAND ----------

# DBTITLE 1,Main
import json

#Function call to reterive box codes from mapping table (Prompt1)
sql_queries = box_to_mapping(result_dict)

sql_queries_dict = prompt2_input_clean(sql_queries)
# Above function output is feed into the below function for Prompt2 to generate aggregate equation
sql_select_statements = generate_sql_select_statements(sql_queries_dict,result_dict)


# COMMAND ----------

# DBTITLE 1,Write to Transpiler Table
# Here Generated SQL script will be stored in the Transpiler table
# Metadata details have to be included like version of the prompt, datetime, aggregate category - this will help us in the dedup steps.
# NEED MODEL DESIGN FOR THIS TABLE

data = json.loads(sql_select_statements)
df = spark.createDataFrame(data)
display(df)

df.write.mode("append").saveAsTable("fdscatalog.fds.aggregate_transpiler_script")


# COMMAND ----------

import json
import openai

def generate_sql_queries(input_data):
    """
    Generate SQL queries for each agg in the dictionary using OpenAI API's Chat Completion method.

    Args:
        input_data (dict): Dictionary with lists of agg.

    Returns:
        dict: Dictionary with the original input data and corresponding SQL SELECT statements.
    """
    # Define the few-shot examples in the chat format
    few_shot_examples = [
        {"role": "system", "content": "You are an AI trained to perform multiple tasks. You will receive a dictionary with agg. You need to:\n1. Generate an SQL query for each agg to retrieve the relevant row from the table."},
        {"role": "user", "content": "Input: {\"agg1\": ['DQbox1', 'DQbox2', 'DQbox3']}"},
        {"role": "assistant", "content": "SQL Queries:\nSELECT * FROM fdscatalog.fds.dq_mapping WHERE boxc in ('DQbox1', 'DQbox2', 'DQbox3');"},
        {"role": "user", "content": "Input: {\"agg\": ['ALbox1', 'ALbox2']}"},
        {"role": "assistant", "content": "SQL Queries:\nSELECT * FROM fdscatalog.fds.dq_mapping WHERE boxc in ('ALbox1', 'ALbox2');"}
    ]
    
    # Convert the input dictionary to a string
    input_str = str(input_data)
    
    # Define the user prompt with the current input data
    user_prompt = {"role": "user", "content": f"Input: {input_str}\n\nSQL Queries:\n"}
    
    # Combine the few-shot examples with the user prompt
    messages = few_shot_examples + [user_prompt]
    
    # Call the OpenAI API to generate the SQL queries
    response = client.chat.completions.create(
        messages=messages,
        model="gpt-4o",
        max_tokens=1000,
        temperature=0.5
    )
    
    # Extract the text from the response
    generated_text = response.choices[0].message.content.strip()

    # Split the generated text into individual queries
    queries = [query.strip() for query in generated_text.split("\n") if query.strip()]
    
    # Create a dictionary linking the original input data with the generated SQL queries
    result_with_queries = {key: {"values": value, "sql_query": queries[idx]} for idx, (key, value) in enumerate(input_data.items())}
    
    return generated_text

# Example usage
input_data = result_dict
result_with_queries = generate_sql_queries(input_data)

# Convert the result_with_queries dictionary to JSON
result_json = json.dumps(result_with_queries, indent=4)

# Display the JSON
display(result_json)

# COMMAND ----------

# MAGIC %pip freeze > requirements.txt
