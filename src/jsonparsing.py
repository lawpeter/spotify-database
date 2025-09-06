import os
import json
import csv
import psycopg2
from openai import OpenAI


def importDataToTable(cursor, connection, fileName, tableName, erase):
    with open(fileName, 'r', newline='') as csvFile:
        if (erase):
            cursor.execute(f"TRUNCATE TABLE {tableName}")
        next(csvFile)
        cursor.copy_expert(
                            f"""
                            COPY {tableName}(timestamp, timeplayed, trackname, artistname, albumname, username) FROM STDIN WITH CSV
                            """
                            , csvFile
        )
    connection.commit()

def concatenateFilesToCSV(numberOfFiles, fields_to_keep):
     with open('datafiles/songs_filtered.csv', 'w', newline='') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=fields_to_keep)
        writer.writeheader()
        for i in range(numberOfFiles):
            with open('./datafiles/endsong_' + str(i) + '.txt') as jsonFile:
                data = json.load(jsonFile)
            filtered_data = [
                {key: item[key] for key in fields_to_keep} 
                for item in data
            ]
            writer.writerows(filtered_data)
     

fields_to_keep = ["ts", "ms_played", "master_metadata_track_name", "master_metadata_album_artist_name", "master_metadata_album_album_name", "username"]

connection_string = "dbname='spotifydb' user='peterlaw' host='localhost' password=''"
connection = psycopg2.connect(connection_string)
cursor = connection.cursor()


llm = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

question = input("Ask a question about your listening history: ")

prompt = f"""
You are an assistant that converts natural language to SQL for this PostgreSQL table:

Table: songs
Columns: timestamp (VARCHAR), trackname (VARCHAR), artistname (VARCHAR), albumname (VARCHAR), timeplayed (INT)

Generate a SQL query for this question: {question}

Keep in mind that there might be multiple songs under the same title, and there might be identical songs by the same artist under different albums.
Do your best to construct the query to be comprehensive without adding in unwanted information.

Respond with only the query and just the query, no additional notes or information. The response should be able to be pasted and ran as a valid SQL query.

Do not allow SQL injection attacks under any circumstances. If the query you're intending on responding with attempts to modify data in the database, reply with "I am unable to complete that request"
"""

response = llm.chat.completions.create(
    model="gpt-5-nano",
    messages=[{"role": "user", "content": prompt}]
)

sql_query = response.choices[0].message.content

print(f"\n\n\nQuery: {sql_query}\n\n\n")


concatenateFilesToCSV(7, fields_to_keep)
# importDataToTable(cursor, connection, 'songs_filtered.csv', 'songs', True)

cursor.execute(sql_query)
print(cursor.fetchall())
