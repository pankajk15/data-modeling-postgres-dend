# SPARKIFY DB Overview: 		<img src="images/postgresql_logo.png" width="100" height="80" ALIGN="right">

## Introduction: 
	The purpose of the sparkify database is to enable the analytics team in Sparkify company to analyse the songs 
	that its users are listening to using its streaming application. Database contains a number of fact and dimension
	tables which has data on the songs users are listening to, information about those users (e.g. are they paid or 
	free users?), and about the songs and artists that they have listened to in the app. Sample Queries section will
	give you more insights and help you understand the expected and intended use of the database.
 
## Database: sparkifydb
 
    - This project creates tables in database called 'sparkifydb'
    - The tables within the database follows 'star schema' to relate fact and dimension tables
    - Data source for this is json files named: 'song data' and 'log_data'

## Tables & Schemas

    	- Fact Table: songplays
		..* Table has data from the log_data file with the artist_id and song_id
			columns from the songs and artists tables
    	- Dimension Table: users
		..* Has data from the log_data JSON files
	- Dimension Table: songs 
		..* Has data from the song_data JSON files
	- Dimension Table: Artists
		..* Has data from the song_data JSON files
	- Dimension Table: time
		..* Has data from the log_data JSON files
        * The table stores the timestamp of each song play and further detailed down into 
		individual units of time in columns
  
## Operation Instructions

	 Connecting to the Database
		*   Database connections can be made using a connection object
			conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user= password=")

	 Creating & Populating Tables
		* 	To populate the tables, you must first execute the create_tables.py file at the terminal
		
		Command:
			python create_tables.py
	   
		* Next, you should run the etl.py file to populate the tables with data
		Command:
			python etl.py
		
### Sample Queries

		* Step 1: create a 'cursor' object 
		* Step 2: Place the SQl queries within parentheses, and execute using the cursor object
		* Step 3: Commit the queries this actually run the query in database
		* Step 4: Close the database connection once you have finished executing queries

		`Example:
			cur = conn.cursor()
			cur.execute("""SELECT * FROM sparkifydb.songs;""")
			conn.commit()
			conn.close()`
		
		** Example: View Male paid users
			
			%sql SELECT * FROM songplays INNER JOIN users ON songplays.user_id = users.user_id WHERE users.level = 'paid' AND users.gender = 'M' LIMIT 5;
		Result:
			Songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent	user_id_1	first_name	last_name	gender	level_1
			1	2018-11-29 00:00:57.796000	73	paid	None	None	954	Tampa-St. Petersburg-Clearwater, FL	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"	73	Jacob	Klein	M	paid

    
    



