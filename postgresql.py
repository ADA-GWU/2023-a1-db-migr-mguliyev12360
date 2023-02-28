import psycopg2

######## Establish a connection to the database ########

# CREATE DATABASE SCHOOL;

# Connection constraints
HOST_ADDRESS = "" # ip address of the server
PORT_NUMBER = "5432"
DB_NAME = "school"
USER_NAME = "" # your username
PASSWORD = "" # your password

# Connect to your PostgreSQL database on a remote server
conn = psycopg2.connect(host=HOST_ADDRESS, port=PORT_NUMBER, database=DB_NAME, user=USER_NAME, password=PASSWORD)

# Open a cursor to perform database operations
cur = conn.cursor()

def display_tables():
    #display tables
    print("\tSTUDENTS TABLE:")
    cur.execute("SELECT * FROM students;")
    print("\t\t", cur.fetchall()) 

    print("\tINTERESTS TABLE:")
    cur.execute("SELECT * FROM interests;")
    print("\t\t", cur.fetchall(), '\n')


######## Part 1: Create & Populate ########

# create table STUDENTS
cur.execute("CREATE TABLE students (ST_ID INT NOT NULL PRIMARY KEY, ST_NAME VARCHAR(20), ST_LAST VARCHAR(20));")

# create table INTERESTS
cur.execute("CREATE TABLE interests (STUDENT_ID INT NOT NULL, INTEREST VARCHAR(15), FOREIGN KEY (STUDENT_ID) REFERENCES students(ST_ID));")

# add students
students = [
    (1, 'Konul', 'Qurbanova'), 
    (2, 'Shahnur', 'Isgandarli'), 
    (3, 'Natavan', 'Mammadova')
]

for s in students:
    cur.execute("INSERT INTO students (ST_ID, ST_NAME, ST_LAST) VALUES (%s, %s, %s);", s)

# add interests
interests = [
    (1, 'Tennis'),  (1, 'Literature'),  (1, 'Math'),  
    (2, 'Tennis'),  (3, 'Math'),        (3, 'Music'), 
    (2, 'Footbal'), (1, 'Chemistry'),   (3, 'Chess')
] 

for i in interests:
    cur.execute("INSERT INTO interests (STUDENT_ID, INTEREST) VALUES (%s, %s);", i)

# display 
print("TABLES BEFORE MIGRATION:")
display_tables()

######## Part 2: Migrate ########
def migrate():
    # Rename the STUDENTS.ST_ID to STUDENTS.STUDENT_ID
    cur.execute("ALTER TABLE students RENAME COLUMN st_id TO student_id;")

    # Change the length of STUDENTS.ST_NAME and STUDENTS.ST_LAST from 15 to 30
    cur.execute("ALTER TABLE students ALTER COLUMN st_name TYPE VARCHAR(30);")

    query = """
        CREATE TABLE INTEREST_TEMP AS (
            SELECT 
                STUDENT_ID,
                JSON_AGG(INTEREST) AS INTERESTS
            FROM INTERESTS
            GROUP BY STUDENT_ID
            ORDER BY STUDENT_ID
        )
    """
    cur.execute(query)
    #replace old table with new one
    cur.execute("DROP TABLE interests;")
    cur.execute("ALTER TABLE INTEREST_TEMP RENAME TO interests;")
    pass


def rollback():
    # rollback changes
    cur.execute("ALTER TABLE students RENAME COLUMN student_id TO st_id;")
    cur.execute("ALTER TABLE students ALTER COLUMN st_name TYPE VARCHAR(15);")
    
    
    cur.execute("SELECT * FROM interests;")
    
    new_table = []
    for row in cur.fetchall():
        for i in row[1]:
            new_table.append((row[0], i)) 
    
    cur.execute("DROP TABLE interests;")
    cur.execute("CREATE TABLE interests (STUDENT_ID INT NOT NULL, INTEREST VARCHAR(15), FOREIGN KEY (STUDENT_ID) REFERENCES students(ST_ID));")
    # add interests
    for i in new_table:
        cur.execute("INSERT INTO interests (STUDENT_ID, INTEREST) VALUES (%s, %s);", i)
        
    pass


print("TABLES AFTER MIGRATION:")
migrate()
display_tables()
print("TABLES AFTER ROLLBACK:")
rollback()
display_tables()

# drop all tables, demo only
cur.execute("DROP TABLE students, interests;")