"""Query the database"""

import sqlite3

# Define a global variable for the log file
LOG_FILE = "query_log.md"


def log_query(query):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")


def general_query(query):
    """General query cursor"""
    conn = sqlite3.connect("urbanizationDB.db")
    # Create a cursor
    cursor = conn.cursor()
    # Execute the query
    cursor.execute(query)
    # If the query modifies the database, commit the changes
    if (
        query.strip().lower().startswith("insert")
        or query.strip().lower().startswith("update")
        or query.strip().lower().startswith("delete")
    ):
        conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    log_query(f"{query}")


def create_rec(
    statefips,
    state,
    gisjoin,
    lat_tract,
    long_tract,
    population,
    adj_radiuspop_5,
    urbanindex,
):
    """create example query"""
    conn = sqlite3.connect("urbanizationDB.db")
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO urbanizationDB
        (statefips, state, gisjoin, lat_tract, long_tract, population, adj_radiuspop_5, urbanindex) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            statefips,
            state,
            gisjoin,
            lat_tract,
            long_tract,
            population,
            adj_radiuspop_5,
            urbanindex,
        ),
    )
    conn.commit()
    conn.close()


def update_rec(
    statefips,
    state,
    gisjoin,
    lat_tract,
    long_tract,
    population,
    adj_radiuspop_5,
    urbanindex,
):
    """Updates the urbanization table based on state and modifies urbanindex for Alabama"""

    # Connect to the SQLite database
    conn = sqlite3.connect("urbanizationDB.db")
    c = conn.cursor()

    # Update the database table based on conditions
    c.execute(
        """
        UPDATE urbanizationDB
        SET state = ?, lat_tract = ?, long_tract = ?, population = ?, adj_radiuspop_5 = ?, 
            urbanindex = CASE 
                            WHEN state = 'Alabama' THEN ?
                            ELSE urbanindex
                        END
        WHERE gisjoin = ?
        """,
        (
            state,
            lat_tract,
            long_tract,
            population,
            adj_radiuspop_5,
            urbanindex * 0.5,
            gisjoin,
        ),
    )

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    # Log the query
    log_query(
        f"""
    UPDATE urbanizationDB SET 
        state='{state}', 
        lat_tract={lat_tract}, 
        long_tract={long_tract}, 
        population={population}, 
        adj_radiuspop_5={adj_radiuspop_5}, 
        urbanindex=CASE WHEN state='Alabama' THEN {urbanindex * 0.5} ELSE urbanindex END 
    WHERE gisjoin='{gisjoin}';
    """
    )


def delete_rec(gisjoin):
    """delete query"""
    conn = sqlite3.connect("urbanizationDB.db")
    c = conn.cursor()
    c.execute("DELETE FROM urbanizationDB WHERE gisjoin=?", (gisjoin,))
    conn.commit()
    conn.close()

    # Log the query
    log_query(f"DELETE FROM urbanizationDB WHERE id={gisjoin};")


def read_data():
    """Read all data from urbanizationDB"""
    conn = sqlite3.connect("urbanizationDB.db")
    c = conn.cursor()
    c.execute("SELECT * FROM urbanizationDB")  # Corrected table name
    data = c.fetchall()
    log_query("SELECT * FROM urbanizationDB;")
    conn.close()
    return data
