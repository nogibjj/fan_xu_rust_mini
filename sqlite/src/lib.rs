use csv::ReaderBuilder; //for loading from csv
use rusqlite::{params, Connection, Result}; 
use std::error::Error;
use std::fs::File; //for loading csv //for capturing errors from loading
                                     // Here we will have a function for each of the commands

// Create a table
pub fn create_table(conn: &Connection, table_name: &str) -> Result<()> {
    let create_query = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            player TEXT PRIMARY KEY,
            position TEXT NOT NULL,
            id TEXT NOT NULL,
            draft_year INTEGER NOT NULL,
            projected_spm REAL NOT NULL,
            superstar REAL NOT NULL,
            starter REAL NOT NULL,
            role_player REAL NOT NULL,
            bust REAL NOT NULL
        )",
        table_name
    );
    conn.execute(&create_query, [])?;
    println!("Table '{}' created successfully.", table_name);
    Ok(()) //returns nothing except an error if it occurs
}

//Read
pub fn query_exec(conn: &Connection, query_string: &str) -> Result<()> {
    // Prepare the query and iterate over the rows returned
    let mut stmt = conn.prepare(query_string)?;

    // Use query_map to handle multiple rows
    let rows = stmt.query_map([], |row| {
        // Assuming the `users` table has an `id` and `name` column
        let player: String = row.get(0)?;
        let position: String = row.get(1)?;
        let id: String = row.get(2)?;
        let draft_year: i64 = row.get(3)?;
        let projected_spm: f64 = row.get(4)?;
        let superstar: f64 = row.get(5)?;
        let starter: f64 = row.get(6)?;
        let role_player: f64 = row.get(7)?;
        let bust: f64 = row.get(8)?;
        Ok((player, position, id, draft_year, projected_spm, superstar, starter, role_player, bust))
    })?;

    // Iterate over the rows and print the results
    for row in rows {
        let (player, position, id, draft_year, projected_spm, superstar, starter, role_player, bust) = row?;
        println!(
            "Player: {}, Position: {}, ID: {}, Draft Year: {}, Projected SPM: {}, Superstar {}, Starter: {}, Role Player {}, Bust: {}", 
            player, position, id, draft_year, projected_spm, superstar, starter, role_player, bust
        );
    }

    Ok(())
}

//delete
pub fn drop_table(conn: &Connection, table_name: &str) -> Result<()> {
    let drop_query = format!("DROP TABLE IF EXISTS {}", table_name);
    conn.execute(&drop_query, [])?;
    println!("Table '{}' dropped successfully.", table_name);
    Ok(())
}

//load data from a file path to a table
pub fn load_data_from_csv(
    conn: &Connection,
    table_name: &str,
    file_path: &str,
) -> Result<(), Box<dyn Error>> { //Box<dyn Error> is a trait object that can represent any error type
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().from_reader(file);

    let insert_query = format!(
        "INSERT INTO {} (player, position, id, draft_year, projected_spm, superstar, starter, role_player, bust) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        table_name
    );
    //this is a loop that expects a specific schema, you will need to change this if you have a different schema
    for result in rdr.records() {
        let record = result?;
        let player: &str = &record[0];
        let position: &str = &record[1];
        let id: &str = &record[2];
        let draft_year: i64 = record[3].parse()?;
        let projected_spm: f64 = record[4].parse()?;
        let superstar: f64 = record[5].parse()?;
        let starter: f64 = record[6].parse()?;
        let role_player: f64 = record[7].parse()?;
        let bust: f64 = record[8].parse()?;

        conn.execute(&insert_query, params![player, position, id, draft_year, projected_spm, superstar, starter, role_player, bust])?;
    }

    println!(
        "Data loaded successfully from '{}' into table '{}'.",
        file_path, table_name
    );
    Ok(())
}