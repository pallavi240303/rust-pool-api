use db::{establish_connection, fetch_depth_data, fetch_earnings_data, fetch_runepool_data, fetch_swaps_data, insert_depth_interval, insert_earning_interval, insert_runepool_interval, insert_swaps_interval};
use server::start_server;
use tokio_postgres::Client;
use chrono::Utc;
mod server;
mod api;
mod model;
mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let client = establish_connection().await?;
    
    // Run the server concurrently
    let _server_task = tokio::spawn(async {
        start_server().await;
    });

    let mut from = fetch_last_end_time(&client).await.unwrap_or_else(|e| {
        println!("Failed to fetch last end_time from the database: {}", e);
        (Utc::now().timestamp() - 3600) as i32 
    });

    let count = 400;
    println!("Fetched end_time is: {}", from);

    loop {
        println!("Fetching row with end_time: {}", from);

        let depth_data = fetch_depth_data(from, count).await;
        let swaps_data = fetch_swaps_data(from, count).await;
        let earnings_data = fetch_earnings_data(from, count).await;
        let runepool_data = fetch_runepool_data(from, count).await;

        // Insert data into the database
        for earning in &earnings_data {
            insert_earning_interval(&client, &earning).await?;
        }
        println!("Earnings intervals inserted successfully!");

        for runepool in &runepool_data {
            insert_runepool_interval(&client, &runepool).await?;
        }
        println!("Rune pool intervals inserted successfully!");

        for depth in &depth_data {
            insert_depth_interval(&client, &depth).await?;
        }
        println!("Depth intervals inserted successfully!");

        for swap in &swaps_data {
            insert_swaps_interval(&client, &swap).await?;
        }
        println!("Swap intervals inserted successfully!");

        if let Some(last_interval) = depth_data.last() {
            let last_end_time = match last_interval.end_time.parse::<i32>() {
                Ok(time) => time,
                Err(e) => {
                    println!("Failed to parse last_end_time: {}", e);
                    continue;
                }
            };

            let current_timestamp = Utc::now().timestamp() as i32;
            println!("Last fetched end_time: {}", last_end_time);
            println!("Current timestamp: {}", current_timestamp);

            // Determine how long to sleep: wait for the remainder of the time until the next fetch
            let sleep_duration = if last_end_time > current_timestamp {
                // If the last end_time is in the future, wait until it passes
                (last_end_time - current_timestamp) as u64
            } else {
                // Otherwise, sleep for 1 hour (3600 seconds)
                3600
            };

            // Update 'from' with the last fetched end_time if it's in the past
            if last_end_time <= current_timestamp {
                from = last_end_time;
                println!("Updated 'from' to: {}", from);
            }

            // Sleep for the calculated duration
            println!("Sleeping for {} seconds...", sleep_duration);
            tokio::time::sleep(std::time::Duration::from_secs(sleep_duration)).await;
        } else {
            println!("No data fetched. Stopping.");
            break;
        }
    }

    Ok(())
}


pub async fn fetch_last_end_time(client: &Client) -> Result<i32, tokio_postgres::Error> {
    
    let row = client
        .query_opt("SELECT start_time FROM depth_intervals ORDER BY end_time DESC LIMIT 1", &[])
        .await?;

    println!("Row fetched: {:?}", row);

    match row {
        Some(r) => {
            let end_time_str: String = r.get(0);
            
            match end_time_str.parse::<i32>() {
                Ok(end_time) => {
                    println!("Last end_time fetched and parsed: {}", end_time);
                    Ok(end_time)
                }
                Err(e) => {
                    println!("Failed to parse end_time: {}, using default timestamp.", e);
                    
                    let fallback_time = (Utc::now().timestamp() - 3600) as i32;
                    Ok(fallback_time)
                }
            }
        }
        None => {
            
            println!("No rows found in depth_intervals, using default timestamp.");
            let fallback_time = (Utc::now().timestamp() - 3600) as i32;
            Ok(fallback_time)
        }
    }
}


