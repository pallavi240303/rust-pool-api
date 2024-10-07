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
    let mut client = establish_connection().await?;

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

        // Fetch data from the database
        let depth_data = fetch_depth_data(from, count).await;
        let swaps_data = fetch_swaps_data(from, count).await;
        let earnings_data = fetch_earnings_data(from, count).await;
        let runepool_data = fetch_runepool_data(from, count).await;

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

            // Calculate the difference
            let time_difference = current_timestamp - last_end_time;
            println!("Time difference: {}", time_difference);

            // Check if the time difference is more than an hour
            if time_difference > 3600 {
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
            } else {
                println!("Last end_time is within the last hour. Sleeping...");
                let sleep_duration = (last_end_time - current_timestamp).max(3600) as u64;
                println!("Sleeping for {} seconds...", sleep_duration);
                drop(client);
                tokio::time::sleep(std::time::Duration::from_secs(sleep_duration)).await;
                client = establish_connection().await?;
                continue;
            }

            // Update 'from' with the last fetched end_time
            from = last_end_time;
            println!("Updated 'from' to: {}", from);
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


