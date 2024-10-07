
use postgres_native_tls::{MakeTlsConnector};
use tokio_postgres::{ tls::MakeTlsConnect, Client, Connection, Error, NoTls, Socket};
use crate::model::{DepthInterval,EarningInterval,Pool,RunePoolInterval,SwapsInterval};
use reqwest::Error as ReqwestError;
use tokio_postgres::Error as PostgresError;
use std::fmt;
use native_tls::{Certificate, TlsConnector};

#[derive(Debug)]
pub enum AppError {
    Reqwest(ReqwestError),
    Postgres(PostgresError),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            AppError::Postgres(e) => write!(f, "Postgres error: {}", e),
        }
    }
}

impl std::error::Error for AppError {}

impl From<ReqwestError> for AppError {
    fn from(error: ReqwestError) -> Self {
        AppError::Reqwest(error)
    }
}

impl From<PostgresError> for AppError {
    fn from(error: PostgresError) -> Self {
        AppError::Postgres(error)
    }
}
pub async fn establish_connection() -> Result<Client, Error> {
    dotenv::dotenv().ok();
    // Adjust the connection string here to remove sslmode=require
    
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let connector = TlsConnector::builder().build().unwrap();
    let connector = MakeTlsConnector::new(connector);
    // Make sure the connection string does not contain sslmode=require
    let (client, connection) = tokio_postgres::connect(&database_url, connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn fetch_depth_data(from:i32,count:i32) -> Vec<DepthInterval> {
    let url = format!(
        "https://midgard.ninerealms.com/v2/history/depths/BTC.BTC?interval=hour&count={}&from={}",count,from
    );
    println!("Fetching depth from URL: {}", url);
    let response: serde_json::Value = reqwest::get(&url)
        .await.unwrap()
        .json()
        .await.unwrap();
    let output_vec: Vec<DepthInterval> = serde_json::from_value(response["intervals"].to_owned()).unwrap();
    return output_vec;
}

pub async fn fetch_swaps_data(from: i32, count: i32) -> Vec<SwapsInterval> {
    let url = format!(
        "https://midgard.ninerealms.com/v2/history/swaps?interval=hour&count={}&from={}",
        count, from
    );
    println!("Fetching swaps from URL: {}", url);
    let response: serde_json::Value = reqwest::get(&url)
        .await.unwrap()
        .json()
        .await.unwrap();
    let output_vec: Vec<SwapsInterval> = serde_json::from_value(response["intervals"].to_owned()).unwrap();
    return output_vec;
}

pub async fn fetch_earnings_data(from: i32,count: i32) -> Vec<EarningInterval>{
    let url = format!(
        "https://midgard.ninerealms.com/v2/history/earnings?interval=hour&count={}&from={}",
        count,from
    );
    println!("Fetching earnings from URL: {}", url);
    let response: serde_json::Value = reqwest::get(&url)
        .await.unwrap()
        .json()
        .await.unwrap();
    let output_vec: Vec<EarningInterval> = serde_json::from_value(response["intervals"].to_owned()).unwrap();
    return output_vec;
}

pub async fn fetch_runepool_data(from: i32, count: i32) -> Vec<RunePoolInterval>{
    let url = format!(
        "https://midgard.ninerealms.com/v2/history/runepool?interval=hour&count={}&from={}",
        count, from
    );
    println!("Fetching rune from URL: {}", url);
    let response: serde_json::Value = reqwest::get(&url)
        .await.unwrap()
        .json()
        .await.unwrap();
    let output_vec: Vec<RunePoolInterval> = serde_json::from_value(response["intervals"].to_owned()).unwrap();
    return output_vec;
}

pub async fn insert_depth_interval(client: &Client, depth: &DepthInterval) -> Result<(), Error> {
    client.execute(
        "INSERT INTO depth_intervals (asset_depth, asset_price, asset_price_usd, end_time, liquidity_units, luvi, members_count, rune_depth, start_time, synth_supply, synth_units, units) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) 
        ON CONFLICT (end_time) DO NOTHING;",
        &[
            &depth.asset_depth,
            &depth.asset_price,
            &depth.asset_price_usd,
            &depth.end_time,
            &depth.liquidity_units,
            &depth.luvi,
            &depth.members_count,
            &depth.rune_depth,
            &depth.start_time,
            &depth.synth_supply,
            &depth.synth_units,
            &depth.units,
        ],
    ).await?;
    Ok(())
}

pub async fn insert_swaps_interval(client: &Client, swap: &SwapsInterval) -> Result<(), Error> {
    client.execute(
        "INSERT INTO swap_history_intervals (average_slip, end_time, from_trade_average_slip, from_trade_count, from_trade_fees, from_trade_volume, from_trade_volume_usd, rune_price_usd, start_time, synth_mint_average_slip, synth_mint_count, synth_mint_fees, synth_mint_volume, synth_mint_volume_usd, synth_redeem_average_slip, synth_redeem_count, synth_redeem_fees, synth_redeem_volume, synth_redeem_volume_usd, to_asset_average_slip, to_asset_count, to_asset_fees, to_asset_volume, to_asset_volume_usd, to_rune_average_slip, to_rune_count, to_rune_fees, to_rune_volume, to_rune_volume_usd, total_count, total_fees, total_volume, total_volume_usd) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33) 
        ON CONFLICT (end_time) DO NOTHING ;",
        &[
            &swap.average_slip,
            &swap.end_time,
            &swap.from_trade_average_slip,
            &swap.from_trade_count,
            &swap.from_trade_fees,
            &swap.from_trade_volume,
            &swap.from_trade_volume_usd,
            &swap.rune_price_usd,
            &swap.start_time,
            &swap.synth_mint_average_slip,
            &swap.synth_mint_count,
            &swap.synth_mint_fees,
            &swap.synth_mint_volume,
            &swap.synth_mint_volume_usd,
            &swap.synth_redeem_average_slip,
            &swap.synth_redeem_count,
            &swap.synth_redeem_fees,
            &swap.synth_redeem_volume,
            &swap.synth_redeem_volume_usd,
            &swap.to_asset_average_slip,
            &swap.to_asset_count,
            &swap.to_asset_fees,
            &swap.to_asset_volume,
            &swap.to_asset_volume_usd,
            &swap.to_rune_average_slip,
            &swap.to_rune_count,
            &swap.to_rune_fees,
            &swap.to_rune_volume,
            &swap.to_rune_volume_usd,
            &swap.total_count,
            &swap.total_fees,
            &swap.total_volume,
            &swap.total_volume_usd,
        ],
    ).await?;
    Ok(())
}

pub async fn insert_earning_interval(client: &Client, earning: &EarningInterval) -> Result<(), tokio_postgres::Error> {
    let row = client
        .query_opt(
            "INSERT INTO earning_intervals (avg_node_count, block_rewards, bonding_earnings, earnings, end_time, liquidity_earnings, liquidity_fees, rune_price_usd, start_time) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (end_time) DO NOTHING 
            RETURNING id",
            &[
                &earning.avg_node_count,
                &earning.block_rewards,
                &earning.bonding_earnings,
                &earning.earnings,
                &earning.end_time,
                &earning.liquidity_earnings,
                &earning.liquidity_fees,
                &earning.rune_price_usd,
                &earning.start_time,
            ],
        )
        .await?;

    // Check if a row was returned
    if let Some(row) = row {
        let interval_id: i32 = row.get(0); // Get the id of the inserted row

        // Insert pools associated with this earning interval
        for pool in &earning.pools {
            insert_pool(client, pool, interval_id).await?;
        }
    } else {
        // Handle the case where no row was inserted due to conflict
        println!("No new row inserted for end_time: {}", earning.end_time);
    }

    Ok(()) // Return Ok if everything was successful
}


pub async fn insert_pool(client: &Client, pool: &Pool ,interval_id: i32) -> Result<(), Error> {
    client.execute(
        "INSERT INTO pools (interval_id,asset_liquidity_fees, earnings, pool, rewards, rune_liquidity_fees, saver_earning, total_liquidity_fees_rune) 
        VALUES ($1, $2, $3, $4, $5, $6, $7,$8) 
        ",
        &[
            &interval_id,
            &pool.asset_liquidity_fees,
            &pool.earnings,
            &pool.pool,
            &pool.rewards,
            &pool.rune_liquidity_fees,
            &pool.saver_earning,
            &pool.total_liquidity_fees_rune,
        ],
    ).await?;
    Ok(())
}

pub async fn insert_runepool_interval(client: &Client, runepool: &RunePoolInterval) -> Result<(), Error> {
    client.execute(
        "INSERT INTO rune_pool_intervals (count, end_time, start_time, units) 
        VALUES ($1, $2, $3, $4) 
        ON CONFLICT (end_time) DO NOTHING ;",
        &[
            &runepool.count,
            &runepool.end_time,
            &runepool.start_time,
            &runepool.units,
        ],
    ).await?;
    Ok(())
}
