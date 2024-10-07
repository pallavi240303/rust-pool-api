use axum::{extract::Query, Json};
use serde::Deserialize;
use serde_json::json;
use crate::{db::establish_connection, model::{EarningInterval, Pool, RunePoolInterval, SwapsInterval}}; 
use crate::model::DepthInterval; 
#[derive(Deserialize)]
pub struct QueryParams {
    page: Option<u32>,
    limit: Option<u32>,
    start_time: Option<String>,
    end_time: Option<String>,
    sort_by: Option<String>,      
    order: Option<String>, 
    pool: Option<String>,      
    interval: Option<String> 
}

pub async fn get_depth_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match establish_connection().await {
        Ok(client) => {
            let mut query = String::new();

            // Handle interval logic
            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('day', to_timestamp(end_time::int))) * FROM depth_intervals");
                    }
                    "week" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('week', to_timestamp(end_time::int))) * FROM depth_intervals");
                    }
                    "month" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('month', to_timestamp(end_time::int))) * FROM depth_intervals");
                    }
                    "year" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('year', to_timestamp(end_time::int))) * FROM depth_intervals");
                    }
                    _ => {
                        query.push_str("SELECT * FROM depth_intervals");
                    }
                }
            } else {
                query.push_str("SELECT * FROM depth_intervals"); 
            }

            let mut filters = Vec::new();

            // Handle filters
            if let Some(start_time) = &params.start_time {
                filters.push(format!("start_time >= '{}'", start_time)); 
            }
            if let Some(end_time) = &params.end_time {
                filters.push(format!("end_time <= '{}'", end_time)); 
            }

            // Add filters to query
            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            let sort_by = params.sort_by.as_deref().unwrap_or("end_time"); 
            let order = params.order.as_deref().unwrap_or("DESC");

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str(&format!(" ORDER BY date_trunc('day', to_timestamp(end_time::int)), {} {}", sort_by, order));
                    }
                    "week" => {
                        query.push_str(&format!(" ORDER BY date_trunc('week', to_timestamp(end_time::int)), {} {}", sort_by, order));
                    }
                    "month" => {
                        query.push_str(&format!(" ORDER BY date_trunc('month', to_timestamp(end_time::int)), {} {}", sort_by, order));
                    }
                    "year" => {
                        query.push_str(&format!(" ORDER BY date_trunc('year', to_timestamp(end_time::int)), {} {}", sort_by, order));
                    }
                    _ => {
                        query.push_str(&format!(" ORDER BY {} {}", sort_by, order));
                    }
                }
            } else {
                query.push_str(&format!(" ORDER BY {} {}", sort_by, order));
            }

            let limit = params.limit.unwrap_or(400); 
            let page = params.page.unwrap_or(1); 
            let offset = (page - 1) * limit; 
            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            println!("Generated SQL Query: {}", query);

            let rows = client.query(&query, &[]).await.unwrap();

            let intervals: Vec<DepthInterval> = rows.iter().map(|row| {
                DepthInterval {
                    asset_depth: row.get("asset_depth"),
                    asset_price: row.get("asset_price"),
                    asset_price_usd: row.get("asset_price_usd"),
                    end_time: row.get("end_time"),
                    liquidity_units: row.get("liquidity_units"),
                    luvi: row.get("luvi"),
                    members_count: row.get("members_count"),
                    rune_depth: row.get("rune_depth"),
                    start_time: row.get("start_time"),
                    synth_supply: row.get("synth_supply"),
                    synth_units: row.get("synth_units"),
                    units: row.get("units"),
                }
            }).collect();

            Json(json!({ "data": intervals }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}



pub async fn get_swaps_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match establish_connection().await {
        Ok(client) => {
            let mut query = String::new();

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('day', to_timestamp(end_time::int))) * FROM swap_history_intervals");
                    }
                    "week" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('week', to_timestamp(end_time::int))) * FROM swap_history_intervals");
                    }
                    "month" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('month', to_timestamp(end_time::int))) * FROM swap_history_intervals");
                    }
                    "year" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('year', to_timestamp(end_time::int))) * FROM swap_history_intervals");
                    }
                    _ => {
                        query.push_str("SELECT * FROM swap_history_intervals"); 
                    }
                }
            } else {
                query.push_str("SELECT * FROM swap_history_intervals"); 
            }

            let mut filters = Vec::new();

            if let Some(start_time) = &params.start_time {
                filters.push(format!("start_time >= '{}'", start_time)); 
            }
            if let Some(end_time) = &params.end_time {
                filters.push(format!("end_time <= '{}'", end_time)); 
            }

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            let sort_column = params.sort_by.as_deref().unwrap_or("end_time"); 
            let order = params.order.as_deref().unwrap_or("DESC");

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('day', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "week" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('week', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "month" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('month', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "year" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('year', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    _ => {
                        query.push_str(&format!(" ORDER BY {} {}", sort_column, order));
                    }
                }
            } else {
                query.push_str(&format!(" ORDER BY {} {}", sort_column, order));
            }

            let limit = params.limit.unwrap_or(400);
            let page = params.page.unwrap_or(1);
            let offset = (page - 1) * limit;
            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            println!("Generated SQL Query: {}", query);

            let rows = client.query(&query, &[]).await.unwrap();

            let intervals: Vec<SwapsInterval> = rows.iter().map(|row| {
                SwapsInterval {
                    average_slip: row.get("average_slip"),
                    end_time: row.get("end_time"),
                    from_trade_average_slip: row.get("from_trade_average_slip"),
                    from_trade_count: row.get("from_trade_count"),
                    from_trade_fees: row.get("from_trade_fees"),
                    from_trade_volume: row.get("from_trade_volume"),
                    from_trade_volume_usd: row.get("from_trade_volume_usd"),
                    rune_price_usd: row.get("rune_price_usd"),
                    start_time: row.get("start_time"),
                    synth_mint_average_slip: row.get("synth_mint_average_slip"),
                    synth_mint_count: row.get("synth_mint_count"),
                    synth_mint_fees: row.get("synth_mint_fees"),
                    synth_mint_volume: row.get("synth_mint_volume"),
                    synth_mint_volume_usd: row.get("synth_mint_volume_usd"),
                    synth_redeem_average_slip: row.get("synth_redeem_average_slip"),
                    synth_redeem_count: row.get("synth_redeem_count"),
                    synth_redeem_fees: row.get("synth_redeem_fees"),
                    synth_redeem_volume: row.get("synth_redeem_volume"),
                    synth_redeem_volume_usd: row.get("synth_redeem_volume_usd"),
                    to_asset_average_slip: row.get("to_asset_average_slip"),
                    to_asset_count: row.get("to_asset_count"),
                    to_asset_fees: row.get("to_asset_fees"),
                    to_asset_volume: row.get("to_asset_volume"),
                    to_asset_volume_usd: row.get("to_asset_volume_usd"),
                    to_rune_average_slip: row.get("to_rune_average_slip"),
                    to_rune_count: row.get("to_rune_count"),
                    to_rune_fees: row.get("to_rune_fees"),
                    to_rune_volume: row.get("to_rune_volume"),
                    to_rune_volume_usd: row.get("to_rune_volume_usd"),
                    total_count: row.get("total_count"),
                    total_fees: row.get("total_fees"),
                    total_volume: row.get("total_volume"),
                    total_volume_usd: row.get("total_volume_usd"),
                }
            }).collect();

            Json(json!({ "data": intervals }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}




pub async fn get_rune_pool_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match establish_connection().await {
        Ok(client) => {
            let mut query = String::new();

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('day', to_timestamp(end_time::int))) * FROM rune_pool_intervals");
                    }
                    "week" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('week', to_timestamp(end_time::int))) * FROM rune_pool_intervals");
                    }
                    "month" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('month', to_timestamp(end_time::int))) * FROM rune_pool_intervals");
                    }
                    "year" => {
                        query.push_str("SELECT DISTINCT ON (date_trunc('year', to_timestamp(end_time::int))) * FROM rune_pool_intervals");
                    }
                    _ => {
                        query.push_str("SELECT * FROM rune_pool_intervals"); 
                    }
                }
            } else {
                query.push_str("SELECT * FROM rune_pool_intervals"); 
            }

            let mut filters = Vec::new();

            if let Some(start_time) = &params.start_time {
                filters.push(format!("start_time >= '{}'", start_time)); 
            }
            if let Some(end_time) = &params.end_time {
                filters.push(format!("end_time <= '{}'", end_time));
            }

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            let sort_column = params.sort_by.as_deref().unwrap_or("end_time"); 
            let order = params.order.as_deref().unwrap_or("DESC"); 

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('day', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "week" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('week', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "month" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('month', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    "year" => {
                        query.push_str(&format!(
                            " ORDER BY date_trunc('year', to_timestamp(end_time::int)), {} {}",
                            sort_column, order
                        ));
                    }
                    _ => {
                        query.push_str(&format!(" ORDER BY {} {}", sort_column, order));
                    }
                }
            } else {
                query.push_str(&format!(" ORDER BY {} {}", sort_column, order));
            }

            let limit = params.limit.unwrap_or(400); 
            let page = params.page.unwrap_or(1); 
            let offset = (page - 1) * limit; 
            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            println!("Generated SQL Query: {}", query);

            let rows = client.query(&query, &[]).await.unwrap();

            let intervals: Vec<RunePoolInterval> = rows.iter().map(|row| {
                RunePoolInterval {
                    count: row.get("count"),
                    end_time: row.get("end_time"),
                    start_time: row.get("start_time"),
                    units: row.get("units"),
                }
            }).collect();

            Json(json!({ "data": intervals }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}



pub async fn get_earning_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match establish_connection().await {
        Ok(client) => {
            let mut query = String::from("SELECT ei.avg_node_count, ei.block_rewards, ei.bonding_earnings, ei.earnings, ei.end_time, \
                                          ei.liquidity_earnings, ei.liquidity_fees, ei.rune_price_usd, ei.start_time, p.pool, \
                                          p.asset_liquidity_fees, p.earnings, p.rewards, p.rune_liquidity_fees, p.saver_earning, \
                                          p.total_liquidity_fees_rune \
                                          FROM earning_intervals ei \
                                          LEFT JOIN pools p ON ei.id = p.interval_id");

            let mut filters = Vec::new();

            if let Some(interval) = &params.interval {
                match interval.as_str() {
                    "day" => {
                        query.push_str(" AND date_trunc('day', to_timestamp(ei.end_time::int)) = date_trunc('day', to_timestamp(ei.start_time::int))");
                    }
                    "week" => {
                        query.push_str(" AND date_trunc('week', to_timestamp(ei.end_time::int)) = date_trunc('week', to_timestamp(ei.start_time::int))");
                    }
                    "month" => {
                        query.push_str(" AND date_trunc('month', to_timestamp(ei.end_time::int)) = date_trunc('month', to_timestamp(ei.start_time::int))");
                    }
                    "year" => {
                        query.push_str(" AND date_trunc('year', to_timestamp(ei.end_time::int)) = date_trunc('year', to_timestamp(ei.start_time::int))");
                    }
                    _ => {
                        
                    }
                }
            }

            if let Some(start_time) = &params.start_time {
                filters.push(format!("ei.start_time >= '{}'", start_time));
            }
            if let Some(end_time) = &params.end_time {
                filters.push(format!("ei.end_time <= '{}'", end_time));
            }

            if let Some(pool) = &params.pool {
                filters.push(format!("p.pool = '{}'", pool));
            }

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            if let Some(sort_by) = &params.sort_by {
                let order = params.order.as_deref().unwrap_or("DESC"); 
            
                let valid_sort_by = match sort_by.as_str() {
                    "start_time" => "ei.start_time",
                    "end_time" => "ei.end_time",
                    "earnings" => "ei.earnings",
                    "rune_price_usd" => "ei.rune_price_usd",
                    _ => "ei.end_time", // Default sort by column
                };
            
                query.push_str(&format!(" ORDER BY {} {}", valid_sort_by, order));
            } else {
                query.push_str(" ORDER BY ei.end_time DESC");
            }

            let limit = params.limit.unwrap_or(27);
            let page = params.page.unwrap_or(1);
            let offset = (page - 1) * limit;
            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
            println!("{:?}",query);

            let rows = client.query(&query, &[]).await.unwrap();

            let mut earnings: Vec<EarningInterval> = vec![];
            for row in rows.iter() {
                let pool = Pool {
                    asset_liquidity_fees: row.get("asset_liquidity_fees"),
                    earnings: row.get("earnings"),
                    pool: row.get("pool"),
                    rewards: row.get("rewards"),
                    rune_liquidity_fees: row.get("rune_liquidity_fees"),
                    saver_earning: row.get("saver_earning"),
                    total_liquidity_fees_rune: row.get("total_liquidity_fees_rune"),
                };

                let existing = earnings.iter_mut().find(|e| e.start_time == row.get::<&str, String>("start_time"));
                if let Some(earning) = existing {
                    earning.pools.push(pool);
                } else {
                    earnings.push(EarningInterval {
                        avg_node_count: row.get("avg_node_count"),
                        block_rewards: row.get("block_rewards"),
                        bonding_earnings: row.get("bonding_earnings"),
                        earnings: row.get("earnings"),
                        end_time: row.get("end_time"),
                        liquidity_earnings: row.get("liquidity_earnings"),
                        liquidity_fees: row.get("liquidity_fees"),
                        rune_price_usd: row.get("rune_price_usd"),
                        start_time: row.get("start_time"),
                        pools: vec![pool],
                    });
                }
            }

            Json(json!({ "data": earnings }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}
