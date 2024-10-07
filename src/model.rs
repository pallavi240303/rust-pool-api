use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize ,FromRow)]
#[serde(rename_all = "camelCase")]
pub struct DepthInterval {
    pub asset_depth: String,
    pub asset_price: String,
    #[serde(rename = "assetPriceUSD")]
    pub asset_price_usd: String,
    pub end_time: String,
    pub liquidity_units: String,
    pub luvi: String,
    pub members_count: String,
    pub rune_depth: String,
    pub start_time: String,
    pub synth_supply: String,
    pub synth_units: String,
    pub units: String,
}

#[derive(Debug, Serialize, Deserialize,FromRow)]
#[serde(rename_all = "camelCase")]
pub struct SwapsInterval {
    pub average_slip: String,
    pub end_time: String,
    pub from_trade_average_slip: String,
    pub from_trade_count: String,
    pub from_trade_fees: String,
    pub from_trade_volume: String,
    #[serde(rename = "fromTradeVolumeUSD")]
    pub from_trade_volume_usd: String,
    #[serde(rename = "runePriceUSD")]
    pub rune_price_usd: String,
    pub start_time: String,
    pub synth_mint_average_slip: String,
    pub synth_mint_count: String,
    pub synth_mint_fees: String,
    pub synth_mint_volume: String,
    #[serde(rename = "synthMintVolumeUSD")]
    pub synth_mint_volume_usd: String,
    pub synth_redeem_average_slip: String,
    pub synth_redeem_count: String,
    pub synth_redeem_fees: String,
    pub synth_redeem_volume: String,
    #[serde(rename = "synthRedeemVolumeUSD")]
    pub synth_redeem_volume_usd: String,
    pub to_asset_average_slip: String,
    pub to_asset_count: String,
    pub to_asset_fees: String,
    pub to_asset_volume: String,
    #[serde(rename = "toAssetVolumeUSD")]
    pub to_asset_volume_usd: String,
    pub to_rune_average_slip: String,
    pub to_rune_count: String,
    pub to_rune_fees: String,
    pub to_rune_volume: String,
    #[serde(rename = "toRuneVolumeUSD")]
    pub to_rune_volume_usd: String,
    pub total_count: String,
    pub total_fees: String,
    pub total_volume: String,
    #[serde(rename = "totalVolumeUSD")]
    pub total_volume_usd: String,
}

#[derive(Debug, Serialize, Deserialize,FromRow)]
#[serde(rename_all = "camelCase")]
pub struct EarningInterval {
    pub avg_node_count: String,
    pub block_rewards: String,
    pub bonding_earnings: String,
    pub earnings: String,
    pub end_time: String,
    pub liquidity_earnings: String,
    pub liquidity_fees: String,
    #[serde(rename = "runePriceUSD")]
    pub rune_price_usd: String,
    pub start_time: String,
    pub pools: Vec<Pool>  // Nested pools array
}

#[derive(Debug, Serialize, Deserialize,FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    pub asset_liquidity_fees: Option<String>,
    pub earnings: Option<String>,
    pub pool: Option<String>,
    pub rewards: Option<String>,
    pub rune_liquidity_fees: Option<String>,
    pub saver_earning: Option<String>,
    pub total_liquidity_fees_rune: Option<String>,
}
#[derive(Debug, Serialize, Deserialize,FromRow)]
#[serde(rename_all = "camelCase")]
pub struct RunePoolInterval {
    pub count: String,
    pub end_time: String,
    pub start_time: String,
    pub units: String,
}
