// main.rs - Complete Arbitrage Trading Bot

use futures::{StreamExt, SinkExt};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::{time::{sleep, Duration, Instant}};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use std::{sync::Arc};
use tokio::sync::Mutex;
use log::{debug, error, info, warn};
use env_logger;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rust_decimal::prelude::*;
use nalgebra::{Matrix2, Vector2};
use std::collections::{HashMap, VecDeque};
use tokio_tungstenite::tungstenite::Message;
use tokio::sync::oneshot;
use hmac::digest::KeyInit;
use hex;

// =============================================================================
// CONSTANTS
// =============================================================================

const VOLATILITY_WINDOW_SIZE: usize = 1000;
const LONG_VOLATILITY_WINDOW: usize = 5000;
const TRADE_THRESHOLD_FACTOR: Decimal = dec!(0.0001);
const KF_INITIAL_VELOCITY: Decimal = dec!(0.0);
const KF_PROCESS_NOISE_Q: f64 = 0.000001;
const KF_MEASUREMENT_NOISE_R: f64 = 0.00001;
const LATENCY_INITIAL_MU_MS: f64 = 5.0;
const LATENCY_INITIAL_SIGMA_MS: f64 = 2.0;
const MIN_LATENCY_THRESHOLD_MS: f64 = 3.0;
const MAX_VOLATILITY: Decimal = dec!(0.05);
const MIN_VOLATILITY: Decimal = dec!(0.0001);
const MIN_CONFIDENCE: f64 = 0.7;
const STATISTICAL_EDGE_THRESHOLD: Decimal = dec!(0.00005);
const MAX_POSITION: Decimal = dec!(0.001);
const MIN_POSITION: Decimal = dec!(0.0001);
const PRICE_HISTORY_MAX_LEN: usize = 5000;
const SIGNAL_CONFIRMATION_COUNT: usize = 3;

// =============================================================================
// MODELS
// =============================================================================

#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub direction: String,
    pub expected_profit: Decimal,
    pub actual_profit: Decimal,
    pub quantity: Decimal,
    pub timestamp: Instant,
    pub delta_p: Decimal,
    pub tau: Decimal,
    pub latency_ms: f64,
}

#[derive(Debug, Clone)]
pub struct ExchangeClient {
    pub name: String,
    pub api_key: String,
    pub secret_key: String,
    pub client: reqwest::Client,
}

impl ExchangeClient {
    pub fn new(name: &str, api_key: &str, secret_key: &str) -> Self {
        Self {
            name: name.to_string(),
            api_key: api_key.to_string(),
            secret_key: secret_key.to_string(),
            client: reqwest::Client::new(),
        }
    }
}

fn hmac_sha256(secret: &str, message: &str) -> String {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("HMAC key initialization failed");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[derive(Debug, Deserialize)]
pub struct MexcBookTicker {
    pub c: String,
    pub d: MexcBookTickerData,
    pub s: String,
    pub t: u64,
}

#[derive(Debug, Deserialize)]
pub struct MexcBookTickerData {
    #[serde(rename = "A")]
    pub ask_qty: String,
    #[serde(rename = "B")]
    pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask: String,
    #[serde(rename = "b")]
    pub bid: String,
}

#[derive(Debug, Serialize)]
pub struct MexcSubscription {
    pub method: String,
    pub params: Vec<String>,
}

#[derive(Debug)]
pub struct OrderManager {
    position: Decimal,
    position_limit: Decimal,
    max_slippage: Decimal,
    min_profit: Decimal,
    exchange_fees: HashMap<String, Decimal>,
    trade_history: Vec<TradeSignal>,
    is_shutting_down: bool,
    consecutive_losses: u32,
    max_consecutive_losses: u32,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            position: Decimal::ZERO,
            position_limit: MAX_POSITION,
            max_slippage: dec!(0.0005),
            min_profit: dec!(0.0005),
            exchange_fees: HashMap::from([
                ("BINANCE".to_string(), dec!(0.0010)),
                ("BYBIT".to_string(), dec!(0.0006)),
                ("MEXC".to_string(), dec!(0.0010)),
            ]),
            trade_history: Vec::with_capacity(1000),
            is_shutting_down: false,
            consecutive_losses: 0,
            max_consecutive_losses: 3,
        }
    }

    pub fn set_shutdown(&mut self) {
        self.is_shutting_down = true;
    }
fn calculate_realistic_slippage(&self, quantity: Decimal, is_buy: bool) -> Decimal {
    // Simple linear model - replace with actual exchange API data if available
    let base_slippage = self.max_slippage;
    let quantity_factor = quantity / dec!(0.01); // Normalize to 0.01 BTC
    
    // More slippage for larger orders
    base_slippage * (Decimal::ONE + quantity_factor * dec!(0.1))
}



    pub async fn execute_arbitrage(
        &mut self,
        buy_source: &str,
        sell_source: &str,
        buy_price: Decimal,
        sell_price: Decimal,
        quantity: Decimal,
        delta_p: Decimal,
        tau: Decimal,
        latency_ms: f64,
        shutdown_tx: &mut Option<oneshot::Sender<()>>,
    ) -> Result<(), String> {

    let buy_slippage = self.calculate_realistic_slippage(quantity, true);
    let sell_slippage = self.calculate_realistic_slippage(quantity, false);

        if self.is_shutting_down {
            return Err("Shutdown in progress".into());
        }
        
        if self.consecutive_losses >= self.max_consecutive_losses {
            return Err("Max consecutive losses reached".into());
        }

        let actual_buy_price = buy_price * (Decimal::ONE + buy_slippage);
        let actual_sell_price = sell_price * (Decimal::ONE - sell_slippage);

        let buy_fee = quantity * actual_buy_price * self.exchange_fees.get(buy_source).unwrap_or(&Decimal::ZERO);
        let sell_fee = quantity * actual_sell_price * self.exchange_fees.get(sell_source).unwrap_or(&Decimal::ZERO);
        
        let expected_profit = (sell_price - buy_price) * quantity;
        let actual_profit = (actual_sell_price - actual_buy_price) * quantity - buy_fee - sell_fee;

        if expected_profit < self.min_profit {
            return Err(format!("Expected profit too small: {}", expected_profit));
        }

        if quantity > self.position_limit {
            return Err("Exceeds position limit".to_string());
        }

        let direction = format!("BUY {} / SELL {}", buy_source, sell_source);

        info!(
            "EXECUTING ARBITRAGE: {} | Qty: {} | Δp: {:.8} | τ: {:.8} | δ: {:.2}ms",
            direction, quantity, delta_p, tau, latency_ms
        );

        info!(
            "PRICES: BUY {} @ {} (est) {} (actual) | SELL {} @ {} (est) {} (actual)",
            buy_source, buy_price, actual_buy_price,
            sell_source, sell_price, actual_sell_price
        );

        info!(
            "PROFIT: Expected: {} | Actual: {} | Fees: {}",
            expected_profit, actual_profit, buy_fee + sell_fee
        );

        self.position += quantity;
        let trade = TradeSignal {
            direction,
            expected_profit,
            actual_profit,
            quantity,
            timestamp: Instant::now(),
            delta_p,
            tau,
            latency_ms,
        };

        if actual_profit.is_sign_negative() {
            self.consecutive_losses += 1;
            warn!("LOSS TRADE #{}: {:.8}", self.consecutive_losses, actual_profit);
        } else {
            self.consecutive_losses = 0;
        }

        self.trade_history.push(trade);

        if let Some(tx) = shutdown_tx.take() {
            info!("🛑 Arbitrage executed successfully! Triggering shutdown...");
            self.is_shutting_down = true;
            tx.send(()).map_err(|_| "Failed to send shutdown signal")?;
        }

        Ok(())
    }

    pub fn reset_position(&mut self) {
        self.position = Decimal::ZERO;
    }

    pub fn get_trade_history(&self) -> &[TradeSignal] {
        &self.trade_history
    }
}

#[derive(Debug, Deserialize)]
pub struct BinanceBookTickerMessage {
    pub u: u64,
    pub s: String,
    pub b: String,
    pub B: String,
    pub a: String,
    pub A: String,
}

#[derive(Debug, Serialize)]
pub struct BybitSubscription {
    pub op: String,
    pub args: Vec<String>,
}

#[derive(Debug)]
pub struct KalmanFilter {
    pub x_hat: Vector2<f64>,
    pub p: Matrix2<f64>,
    pub q: Matrix2<f64>,
    pub r: Matrix2<f64>,
    pub a: Matrix2<f64>,
    pub h: Matrix2<f64>,
}

impl KalmanFilter {
    pub fn new(
        initial_price: Decimal,
        initial_velocity: Decimal,
        process_noise_q: f64,
        measurement_noise_r: f64,
    ) -> Self {
        let initial_price_f64 = initial_price.to_f64().unwrap_or_default();
        let initial_velocity_f64 = initial_velocity.to_f64().unwrap_or_default();
        let x_hat = Vector2::new(initial_price_f64, initial_velocity_f64);

        Self {
            x_hat,
            p: Matrix2::identity(),
            q: Matrix2::new(process_noise_q, 0.0, 0.0, process_noise_q * 0.1),
            r: Matrix2::new(measurement_noise_r, 0.0, 0.0, 1e-6),
            a: Matrix2::new(1.0, 1.0, 0.0, 1.0),
            h: Matrix2::new(1.0, 0.0, 0.0, 0.0),
        }
    }

    pub fn predict(&mut self) {
        self.x_hat = self.a * self.x_hat;
        self.p = self.a * self.p * self.a.transpose() + self.q;
    }

    pub fn adapt_noise(&mut self, residual: f64, innovation: f64) {
        let q_adapt = 0.95 * self.q[(0,0)] + 0.05 * residual.powi(2).max(1e-6);
        self.q = Matrix2::new(q_adapt, 0.0, 0.0, q_adapt * 0.1);
        
        let r_adapt = 0.95 * self.r[(0,0)] + 0.05 * innovation.powi(2).max(1e-6);
        self.r = Matrix2::new(r_adapt, 0.0, 0.0, 1e-6);
    }

    pub fn update(&mut self, measurement_price: Decimal) {
        let z = measurement_price.to_f64().unwrap_or_default();
        let innovation = z - (self.h * self.x_hat)[0];
        
        self.predict();
        
        let y = Vector2::new(innovation, 0.0);
        let s = self.h * self.p * self.h.transpose() + self.r;
        let k = self.p * self.h.transpose() * s.try_inverse().unwrap();

        self.x_hat += k * y;
        let i = Matrix2::identity();
        self.p = (i - k * self.h) * self.p;
        
        self.adapt_noise((z - self.x_hat[0]).abs(), innovation);
    }

    pub fn get_filtered_price(&self) -> Decimal {
        Decimal::try_from(self.x_hat[0]).unwrap_or_default()
    }
}

pub struct BayesianLatencyEstimator {
    alpha: f64,
    beta: f64,
    mu_0: f64,
    lambda: f64,
    n: u64,
    sum_ln_delta: f64,
    sum_sq_ln_delta: f64,
}

impl BayesianLatencyEstimator {
    pub fn new(initial_mu_ms: f64, initial_sigma_ms: f64) -> Self {
        let mu_ln = initial_mu_ms.ln();
        let sigma_sq_ln = (initial_sigma_ms / initial_mu_ms).powi(2).ln_1p();
        
        Self {
            alpha: 2.0,
            beta: 1.0,
            mu_0: mu_ln,
            lambda: 1.0 / sigma_sq_ln,
            n: 0,
            sum_ln_delta: 0.0,
            sum_sq_ln_delta: 0.0,
        }
    }

    pub fn observe_latency(&mut self, broker_ts: Instant, exchange_ts: Instant) {
        
    
        let delta_ms = broker_ts.duration_since(exchange_ts).as_millis() as f64 ;
        if delta_ms <= 0.0 { return; }

        let ln_delta = delta_ms.ln();
        
        self.n += 1;
        self.sum_ln_delta += ln_delta;
        self.sum_sq_ln_delta += ln_delta.powi(2);

        let lambda_n = self.lambda + self.n as f64;
        let mu_n = (self.lambda * self.mu_0 + self.sum_ln_delta) / lambda_n;
        
        self.alpha += self.n as f64 / 2.0;
        self.beta += 0.5 * (self.sum_sq_ln_delta + self.lambda * self.mu_0.powi(2) - lambda_n * mu_n.powi(2));

        self.mu_0 = mu_n;
        self.lambda = lambda_n;
    }

    pub fn get_estimated_delta_ms(&self) -> f64 {
        (self.mu_0 - 1.0/self.lambda).exp()
    }

    pub fn get_confidence(&self) -> f64 {
        1.0 - (self.beta / (self.alpha * self.lambda)).sqrt()
    }
}

#[derive(Debug)]
pub struct FeedHealth {
    last_update: Instant,
    update_frequency: Duration,
    timeouts: u32,
    consecutive_errors: u32,
}

impl FeedHealth {
    pub fn new() -> Self {
        Self {
            last_update: Instant::now(),
            update_frequency: Duration::from_millis(100),
            timeouts: 0,
            consecutive_errors: 0,
        }
    }

    pub fn record_update(&mut self) {
        let now = Instant::now();
        self.update_frequency = now - self.last_update;
        self.last_update = now;
        self.consecutive_errors = 0;
    }

    pub fn record_error(&mut self) {
        self.consecutive_errors += 1;
        if self.consecutive_errors > 3 {
            self.timeouts += 1;
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.consecutive_errors < 3 && 
        self.update_frequency < Duration::from_millis(500)
    }

    pub fn weight(&self) -> f64 {
        let mut weight = 1.0;
        weight *= 0.5f64.powi(self.consecutive_errors as i32);
        weight *= 1.0 / (1.0 + self.update_frequency.as_millis() as f64 / 100.0);
        weight
    }
}

pub struct SafetyMonitor {
    max_drawdown: Decimal,
    max_trade_rate: u32,
    last_trades: VecDeque<Instant>,
    equity: Decimal,
    peak_equity: Decimal,
}

impl SafetyMonitor {
    pub fn new() -> Self {
        Self {
            max_drawdown: dec!(0.05),
            max_trade_rate: 10,
            last_trades: VecDeque::with_capacity(20),
            equity: Decimal::ZERO,
            peak_equity: Decimal::ZERO,
        }
    }

    pub fn check_trade_allowed(&mut self, profit: Decimal) -> bool {
        self.equity += profit;
        self.peak_equity = self.peak_equity.max(self.equity);
        
        let drawdown = (self.peak_equity - self.equity) / self.peak_equity.max(dec!(0.01));
        if drawdown > self.max_drawdown {
            error!("Circuit breaker: Max drawdown exceeded ({:.2}%)", drawdown * dec!(100));
            return false;
        }
        
        let now = Instant::now();
        self.last_trades.retain(|t| now.duration_since(*t) < Duration::from_secs(60));
        if self.last_trades.len() >= self.max_trade_rate as usize {
            warn!("Trade rate limit exceeded");
            return false;
        }
        
        self.last_trades.push_back(now);
        true
    }

    pub fn emergency_stop(&self) {
        error!("EMERGENCY STOP ACTIVATED");
    }
}

#[derive(Debug, Clone)]
pub struct PricePoint {
    pub price: Decimal,
    pub timestamp: Instant,
    pub exchange_timestamp: u64,
    pub source: String,
}

#[derive(Debug)]
pub struct PriceFeed {
    pub latest_price: PricePoint,
    pub kalman_filter: KalmanFilter,
    pub signal_history: Vec<bool>,
}

pub struct StrategyState {
    pub price_feeds: HashMap<String, PriceFeed>,
    pub feed_health: HashMap<String, FeedHealth>,
    pub order_manager: OrderManager,
    pub safety_monitor: SafetyMonitor,
    pub latency_estimator: BayesianLatencyEstimator,
    pub price_history: VecDeque<PricePoint>,
    pub feed_weights: HashMap<String, f64>,
    pub daily_trades: u32,
    pub last_trade_day: Option<chrono::NaiveDate>,
    pub best_opportunity: Option<(String, String, Decimal, Decimal, Decimal)>,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub is_shutting_down: bool,
}

impl StrategyState {
    pub fn new(shutdown_tx: oneshot::Sender<()>) -> Self {
        let mut weights = HashMap::new();
        weights.insert("BINANCE_BTCUSDT".into(), 1.0);
        weights.insert("BYBIT_BTCUSDT".into(), 1.0);
        weights.insert("MEXC_BTCUSDT".into(), 0.5);

        let mut feed_health = HashMap::new();
        feed_health.insert("BINANCE_BTCUSDT".into(), FeedHealth::new());
        feed_health.insert("BYBIT_BTCUSDT".into(), FeedHealth::new());
        feed_health.insert("MEXC_BTCUSDT".into(), FeedHealth::new());

        Self {
            price_feeds: HashMap::new(),
            price_history: VecDeque::with_capacity(PRICE_HISTORY_MAX_LEN),
            latency_estimator: BayesianLatencyEstimator::new(
                LATENCY_INITIAL_MU_MS,
                LATENCY_INITIAL_SIGMA_MS
            ),
            feed_weights: weights,
            feed_health,
            order_manager: OrderManager::new(),
            safety_monitor: SafetyMonitor::new(),
            daily_trades: 0,
            last_trade_day: None,
            best_opportunity: None,
            shutdown_tx: Some(shutdown_tx),
            is_shutting_down: false,
        }
    }

    pub async fn handle_binance_message(&mut self, text: &str) {
        if text.trim().is_empty() {
            return;
        }
        
        if text.contains("result") || text.contains("id") {
            debug!("Binance connection message: {}", text);
            return;
        }
        
        match serde_json::from_str::<BinanceBookTickerMessage>(text) {
            Ok(ticker) => {
                if ticker.s != "BTCUSDT" {
                    debug!("Ignoring ticker for symbol: {}", ticker.s);
                    return;
                }
                
                match (ticker.b.parse::<Decimal>(), ticker.a.parse::<Decimal>()) {
                    (Ok(bid), Ok(ask)) => {
                        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask <= bid {
                            warn!("Invalid Binance prices - bid: {}, ask: {}", bid, ask);
                            return;
                        }
                        
                        let mid_price = (bid + ask) / dec!(2);
                        
                        self.update_feed(
                            "BINANCE_BTCUSDT".into(),
                            PricePoint {
                                price: mid_price,
                                timestamp: Instant::now(),
                                exchange_timestamp: ticker.u,
                                source: "BINANCE".into(),
                            },
                        ).await;
                    }
                    (Err(e1), Err(e2)) => {
                        error!("Failed to parse both Binance bid ({}) and ask ({}): {} | {}", 
                               ticker.b, ticker.a, e1, e2);
                    }
                    (Err(e), _) => {
                        error!("Failed to parse Binance bid ({}): {}", ticker.b, e);
                    }
                    (_, Err(e)) => {
                        error!("Failed to parse Binance ask ({}): {}", ticker.a, e);
                    }
                }
            }
            Err(e) => {
                if text.contains("\"s\"") && text.contains("\"b\"") && text.contains("\"a\"") {
                    error!("Binance ticker parse error: {} | Raw message: {}", e, text);
                } else {
                    debug!("Binance non-ticker message: {} | Error: {}", text, e);
                }
                
                if let Some(health) = self.feed_health.get_mut("BINANCE_BTCUSDT") {
                    health.record_error();
                }
            }
        }
    }
    pub async fn handle_mexc_message(&mut self, text: &str) {
        if text.trim().is_empty() {
            return;
        }
                
        if text.contains("\"code\":") || text.contains("\"msg\":") {
            if text.contains("\"code\":200") {
                info!("MEXC subscription successful");
            } else {
                warn!("MEXC response: {}", text);
            }
            return;
        }
        
        match serde_json::from_str::<MexcBookTicker>(text) {
            Ok(ticker) => {
                let bid = match ticker.d.bid.parse::<Decimal>() {
                    Ok(p) => p,
                    Err(_) => { warn!("Invalid bid price"); return; }
                };
                let ask = match ticker.d.ask.parse::<Decimal>() {
                    Ok(p) => p,
                    Err(_) => { warn!("Invalid ask price"); return; }
                };

                if ticker.s != "BTCUSDT" {
                    debug!("Ignoring MEXC ticker for symbol: {}", ticker.s);
                    return;
                }
                
                if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask <= bid {
                    warn!("Invalid MEXC prices - bid: {}, ask: {}", bid, ask);
                    return;
                }

                let mid_price = (bid + ask) / dec!(2);

                self.update_feed(
                    "MEXC_BTCUSDT".into(),
                    PricePoint {
                        price: mid_price,
                        timestamp: Instant::now(),
                        exchange_timestamp: ticker.t,
                        source: "MEXC".into(),
                    },
                ).await;
            }
            Err(e) => {
                if let Ok(raw_value) = serde_json::from_str::<serde_json::Value>(text) {
                    if raw_value.get("d").is_some() {
                        debug!("Unhandled MEXC data message: {}", text);
                    } else if raw_value.get("c").is_some() {
                        debug!("MEXC system message: {}", text);
                    }
                }
                
                error!("MEXC parse error: {} | Raw message: {}", e, text);
                if let Some(health) = self.feed_health.get_mut("MEXC_BTCUSDT") {
                    health.record_error();
                }
            }
        }
    }

    async fn update_feed(&mut self, key: String, point: PricePoint) {
        if let Some(health) = self.feed_health.get_mut(&key) {
            health.record_update();
            if !health.is_healthy() {
                warn!("Feed {} is unhealthy - latency: {:?}", key, health.update_frequency);
            }
        }

        if key.starts_with("BINANCE") || key.starts_with("BYBIT") {
            self.price_history.push_back(point.clone());
            if self.price_history.len() > PRICE_HISTORY_MAX_LEN {
                self.price_history.pop_front();
            }
        }

        let feed = self.price_feeds.entry(key.clone())
            .or_insert_with(|| {
                let health_weight = self.feed_health.get(&key)
                    .map(|h| h.weight())
                    .unwrap_or(1.0);
                    
                PriceFeed {
                    latest_price: point.clone(),
                    kalman_filter: KalmanFilter::new(
                        point.price,
                        KF_INITIAL_VELOCITY,
                        KF_PROCESS_NOISE_Q * (2.0 - health_weight as f64),
                        KF_MEASUREMENT_NOISE_R * (2.0 - health_weight as f64),
                    ),
                    signal_history: Vec::new(),
                }
            });

        feed.kalman_filter.update(point.price);
        let filtered_price = feed.kalman_filter.get_filtered_price();

        feed.latest_price = PricePoint {
            price: filtered_price,
            ..point
        };

        self.update_feed_weights().await;
        self.check_for_statistical_edge().await;
    }

    fn get_exchange_price_at_time(&self, target_time: Instant) -> Decimal {
        let mut best_price = Decimal::ZERO;
        let mut smallest_diff = Duration::MAX;

        for point in &self.price_history {
            let diff = if point.timestamp > target_time {
                point.timestamp - target_time
            } else {
                target_time - point.timestamp
            };

            if diff < smallest_diff {
                smallest_diff = diff;
                best_price = point.price;
            }
        }

        best_price
    }

    async fn check_for_statistical_edge(&mut self) {
        let current_date = chrono::Utc::now().date_naive();
        if self.last_trade_day != Some(current_date) {
            self.daily_trades = 0;
            self.last_trade_day = Some(current_date);
            self.best_opportunity = None;
        }
        
        if self.daily_trades >= 3 {
            return;
        }

        let mexc_feed = match self.price_feeds.get("MEXC_BTCUSDT") {
            Some(feed) => feed,
            None => {
                debug!("MEXC feed not available yet");
                return;
            }
        };

        let mexc_health = match self.feed_health.get("MEXC_BTCUSDT") {
            Some(health) => health,
            None => return,
        };

        let true_latency: Duration = mexc_feed.latest_price.timestamp.elapsed();
        if true_latency > Duration::from_millis(100) || !mexc_health.is_healthy() {
            return;
        }

        if let Some(latest_exchange) = self.get_most_recent_exchange_feed() {
            if latest_exchange.latest_price.timestamp.elapsed() > Duration::from_millis(100) {
                return;
            }
        }

        let (triangulated_exchange_price, confidence) = self.triangulate_exchange_prices();
        let mexc_price = mexc_feed.latest_price.price;
        let mexc_timestamp = mexc_feed.latest_price.timestamp;

        let estimated_delay_ms = self.latency_estimator.get_estimated_delta_ms();
        let delay_duration = Duration::from_millis(estimated_delay_ms as u64);
        
        let historical_exchange_price = self.get_exchange_price_at_time(
            mexc_timestamp - delay_duration
        );
        
        let temporal_disparity = mexc_price - historical_exchange_price;

        if let Some(latest_exchange_feed) = self.get_most_recent_exchange_feed() {
            self.latency_estimator.observe_latency(
                mexc_timestamp,
                latest_exchange_feed.latest_price.timestamp
            );
        }

        let short_vol = self.calculate_volatility(VOLATILITY_WINDOW_SIZE);
        let long_vol = self.calculate_volatility(LONG_VOLATILITY_WINDOW);
        let volatility = short_vol.max(long_vol).max(MIN_VOLATILITY);
        
        let tau = volatility * TRADE_THRESHOLD_FACTOR * triangulated_exchange_price;
        
        let mexc_feed = self.price_feeds.get_mut("MEXC_BTCUSDT").unwrap();
        mexc_feed.signal_history.push(temporal_disparity.abs() > tau);
        
        let recent_signals = mexc_feed.signal_history.iter()
            .rev()
            .take(SIGNAL_CONFIRMATION_COUNT);
        
        let confirmed_edge = recent_signals.filter(|&&x| x).count() >= SIGNAL_CONFIRMATION_COUNT / 2;
        
        let has_statistical_edge = confirmed_edge
            && temporal_disparity.abs() > tau
            && temporal_disparity.abs() > STATISTICAL_EDGE_THRESHOLD * triangulated_exchange_price
            && estimated_delay_ms > MIN_LATENCY_THRESHOLD_MS
            && volatility < MAX_VOLATILITY
            && confidence > MIN_CONFIDENCE
            && mexc_health.is_healthy();

        if has_statistical_edge {
            let quantity = (temporal_disparity.abs() / (volatility * dec!(2)))
                .min(MAX_POSITION)
                .max(MIN_POSITION);
            
            let buy_exchange = if temporal_disparity > dec!(0) {
                self.get_best_exchange_for_buy()
            } else {
                "MEXC".to_string()
            };

            let sell_exchange = if temporal_disparity > dec!(0) {
                "MEXC".to_string()
            } else {
                self.get_best_exchange_for_sell()
            };

            let buy_price = self.get_best_price(&buy_exchange);
            let sell_price = self.get_best_price(&sell_exchange);
            
            let mut shutdown_tx = self.shutdown_tx.take();
            
            match self.order_manager.execute_arbitrage(
                &buy_exchange,
                &sell_exchange,
                buy_price,
                sell_price,
                quantity,
                temporal_disparity,
                tau,
                estimated_delay_ms,
                &mut shutdown_tx
            ).await {
                Ok(_) => {
                    self.is_shutting_down = true;
                    self.daily_trades += 1;
                }
                Err(e) => {
                    warn!("⚠️ Arbitrage failed: {}", e);
                    self.shutdown_tx = shutdown_tx;
                }
            }
        }
    }

    fn get_most_recent_exchange_feed(&self) -> Option<&PriceFeed> {
        self.price_feeds.iter()
            .filter(|(key, _)| key.starts_with("BINANCE") || key.starts_with("BYBIT"))
            .map(|(_, feed)| feed)
            .max_by_key(|feed| feed.latest_price.timestamp)
    }

    fn get_best_exchange_for_buy(&self) -> String {
        self.price_feeds.iter()
            .filter(|(key, _)| key.starts_with("BINANCE") || key.starts_with("BYBIT"))
            .min_by_key(|(_, feed)| feed.latest_price.price)
            .map(|(key, _)| key.split('_').next().unwrap_or("BINANCE").to_string())
            .unwrap_or_else(|| "BINANCE".to_string())
    }

    fn get_best_exchange_for_sell(&self) -> String {
        self.price_feeds.iter()
            .filter(|(key, _)| key.starts_with("BINANCE") || key.starts_with("BYBIT"))
            .max_by_key(|(_, feed)| feed.latest_price.price)
            .map(|(key, _)| key.split('_').next().unwrap_or("BINANCE").to_string())
            .unwrap_or_else(|| "BINANCE".to_string())
    }

    fn calculate_volatility(&self, window_size: usize) -> Decimal {
        if self.price_history.len() < 2 {
            return MIN_VOLATILITY;
        }

        let window = if window_size > self.price_history.len() {
            self.price_history.len()
        } else {
            window_size
        };

        let mut returns = Vec::new();
        for i in 1..window {
            let ret = (self.price_history[i].price - self.price_history[i-1].price) / self.price_history[i-1].price;
            returns.push(ret);
        }

        let mean = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns.iter()
            .map(|r| (*r - mean)*(*r - mean))
            .sum::<Decimal>() / Decimal::from(returns.len());

        variance.sqrt().unwrap_or_default()
    }
    
    async fn update_feed_weights(&mut self) {
        for (source, feed) in &self.price_feeds {
            let health_weight = self.feed_health.get(source)
                .map(|h| h.weight())
                .unwrap_or(1.0);
                
            let latency_from_now = feed.latest_price.timestamp.elapsed().as_millis() as f64;
            let weight = health_weight / (1.0 + latency_from_now / 1000.0);
            self.feed_weights.insert(source.clone(), weight);
        }
    }

    fn triangulate_exchange_prices(&self) -> (Decimal, f64) {
        let mut weighted_sum = Decimal::ZERO;
        let mut total_weight = Decimal::ZERO;
        let mut confidence_sum = 0.0;
        let mut active_exchanges = 0;

        for (source_key, feed) in &self.price_feeds {
            if source_key.starts_with("BINANCE") || source_key.starts_with("BYBIT") {
                let weight = Decimal::from_f64(*self.feed_weights.get(source_key).unwrap_or(&1.0))
                    .unwrap_or(Decimal::ONE);
                
                if let Some(health) = self.feed_health.get(source_key) {
                    if health.is_healthy() && weight > dec!(0.5) {
                        weighted_sum += feed.latest_price.price * weight;
                        total_weight += weight;
                        confidence_sum += *self.feed_weights.get(source_key).unwrap_or(&1.0);
                        active_exchanges += 1;
                    }
                }
            }
        }

        if total_weight.is_zero() || active_exchanges == 0 {
            (Decimal::ZERO, 0.0)
        } else {
            let avg_confidence = confidence_sum / active_exchanges as f64;
            (weighted_sum / total_weight, avg_confidence)
        }
    }

    fn triangulate_price(&self) -> Option<(Decimal, f64)> {
        self.triangulate_exchange_prices().into()
    }

    fn get_best_price(&self, source: &str) -> Decimal {
        self.price_feeds.get(&format!("{}_BTCUSDT", source))
            .map(|feed| feed.latest_price.price)
            .unwrap_or_default()
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .format_timestamp_millis()
        .init();

    info!("🚀 Starting Arbitrage Bot");

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let strategy_state = Arc::new(Mutex::new(StrategyState::new(shutdown_tx)));

    let binance_state = strategy_state.clone();
    tokio::spawn(async move {
        let binance_url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";
        info!("Connecting to Binance WebSocket at: {}", binance_url);
        
        match connect_async(binance_url.into_client_request().unwrap()).await {
            Ok((ws_stream, _)) => {
                info!("✅ Connected to Binance WebSocket");
                let (_, mut read) = ws_stream.split();
                
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(msg) => {
                            if let Ok(text) = msg.to_text() {
                                binance_state.lock().await.handle_binance_message(text).await;
                            }
                        },
                        Err(e) => error!("Binance WebSocket error: {}", e),
                    }
                }
            }
            Err(e) => error!("Failed to connect to Binance: {}", e),
        }
    });

    let mexc_state = strategy_state.clone();
    tokio::spawn(async move {
        let mexc_url = "wss://wbs.mexc.com/ws";
        info!("Connecting to MEXC WebSocket at: {}", mexc_url);
        
        match connect_async(Url::parse(mexc_url).unwrap()).await {
            Ok((ws_stream, _)) => {
                info!("✅ Connected to MEXC WebSocket");
                let (mut write, mut read) = ws_stream.split();
                
                let sub_msg = serde_json::json!({
                    "method": "SUBSCRIPTION",
                    "params": ["spot@public.bookTicker.v3.api@BTCUSDT"]
                });
                
                if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                    error!("Failed to send MEXC subscription: {}", e);
                    return;
                }
                
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(msg) => {
                            if let Ok(text) = msg.to_text() {
                                mexc_state.lock().await.handle_mexc_message(text).await;
                            }
                        },
                        Err(e) => error!("MEXC WebSocket error: {}", e),
                    }
                }
            }
            Err(e) => error!("Failed to connect to MEXC: {}", e),
        }
    });

    tokio::select! {
        _ = shutdown_rx => {
            info!("Shutdown signal received. Exiting...");
        },
        _ = sleep(Duration::from_secs(3600)) => {
            info!("Timeout reached. Exiting...");
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received. Exiting...");
        }
    }
}