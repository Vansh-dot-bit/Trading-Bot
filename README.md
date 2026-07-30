DELTA EXCHANGE INDIA TRADING BOT - v12.8
📋 Overview
A sophisticated algorithmic trading bot for Delta Exchange India that implements multiple trading strategies including reversal detection, volatility expansion, support/resistance breakouts, and trend-following systems. The bot features dual SuperTrend analysis for dynamic take-profit management, Market Strength Index (MSI) filtering, and comprehensive email notifications via Gmail.

🚀 Key Features
Multiple Trading Strategies:

Reversal Model: Identifies potential trend reversals using price action patterns

Volatility Expansion: Catches momentum breakouts from consolidation

Range Break: Identifies when price breaks out of trading ranges

Support/Resistance Strategies: Trades breaks, rejections, and false breakouts at key price levels

Trend Following: Uses dual SuperTrend analysis to capture sustained moves

Dual SuperTrend System:

SuperTrend (14, 2.0): Fast/Accurate trend detection

SuperTrend (21, 1.0): Slow/Stable trend confirmation

Dynamic TP: Enter trend-following mode when both confirm

Exit when both SuperTrends reverse direction

Market Strength Index (MSI):

Measures momentum and market conditions

Short trades require MSI > 55 (overbought/strong bearish momentum)

Long trades require MSI < 40 (oversold/strong bullish momentum)

Blocks long trades when MSI < 24 (extreme oversold)

Risk Management:

Daily loss limit (based on realized PnL)

Configurable risk per trade (percentage of capital)

Max concurrent trades limitation

Stop-loss triggers on candle close only

Smart Price Formatting:

Auto-detects decimal places based on price magnitude

Full precision for altcoins (micro-prices up to 10 decimals)

Clean formatting for BTC/ETH (2-3 decimals)

Gmail Notifications:

Trade signals

Trade execution confirmations

Trade closure (profit/loss)

SuperTrend confirmations and exits

Daily loss warnings and limits

Startup report

📦 Installation
Prerequisites
Python 3.8 or higher

Delta Exchange India account

(Optional) Gmail App Password for notifications

Dependencies
bash
pip install requests websocket-client
🚀 Quick Start
python
python bot.py
Follow the interactive setup prompts:

Select timeframe (1m, 5m, 15m, 1h)

Choose paper or live trading mode

Enter API credentials (live mode)

Configure trade directions

Set up Gmail notifications (optional)

Enter trading parameters

⚙️ Configuration
Interactive Parameters
Parameter	Description	Default
Timeframe	Chart timeframe for analysis	1h
Mode	Paper or Live trading	Paper
Leverage	Trading leverage (1-200x)	5x
Risk per trade	Percentage of capital to risk	2%
Max open trades	Maximum concurrent trades	2
Daily loss limit	% of capital to stop trading	5%
Pattern tolerance	Tolerance for reversal pattern detection	0.1%
Environment Variables (Optional)
For automated deployment, you can set these environment variables:

bash
export DELTA_API_KEY="your_api_key"
export DELTA_API_SECRET="your_api_secret"
export GMAIL_SENDER="your_email@gmail.com"
export GMAIL_APP_PASSWORD="your_app_password"
📊 Trading Strategies
1. Reversal Model (With Market Strength Index Filter)
This strategy identifies potential trend reversals by detecting specific price action patterns that signal exhaustion of the current trend.

Pattern Formation Process:
Phase 1: Trend Continuation (2 Candles)

For Short Trades: Two consecutive bullish candles with the second closing higher than the first

Indicates strong buying momentum that may be exhausting

Shows aggressive buying pressure

For Long Trades: Two consecutive bearish candles with the second closing lower than the first

Indicates strong selling momentum that may be exhausting

Shows aggressive selling pressure

Phase 2: Indecision Pattern (1 Candle)

A small-bodied candle appears after the strong momentum

Body size ≤ 10% of total candle range

Represents buyer-seller indecision/equilibrium

Often appears at potential turning points

Higher lows (for bullish reversal) or lower highs (for bearish reversal)

Phase 3: Confirmation (1 Candle)

Short Trade Confirmation: Bearish candle closes below the indecision candle's LOW

Confirms sellers have taken control

Momentum has shifted from bullish to bearish

Long Trade Confirmation: Bullish candle closes above the indecision candle's HIGH

Confirms buyers have taken control

Momentum has shifted from bearish to bullish

Entry & Exit Rules:

Entry: On confirmation candle close

Stop Loss: Indecision pattern's HIGH (shorts) or LOW (longs)

Take Profit: 2:1 Risk-Reward ratio

Market Strength Index Filter:

Short Trades: Require MSI > 55 (overbought/strong bearish momentum)

Confirms price has moved too far, too fast

Increases probability of reversal

Long Trades: Require MSI < 40 (oversold/strong bullish momentum)

Confirms price has dropped too far, too fast

Increases probability of reversal

BLOCKS trades when MSI < 24 (extreme oversold)

Prevents catching falling knives

Waits for more reliable reversal signals

2. Volatility Expansion (No MSI Filter)
This strategy identifies when price breaks out of a consolidation range with strong momentum.

Pattern Formation Process:
Phase 1: Range Identification

Look back 21 candles to find the trading range

Identify the HIGHEST HIGH and LOWEST LOW of these 21 candles

This represents the recent consolidation zone

Phase 2: Breakout Detection

Short Setup: Current candle breaks BELOW the lowest low

Price has broken support

Indicates bearish momentum building

Long Setup: Current candle breaks ABOVE the highest high

Price has broken resistance

Indicates bullish momentum building

Phase 3: Quality Validation

Check the breakout candle's wick length

Wick must be ≤ 10% of total candle range

Small wick = strong breakout conviction

Large wick = rejection/indecision

Entry & Exit Rules:

Entry: Immediately at candle close (no waiting for confirmation)

Stop Loss: Breakout candle's HIGH (shorts) or LOW (longs)

Take Profit: 2:1 Risk-Reward ratio

Rationale:

Low volatility periods (consolidation) are often followed by high volatility

Clean breaks with small wicks tend to have stronger follow-through

Momentum often continues in breakout direction

3. Range Break Strategy (No MSI Filter)
This strategy identifies when price breaks out of a well-defined trading range.

Pattern Formation Process:
Phase 1: Range Definition

Latest 7 completed candles define the range

Calculate RANGE HIGH (maximum high)

Calculate RANGE LOW (minimum low)

This is a rolling range (recalculated each candle)

Phase 2: First Breakout

Short Setup: Break candle closes BELOW range low

Initial breakdown of support

First sign of bearish momentum

Long Setup: Break candle closes ABOVE range high

Initial breakout of resistance

First sign of bullish momentum

Phase 3: Confirmation

Short Setup: Next bearish candle closes BELOW break candle's close

Confirms breakdown is real (not a fakeout)

Shows continued selling pressure

Long Setup: Next bullish candle closes ABOVE break candle's close

Confirms breakout is real (not a fakeout)

Shows continued buying pressure

Entry & Exit Rules:

Entry: On confirmation candle close

Stop Loss: Break candle's HIGH (shorts) or LOW (longs)

Take Profit: 2:1 Risk-Reward ratio

Rationale:

Ranges represent equilibrium between buyers and sellers

Breakout indicates one side has gained control

Confirmation helps reduce false breakouts

4. Support/Resistance Strategies (No MSI Filter)
This module identifies key price levels where the market has historically shown interest.

Level Detection Process:
Phase 1: Swing Point Identification

Analyze the last 100 completed candles

Swing High: A candle whose high is greater than the highs of the previous 5 candles and the next 5 candles

Swing Low: A candle whose low is lower than the lows of the previous 5 candles and the next 5 candles

Swing levels are confirmed only after enough future candles exist (minimum age of 3 candles)

Phase 2: Level Merging

Nearby levels within the merge threshold (0.5%) are combined into a single level

Reduces noise from multiple nearby swing points

Creates cleaner support/resistance zones

Phase 3: Level Aging

Each level has an age counter that increments each candle

Levels expire after maximum age (50 candles) unless refreshed

Stronger levels (more touches) can stay longer (up to 100 candles)

Levels younger than minimum age (3 candles) are not considered valid

Phase 4: Strength Tracking

Each level stores a strength value which increases whenever price respects or retests the level

Strength increases when:

Price touches the level (within threshold)

A new swing point forms near the level

Stronger levels are prioritized during strategy evaluation

Minimum strength of 1 required for trading

Sub-Strategy A: Breakout (Confirmation Required)
Short Setup (Support Breakdown):

Break candle closes BELOW support level

Bearish confirmation candle closes BELOW break candle's close

Entry on confirmation candle close

Stop Loss = Break candle HIGH

Long Setup (Resistance Breakout):

Break candle closes ABOVE resistance level

Bullish confirmation candle closes ABOVE break candle's close

Entry on confirmation candle close

Stop Loss = Break candle LOW

Rationale:

Levels that have been tested multiple times are more significant

Breakouts with confirmation help reduce false signals

Stronger levels tend to produce better risk-reward opportunities

Sub-Strategy B: Rejection (Immediate Execution)
Monitoring Process:

Identify level zones (0.5% tolerance around exact level)

When price reaches zone, start monitoring

Monitor for up to 25 candles

Execute at the close of the rejection candle without waiting for an additional confirmation candle

Expire if no rejection within 25 candles

Short Setup (Resistance Rejection):

Price reaches resistance zone

Bearish candle forms (close < high)

Indicates rejection at resistance

Execute at the close of the rejection candle

Stop Loss = Rejection candle HIGH

Long Setup (Support Rejection):

Price reaches support zone

Bullish candle forms (close > low)

Indicates rejection at support

Execute at the close of the rejection candle

Stop Loss = Rejection candle LOW

Rationale:

Zone-based approach is more realistic than exact levels

Immediate execution captures the reversal

Window prevents stale setups from being considered

Sub-Strategy C: False Breakout (Confirmation Required)
Short Setup (Resistance Fakeout):

Price breaks ABOVE resistance

SAME candle closes BACK BELOW resistance

Bearish reversal candle forms

Bearish confirmation candle closes BELOW reversal candle's close

Entry on confirmation candle close

Stop Loss = Reversal candle HIGH

Long Setup (Support Fakeout):

Price breaks BELOW support

SAME candle closes BACK ABOVE support

Bullish reversal candle forms

Bullish confirmation candle closes ABOVE reversal candle's close

Entry on confirmation candle close

Stop Loss = Reversal candle LOW

Rationale:

Attempts to identify failed breakout scenarios

Designed to reduce false signals from fakeouts

Confirmation required for additional safety

5. Momentum Reversal (With Market Strength Index Filter)
This is a fallback strategy when the Reversal Model doesn't trigger.

Pattern Formation Process:
Short Setup (Bullish→Bearish→Bearish):

Bullish candle (strong buying)

Bearish candle (selling starts)

Bearish signal candle closes BELOW first candle's LOW

Confirms momentum has shifted to downside

Long Setup (Bearish→Bullish→Bullish):

Bearish candle (strong selling)

Bullish candle (buying starts)

Bullish signal candle closes ABOVE first candle's HIGH

Confirms momentum has shifted to upside

Market Strength Index Filter:

Short: Requires MSI > 55 (overbought/strong bearish momentum)

Long: Requires MSI < 40 (oversold/strong bullish momentum)

Block Long: When MSI < 24 (extreme oversold)

🔄 Dual SuperTrend System
Overview
The bot uses a dual SuperTrend analysis system that works in two phases:

Phase 1: Initial Entry
Trade enters with standard 2:1 R:R take-profit

SuperTrend indicators monitor in background

No trend confirmation required for entry

Phase 2: Trend Confirmation
The bot analyzes market trends using two SuperTrend indicators:

SuperTrend (14, 2.0):

Length: 14 periods

Factor: 2.0

More sensitive to recent price action

Catches trends early

SuperTrend (21, 1.0):

Length: 21 periods

Factor: 1.0

More stable, fewer false signals

Confirms trend strength

Confirmation Conditions:

Short Trade: Both SuperTrends show bearish trend (RED)

Confirms strong bearish momentum

Cancels fixed take-profit

Switches to trend-following exit

Long Trade: Both SuperTrends show bullish trend (GREEN)

Confirms strong bullish momentum

Cancels fixed take-profit

Switches to trend-following exit

Phase 3: Trend-Following Exit
Exit Conditions:

Short Trade: Both SuperTrends flip to bullish (GREEN)

Bearish trend has reversed

Exit to protect profits

Long Trade: Both SuperTrends flip to bearish (RED)

Bullish trend has reversed

Exit to protect profits

Rationale:

Combines fast and slow SuperTrend analysis

Fast indicator catches trends early

Slow indicator confirms trend strength

Both required for confirmation (reduces false signals)

Both required for exit (ensures true reversal)

📧 Gmail Notifications
Setting Up Gmail App Password
Go to Google Account → Security

Enable 2-Step Verification

Go to App Passwords

Generate a 16-character password

Use this password (not your regular password)

Notification Types
Type	When	Content
Signal Alert	Strategy identifies trade	Entry, SL, TP, MSI, risk details
Trade Executed	Order fills	Size, entry price, order ID
SuperTrend Confirmed	Both STs confirm	Updated exit strategy
SuperTrend Exit	STs reverse	Exit price, PnL
Trade Closed	TP/SL hit	Close reason, PnL
Daily Loss Warning	80% of limit	Current loss percentage
Daily Limit Hit	Limit reached	Trading stopped
Startup Report	Bot starts	Configuration summary
📈 Price Precision (smart_fmt)
The bot automatically formats prices based on magnitude:

Price Range	Decimal Places	Example
≥ 10,000	2	65,000.00
≥ 1,000	3	3,456.789
≥ 100	4	456.7890
≥ 10	5	12.34567
≥ 1	6	1.234567
≥ 0.1	7	0.1234567
≥ 0.01	8	0.01234567
< 0.01	10	0.0012345678
📁 File Structure
text
DeltaBot/
├── bot.py              # Main bot code
├── bot.log             # Rotating log file (auto-created)
├── README.md           # This file
└── requirements.txt    # Python dependencies
🔧 Advanced Customization
Constants You Can Modify
python
# Market Strength Index Parameters
MSI_PERIOD = 14
MSI_OVERBOUGHT = 55.0      # Bearish momentum threshold
MSI_OVERSOLD = 40.0        # Bullish momentum threshold
MSI_MIN_CANDLES = MSI_PERIOD + 1
MSI_EXTREME_OVERSOLD = 24.0  # Blocks long trades

# Reversal Pattern Parameters
REVERSAL_BODY_RATIO_MAX = 0.10  # Max body size for reversal patterns

# Risk Parameters
TP_RR_RATIO = 2.0
TP_MAX_PCT = 0.05
DAILY_LOSS_LIMIT_PCT = 0.05

# Range Break
RANGE_BREAK_LOOKBACK = 7

# Volatility Expansion
VOL_EXP_LOOKBACK = 21
VOL_EXP_MAX_WICK_RATIO = 0.10

# Support/Resistance
SR_LOOKBACK = 100
SR_SWING_SENSITIVITY = 5
SR_MERGE_THRESHOLD = 0.005
SR_MIN_LEVEL_AGE = 3
SR_MAX_LEVEL_AGE = 50
SR_MIN_STRENGTH = 1
SR_PRICE_TOUCH_THRESHOLD = 0.002

# S/R Rejection
SR_REVERSAL_MONITOR_WINDOW = 25
SR_ZONE_TOLERANCE = 0.005

# SuperTrend
ST1_LENGTH = 14
ST1_FACTOR = 2.0
ST2_LENGTH = 21
ST2_FACTOR = 1.0
🛡️ Error Handling
The bot includes comprehensive error handling for:

WebSocket disconnections (auto-reconnect with backoff)

API rate limiting (exponential backoff)

Authentication failures

Network timeouts

Missing data

Invalid candles

📊 Monitoring
The bot provides real-time status updates every 60 seconds:

text
[STATUS] Open=2/2  Signals=45  TPs=18  SLs=12  WS=connected  
Daily loss: $23.40 / $100.00 (23.4%)  
Trades=['BTCUSD_PERP', 'ETHUSD_PERP']  [BTCUSD:ST-MODE] [ETHUSD:NO-MSI]
Status Fields
Open: Active trades / Max allowed

Signals: Total signals generated

TPs: Take-profit hits

SLs: Stop-loss hits

WS: WebSocket state (connected/disconnected/reconnecting)

Daily loss: Current loss / Limit

Trades: Active trade symbols

ST-MODE: Trade in SuperTrend exit mode

NO-MSI: Trade using no MSI filter

🧪 Testing Gmail
Before starting the bot, you can test Gmail notifications:

python
# Run the bot and select 'y' when asked
Test Gmail notifications first? (y/N) : y
🔒 Security Best Practices
Never share API credentials

Use read-only keys for paper trading

Enable 2FA on your Delta account

Use Gmail App Password (not regular password)

Monitor trade execution logs

📝 Logging
The bot creates a rotating log file (bot.log):

Max size: 5MB

Backup count: 3

Log levels: DEBUG (file), INFO (console)

🐛 Troubleshooting
Common Issues
Authentication Failed

Verify API key and secret

Check Delta Exchange India URL

Ensure account has trading permissions

WebSocket Disconnects

Check network connectivity

Firewall blocking WebSocket ports

Increase max_reconnect_attempts

No Signals Generated

Check timeframe and symbol availability

Verify MSI filters (may be too restrictive)

Ensure sufficient historical candles

Gmail Notifications Fail

Verify App Password (16 characters, no spaces)

Check sender and recipient emails

Ensure Less Secure Apps is enabled

📚 API References
Delta Exchange API

WebSocket Documentation

📄 License
This project is for educational purposes. Use at your own risk.

⚠️ Disclaimer
This is not financial advice. Trading cryptocurrencies carries substantial risk. The bot is provided as-is without warranty. Past performance does not guarantee future results. Always test strategies in paper mode first.

🔄 Version History
v12.8 (Current)
Consolidated all candle patterns into unified reversal detection

Added S/R Rejection strategy with 25-candle monitoring window

Added S/R False Breakout strategy

Implemented Support/Resistance strength tracking

Added auto-formatting price precision (smart_fmt)

Improved error handling and reconnection logic

Renamed RSI to Market Strength Index (MSI) for better clarity

v12.7
Added Range Break strategy

Added S/R Breakout strategy with confirmation

Improved SuperTrend exit logic

Enhanced PnL fetching with retries

v12.6
Initial release with reversal detection

Dual SuperTrend system

Gmail notifications

Paper/Live trading modes

🤝 Support
For issues and questions:

Check the troubleshooting section

Review bot.log for error details

Verify your configuration settings

Test in paper mode first

📈 Strategy Decision Flow
text
Market Analysis
    │
    ├── With Market Strength Index Filter
    │       │
    │       ├── Reversal Model (2-candle continuation + indecision + confirmation)
    │       │   • Short: MSI > 55 required (strong bearish momentum)
    │       │   • Long: MSI < 40 required (strong bullish momentum)
    │       │   • Block Long: MSI < 24 (extreme oversold)
    │       │
    │       └── Momentum Reversal (3-candle reversal pattern)
    │           • Short: MSI > 55 required
    │           • Long: MSI < 40 required
    │
    └── Without MSI Filter
            │
            ├── Volatility Expansion (21-candle break + wick ≤ 10%)
            │   • Immediate execution at candle close
            │
            ├── Range Break (7-candle range + confirmation)
            │   • Entry on confirmation candle close
            │
            ├── S/R Breakout (100-candle S/R + strength + confirmation)
            │   • Entry on confirmation candle close
            │
            ├── S/R Rejection (Zone-based + 25-candle window)
            │   • Execute at rejection candle close
            │
            └── S/R False Breakout (Fakeout + confirmation)
                • Entry on confirmation candle close
🎯 Strategy Selection Guide
Market Condition	Recommended Strategy	Rationale
Strong Trending	Volatility Expansion	Catches momentum early
Sideways/Ranging	Range Break	Identifies range breakouts
Near Support/Resistance	S/R Breakout or Rejection	Key levels offer high probability
Fakeouts	S/R False Breakout	Attempts to identify failed breakouts
Volatile/Exhausted	Reversal Model	Identifies trend exhaustion
Stable Trend	Dual SuperTrend System	Captures sustained moves
📊 Position Sizing
Position size is calculated based on multiple factors:

Account Balance: Current available trading capital

Risk Percentage: Configured risk per trade (e.g., 2%)

Stop-Loss Distance: Difference between entry and stop-loss price

Leverage: Configured leverage multiplier

Contract Specifications: Delta Exchange contract sizes

The calculation ensures that the maximum loss per trade does not exceed the configured risk percentage of the trading capital, while also respecting margin requirements based on the selected leverage.

Daily Loss Limit Example
text
Trading Capital: $10,000
Daily Loss Limit: 5% ($500)

If losses reach $400 (80% of limit):
→ Warning notification sent

If losses reach $500 (100% of limit):
→ Trading halted until UTC day reset
→ Limit hit notification sent
🏆 Best Practices
For Optimal Performance
Start with Paper Trading: Test strategies without real money

Use Appropriate Timeframes: Match strategy to market conditions

Monitor Daily Loss: Review daily PnL to prevent large drawdowns

Keep Logs: Review bot.log for debugging and optimization

Update Regularly: Use latest version with improvements

Strategy Selection Tips
Trending Markets: Volatility Expansion + S/R Breakout

Ranging Markets: Range Break + S/R Rejection

Volatile Markets: Reversal Model + S/R False Breakout

Mixed Markets: Run all strategies (let market decide)

Risk Management Rules
Never risk more than configured percentage per trade

Always use stop-losses

Respect daily loss limits

Don't over-leverage (max 5x recommended)

Diversify across symbols

🔄 WebSocket Reconnection Flow
text
Initial Connection
    │
    ├── Success → Connected
    │
    └── Failed → Reconnecting
            │
            ├── Attempt 1: Wait 5s
            ├── Attempt 2: Wait 10s
            ├── Attempt 3: Wait 20s
            ├── Attempt 4: Wait 30s
            └── After 10 attempts: Disconnected
📈 SuperTrend Exit Example
text
SHORT Trade Entry
    │
    ├── Entry Price: $50,000
    ├── Take Profit: $48,000 (2:1 R:R)
    └── Stop Loss: $51,000
    │
    ├── Price drops to $49,000
    │   └── SuperTrend (14,2) turns BEARISH
    │   └── SuperTrend (21,1) turns BEARISH
    │       └── ✓ Both confirmed → Switch to Trend mode
    │           └── Cancel TP bracket
    │           └── Exit when both turn BULLISH
    │
    └── Price continues to $45,000
        └── SuperTrend (14,2) turns BULLISH
        └── SuperTrend (21,1) turns BULLISH
            └── ✓ Both reversed → EXIT
                └── Realized PnL: +$5,000
🎨 Customization Examples
Adjust Market Strength Index Thresholds for Volatile Coins
python
MSI_OVERBOUGHT = 60.0  # Higher for volatile coins
MSI_OVERSOLD = 35.0    # Lower for volatile coins
MSI_EXTREME_OVERSOLD = 20.0  # Adjust extreme oversold level
Make Reversal Patterns More Sensitive
python
REVERSAL_BODY_RATIO_MAX = 0.15  # 15% instead of 10%
Adjust S/R Detection
python
SR_MERGE_THRESHOLD = 0.01       # 1% instead of 0.5%
SR_MIN_STRENGTH = 2             # Require minimum strength of 2
Modify SuperTrend for Different Timeframes
python
# For 5m timeframe (more sensitive)
ST1_LENGTH = 10
ST1_FACTOR = 2.5

# For 1h timeframe (less sensitive)  
ST1_LENGTH = 20
ST1_FACTOR = 1.5
🏗️ System Architecture
text
Market Data
    │
    ▼
Historical Candle Collection
    │
    ▼
Indicator Calculation
    │
    ▼
Strategy Evaluation
    │
    ▼
Risk Management
    │
    ▼
Order Execution
    │
    ▼
Trade Monitoring
    │
    ▼
Email Notifications
    │
    ▼
Logging & Performance Tracking
Architecture Components
1. Market Data

Receives real-time price data via WebSocket

Collects historical candle data via REST API

Maintains synchronized candle store

2. Historical Candle Collection

Loads initial historical data on startup

Updates candle store with each new candle

Maintains configurable lookback periods

3. Indicator Calculation

Calculates Market Strength Index (MSI) for momentum analysis

Computes SuperTrend values for trend detection

Identifies support/resistance levels

Detects swing points and patterns

4. Strategy Evaluation

Evaluates all configured strategies in order

Applies MSI filters where configured

Validates pattern requirements

Generates trade signals

5. Risk Management

Calculates position sizes based on risk parameters

Enforces daily loss limits

Manages concurrent trade limits

Places stop-loss and take-profit orders

6. Order Execution

Places market orders for entry

Sets bracket orders for take-profit

Closes positions on exit signals

Handles order fill confirmations

7. Trade Monitoring

Tracks active trades

Monitors for TP/SL triggers

Checks SuperTrend confirmations and exits

Updates trade status and PnL

8. Email Notifications

Sends signal alerts

Notifies trade execution and closure

Reports SuperTrend events

Warns of daily loss limits

9. Logging & Performance Tracking

Records all bot activity

Trades performance metrics

Generates status reports

Maintains audit trail

📋 Strategy Evaluation Order
The bot evaluates strategies in the following order within each candle processing cycle:

Short Trades (in order):
With Market Strength Index Filter:

Reversal Model (check_short_signal)

Requires MSI > 55 (strong bearish momentum)

Pattern: 2 Bullish continuation + Indecision + Bearish confirmation

Momentum Reversal (fallback in check_short_signal)

Requires MSI > 55

Pattern: Bullish → Bearish → Bearish

Without MSI Filter:

Range Break (check_short_signal_no_rsi)

No MSI requirement

Pattern: 7-candle range breakdown + confirmation

Volatility Expansion (check_short_signal_no_rsi)

No MSI requirement

Pattern: 21-candle low break + wick check

S/R Breakout (check_short_signal_no_rsi)

No MSI requirement

Pattern: Support breakdown + confirmation

S/R False Breakout (check_short_signal_no_rsi)

No MSI requirement

Pattern: Resistance fakeout + confirmation

S/R Rejection (check_short_signal_no_rsi)

No MSI requirement

Pattern: Resistance rejection (immediate execution)

Long Trades (in order):
With Market Strength Index Filter:

Reversal Model (check_long_signal)

Requires MSI < 40 (strong bullish momentum)

Blocks if MSI < 24 (extreme oversold)

Pattern: 2 Bearish continuation + Indecision + Bullish confirmation

Momentum Reversal (fallback in check_long_signal)

Requires MSI < 40

Blocks if MSI < 24

Pattern: Bearish → Bullish → Bullish

Without MSI Filter:

Range Break (check_long_signal_no_rsi)

No MSI requirement

Pattern: 7-candle range breakout + confirmation

Volatility Expansion (check_long_signal_no_rsi)

No MSI requirement

Pattern: 21-candle high break + wick check

S/R Breakout (check_long_signal_no_rsi)

No MSI requirement

Pattern: Resistance breakout + confirmation

S/R False Breakout (check_long_signal_no_rsi)

No MSI requirement

Pattern: Support fakeout + confirmation

S/R Rejection (check_long_signal_no_rsi)

No MSI requirement

Pattern: Support rejection (immediate execution)

Important Notes:
Once a valid strategy generates a trade signal, the bot follows its normal risk management and execution flow

The bot stops evaluating further strategies for that symbol after a signal is generated

MSI-filtered strategies are always evaluated before no-MSI strategies

Short and long trades are evaluated independently

The evaluation order is designed to prioritize strategies with additional confirmation filters
