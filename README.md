TradePilot
TradePilot is an automated cryptocurrency trading system built
around the Delta Exchange India API. It combines a Python trading engine
with market-monitoring services, email alerts, a FastAPI backend,
PostgreSQL persistence, and a Next.js dashboard.
> **Important:** Trading involves substantial financial risk. TradePilot
> is software, not financial advice or a guarantee of profit. Start in
> paper mode, verify exchange settings and position sizing, and never
> deploy live trading without reviewing the code and testing it
> independently.
---
Table of contents
Project overview
Architecture
Features
Trading engine
Market-monitoring services
Dashboard and backend
Project structure
Requirements
Installation
Configuration and secrets
Run the trading engine
Run the web application
Backend API
Database
Logging and generated data
Deployment notes
Security checklist
Current development status
Disclaimer
Project overview
TradePilot is organized into three main parts:
Trading engine --- Python code that retrieves market data,
evaluates trading conditions, manages positions, and communicates
with Delta Exchange India.
Backend --- FastAPI services that expose bot controls and data
to the web application, with SQLAlchemy/PostgreSQL persistence.
Frontend --- A Next.js application that provides the TradePilot
dashboard and navigation for signals, trades, strategies, market
information, analytics, notifications, and settings.
The trading engine remains the source of truth for trading decisions.
The dashboard should display backend/engine data rather than
independently reimplementing trading indicators or strategy rules.
Architecture
``` text
                 Next.js dashboard
                        |
                 HTTP / API requests
                        |
                   FastAPI backend
                  /       |       \
             BotManager  Services  PostgreSQL
                  |
             TradingBot engine
                  |
          Delta Exchange India
                  |
       REST API + market WebSocket

Additional monitoring services:
  - Federal Reserve rate detector
  - US technology-stock index detector
  - Quant Pairs Divergence strategy
  - Other companion modules imported by the trading engine
```
Features
Trading and exchange connectivity
Delta Exchange India REST API integration.
Delta Exchange India WebSocket market-data connection.
Historical candle loading and completed-candle processing.
Configurable trading symbols and timeframes.
Paper and live execution modes.
Long and short direction controls.
Configurable leverage, trading capital, risk percentage, and maximum
concurrent trades.
Position monitoring, stop-loss handling, take-profit handling, and
trade-close processing.
Local logging to the terminal and rotating `bot.log` files.
The exchange API credentials must remain private and must never be
placed in frontend code.
Risk and position management
The trading engine contains risk controls such as:
Risk percentage per trade.
Position sizing based on the trade's stop-loss distance and
contract/exchange constraints.
Maximum concurrent trades.
Daily realized-loss tracking and a daily loss limit.
Stop-loss and take-profit exit handling.
Post-exit cooldown handling for symbols.
Actual position size can depend on contract specifications, lot-size
conversion, available capital, leverage, and exchange validation. Do not
assume that leverage alone determines the maximum loss on a trade.
Signals and notifications
The engine maintains in-memory signal and trade state and supports
callback hooks for dashboard integration. The Gmail notifier can send
alerts for events such as signals, trade execution, trade closure,
SuperTrend exits, and risk-limit events. Email delivery depends on valid
Gmail SMTP credentials and configuration.
Trading engine
The main engine is in `bot.py` (or the current project-specific
trading-engine module). It is responsible for:
Exchange communication.
Candle buffers and closed-candle evaluation.
Strategy evaluation.
Entry and exit decisions.
Position sizing and exchange order handling.
Stop-loss and take-profit monitoring.
Daily risk controls.
Logging and notification callbacks.
Strategy families
The project has developed multiple strategy families, including:
Reversal / price-action setups.
Doji reversal setups.
Range-break setups.
Volatility-expansion setups.
Support/resistance breakout setups.
Support/resistance false-breakout reversal setups.
Engulfing-candle setups.
Harami-candle setups.
The exact enabled strategies and their current conditions are defined by
the version of the trading-engine source and runtime configuration.
Review the current source before assuming that every historical strategy
is enabled.
Indicators and trade management
The trading engine has used RSI/MSI-style momentum filtering and dual
SuperTrend monitoring. The dual SuperTrend configuration documented in
the project is:
SuperTrend (14, 2)
SuperTrend (21, 1)
When both SuperTrend readings align with the trade direction, the engine
can use that confirmation in its trade-management logic. A reversal of
both readings can be used as an exit condition. Indicator values and
thresholds should be checked against the current source code before
changing configuration.
Support and resistance
The engine maintains support/resistance levels and uses them in the
relevant breakout and false-breakout strategies. Levels are derived from
candle history and evaluated against current market price. A newly
detected level is not, by itself, necessarily a trade signal or an email
alert; alerts depend on the configured signal/notification conditions.
Market-monitoring services
Federal Reserve rate detector --- `fed_rate_detector.py`
This service:
Finds the latest Federal Reserve FOMC statement from the official
Federal Reserve website.
Extracts the federal funds target range and the decision wording.
Compares the new decision with the previously saved state.
Records state/history in JSON files.
Can send an informational alert when the target range changes.
Supports a configurable polling interval when run as a background
service.
A first run establishes a baseline and does not send a change alert. A
new statement with an unchanged target range is recorded without
treating it as a rate change. The market interpretation in an alert is
informational, not a guaranteed crypto-market prediction.
Technology-stock index detector --- `tech_stock.py`
This companion service builds a synthetic US technology-stock index
using:
Massive for daily OHLC market data.
Financial Modeling Prep (FMP) for float-share data.
Estimated free-float market-cap weights.
A synthetic index base value of 1,000.
For each new completed index candle, it compares the latest close and
candle range with the averages of the previous 100 completed index
candles. The current implementation evaluates:
LONG alert: latest close is above the previous-100-candle
average close and latest range is above the previous-100-candle
average range.
SHORT alert: latest close is below the previous-100-candle
average close and latest range is above the previous-100-candle
average range.
The index is a current-weight synthetic calculation, not an official
historical index. Its weighted OHLC range is approximate because
component stock highs and lows do not necessarily occur at the same
time. Alerts are written to the terminal and
`tech_index_data/alerts.log`; the service can also use the shared
notifier when integrated.
Quant Pairs Divergence --- `quant_strategy.py`
This is a separate pairs-analysis module. It calculates statistics from
the latest 200 synchronized completed candles for two symbols.
Current strategy constants in the module:
Parameter                                                   Value
---
Entry Z-score                                                2.25
Hard-stop absolute Z-score                                    4.0
Mean-reversion exit zone                           -0.25 to +0.25
ADF maximum p-value                                          0.05
ADF recheck interval                                   10 candles
Lookback                       200 synchronized completed candles
Direction logic:
Z-score at or below -2.25: long leg A and short leg B.
Z-score at or above +2.25: short leg A and long leg B.
Entry requires the ADF condition to pass as well as the Z-score
threshold.
Exit can occur when the Z-score returns to the mean-reversion zone.
A hard exit condition is defined when the absolute Z-score reaches
4.0.
The main bot remains responsible for exchange execution and
account-level risk checks. Quant pair symbols and contract lot
conversions must follow the format expected by the current bot
configuration.
Other companion modules
The main engine imports additional companion modules, including
`serede.py`. Their behavior should be documented from the current source
implementation rather than inferred from the filename.
Dashboard and backend
Backend
The backend uses:
FastAPI for HTTP endpoints.
SQLAlchemy for database access.
PostgreSQL for persistent records.
`BotManager` to start, stop, and inspect the Python trading engine.
The backend currently exposes bot status, start/stop controls, signals,
active trades, and stop-loss-event routes. The exact schemas are defined
in `backend/config.py`, `backend/models.py`, and the route definitions
in `backend/main.py`.
Frontend
The frontend is a Next.js application using TypeScript and Tailwind CSS.
The planned dashboard navigation includes:
Route              Purpose
---
`/`                Dashboard
`/signals`         Generated trading signals
`/trades`          Active and historical trades
`/strategies`      Strategy information and controls
`/market`          Market information
`/analytics`       Performance analytics
`/notifications`   Alerts and events
`/settings`        Bot configuration
Some pages may still be placeholders while the UI and backend
integration are being developed. Do not assume that every dashboard card
or route is already backed by live database data.
Project structure
The local development project is organized approximately as follows:
``` text
Trade Pilot/
├── backend/
│   ├── main.py
│   ├── bot_manager.py
│   ├── config.py
│   ├── database.py
│   ├── models.py
│   └── db_service.py
│
├── bot/
│   ├── __init__.py
│   └── trading_bot.py
│
├── frontend/
│   ├── app/
│   │   ├── page.tsx
│   │   ├── analytics/
│   │   ├── market/
│   │   ├── notifications/
│   │   ├── settings/
│   │   ├── signals/
│   │   ├── strategies/
│   │   └── trades/
│   └── components/
│
├── bot.py
├── quant_strategy.py
├── fed_rate_detector.py
├── tech_stock.py
├── serede.py
├── bot.log
└── README.md
```
This is a guide to the project layout, not a guarantee that every listed
file exists in every branch or deployment. Keep the actual repository
structure and import paths consistent. In particular, the standalone bot
entry point and the dashboard-integrated bot module should not be
confused or substituted without checking their imports.
Requirements
Python
Use a Python version compatible with all installed dependencies and the
current project source.
Core packages used across the project include:
``` text
requests
websocket-client
numpy
pandas
statsmodels
beautifulsoup4
fastapi
uvicorn
sqlalchemy
psycopg2-binary
pydantic
```
The exact dependency set can vary by enabled module. Maintain a project
`requirements.txt` or separate dependency files and install from those
files for repeatable deployments.
Frontend
Node.js (LTS recommended).
npm.
Next.js.
React.
TypeScript.
Tailwind CSS.
Installation
Clone the repository:
``` bash
git clone <YOUR_REPOSITORY_URL>
cd "Trade Pilot"
```
Create and activate a Python virtual environment.
Windows PowerShell:
``` powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```
Linux / macOS:
``` bash
python3 -m venv venv
source venv/bin/activate
```
Install the Python dependencies:
``` bash
pip install -r requirements.txt
```
If the repository uses separate dependency files for optional services,
install the relevant file as documented in that module.
Install frontend dependencies:
``` bash
cd frontend
npm install
```
Configuration and secrets
Do not commit API keys, passwords, private keys, or account information.
The project may require the following credentials, depending on which
services are enabled:
Credential                      Used for
---
Delta Exchange API key          Authenticated exchange requests
Delta Exchange API secret       Request signing
Gmail sender and app password   Email notifications
Massive API key                 US stock daily market data
FMP API key                     Float-share data for index weights
PostgreSQL connection URL       Backend database connection
Use environment variables or a secrets manager. A local `.env` file
should be excluded from Git using `.gitignore`. Never expose backend
secrets through `NEXT_PUBLIC_*` variables or browser-side JavaScript.
Example placeholder names (adapt them to the exact names read by your
current code):
``` env
DELTA_API_KEY=
DELTA_API_SECRET=

GMAIL_SENDER=
GMAIL_APP_PASSWORD=
GMAIL_RECIPIENT=

MASSIVE_API_KEY=
FMP_API_KEY=

DATABASE_URL=
```
Do not paste real credentials into README files, source files,
screenshots, issue trackers, or chat messages.
Run the trading engine
The exact entry point depends on the deployment mode and current branch.
For the standalone interactive bot, the source has historically been
launched with:
``` bash
python bot.py
```
Follow the prompts and verify all selected symbols, timeframe, trading
capital, lot size, risk settings, and mode before allowing the process
to trade.
For the dashboard-integrated engine, the backend's `BotManager` imports
the configured `TradingBot` class and starts it in a background thread.
Use the backend start route only after confirming that the API
configuration model supplies every field required by the current
`TradingBot` constructor and startup checks.
Keep paper mode enabled during development. Do not start live
trading just to verify that the website or API is working.
Run the web application
Run the backend from the repository root:
``` bash
uvicorn backend.main:app --reload
```
The local API is normally available at:
``` text
http://127.0.0.1:8000
```
FastAPI interactive documentation:
``` text
http://127.0.0.1:8000/docs
```
Run the frontend in a second terminal:
``` bash
cd frontend
npm run dev
```
The local dashboard is normally available at:
``` text
http://localhost:3000
```
Keep both development processes running while using the dashboard. If
the frontend cannot reach the backend, check that the backend process is
running and that the FastAPI CORS configuration allows the exact
frontend origin (for example, `http://localhost:3000`).
Backend API
The current backend route set includes:
Method   Endpoint                      Purpose
---
`GET`    `/`                           Backend identity/health message
`GET`    `/api/health`                 Health status
`GET`    `/api/bot/status`             Bot running state and summary
`POST`   `/api/bot/start`              Start bot using submitted configuration
`POST`   `/api/bot/stop`               Stop the bot
`GET`    `/api/bot/signals`            Current in-memory signals
`GET`    `/api/bot/trades`             Current active trades
`GET`    `/api/bot/stop-loss-events`   Current stop-loss events
`POST`   `/api/bot/config`             Accept/return bot configuration
The exact request and response fields are defined by the backend source.
Some data routes currently return in-memory bot state; persistent
history and complete dashboard analytics require the corresponding
database writes and read endpoints to be implemented and connected.
Database
PostgreSQL is used for persistent application data through SQLAlchemy
models and services.
The current database service includes operations for:
Saving signals.
Saving newly opened trades.
Updating a trade when it closes.
Saving stop-loss events.
Saving support/resistance levels.
A successful database insert confirms that the database connection and
insert path work; it does not by itself prove that every live bot event
is already wired to persistence. Verify the callbacks and database
service calls in the running bot integration.
For local development, create the PostgreSQL database and configure the
backend connection string before starting FastAPI. Database schema
creation/migrations should be managed deliberately as the project
evolves.
Logging and generated data
Depending on which services are enabled, the project can create files
such as:
`bot.log` --- main bot log, with rotating log files.
`fed_rate_state.json` --- latest processed Fed decision.
`fed_rate_history.json` --- Fed decision history.
`fed_rate_detector.log` --- Fed detector log.
`tech_index_data/state.json` --- last processed technology-index
candle.
`tech_index_data/stock_weights.csv` --- estimated stock weights.
`tech_index_data/tech_index_daily.csv` --- synthetic index data.
`tech_index_data/stock_daily_ohlc.csv` --- component stock data.
`tech_index_data/alerts.log` --- technology-index alerts.
Treat generated state and history files as runtime data. Decide
explicitly which files should be backed up or persisted, and do not
commit private or account-specific data.
Deployment notes
The intended cloud deployment architecture uses:
AWS EC2 for the backend and trading process.
PostgreSQL, preferably managed with Amazon RDS for production.
Nginx as a reverse proxy.
HTTPS for browser/API traffic.
A process manager such as `systemd` for long-running services.
AWS Secrets Manager or another secure secret store for credentials.
For a first deployment:
Provision the server and restrict SSH access.
Clone the private repository.
Create the Python environment and install dependencies.
Configure secrets on the server, not in Git.
Configure PostgreSQL connectivity and backups.
Start and verify the backend in paper mode.
Start the frontend behind the reverse proxy.
Verify logs, restart behavior, API access, and database persistence.
Only consider live mode after independent review and controlled
testing.
Do not run multiple bot instances against the same account and symbols
unless the system has explicit coordination and duplicate-order
protection.
Security checklist
[ ] Keep Delta API credentials out of Git and frontend code.
[ ] Use the minimum exchange API permissions required.
[ ] Keep paper mode enabled during development.
[ ] Restrict live API credentials and rotate them if exposed.
[ ] Use HTTPS outside local development.
[ ] Restrict database network access.
[ ] Use strong database credentials and backups.
[ ] Protect the bot start/stop endpoints with authentication before
public deployment.
[ ] Configure CORS for trusted frontend origins only.
[ ] Exclude `.env`, private keys, logs containing sensitive
information, virtual environments, caches, and temporary data from
Git.
[ ] Review order sizing, contract multipliers, stop-loss behavior,
and exchange responses before live use.
Current development status
TradePilot is under active development. The Python trading engine and
its companion monitoring modules are separate from the dashboard work.
The FastAPI backend, PostgreSQL persistence layer, and Next.js routes
are being connected incrementally.
The dashboard may contain placeholder pages or summary cards that are
not yet connected to live database-backed data. Before describing a
feature as production-ready, verify that:
The engine emits the event.
The backend receives it.
The database persists it when required.
The API returns it.
The frontend displays the returned data correctly.
Disclaimer
TradePilot is provided for software development and research purposes.
Cryptocurrency derivatives are high-risk products and can result in
rapid losses, including losses exceeding expectations due to leverage,
slippage, gaps, liquidity, exchange outages, API failures, or incorrect
configuration. Historical behavior does not predict future results.
You are solely responsible for exchange permissions, configuration,
testing, monitoring, and trading decisions. Always review the current
source code and exchange documentation before using live funds.
