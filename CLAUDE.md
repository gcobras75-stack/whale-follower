# Whale Follower Bot — Contexto completo para Claude Code

## Qué es este proyecto
Bot de trading algorítmico de criptomonedas que opera en Bybit con dinero real ($90 USD).
Corre en Railway (cloud), persiste trades en Supabase, envía alertas por Telegram.

## Stack
- **Python 3.11** + asyncio (todo el bot es un solo proceso async)
- **Railway** — hosting cloud, región asia-southeast1 (Singapore), auto-deploy desde GitHub
- **Supabase** — PostgreSQL para persistencia de trades
- **Bybit API v5** — ejecución de órdenes reales (PRODUCTION=true)
- **Telegram Bot** — alertas y comandos /status /trades

## Estado actual del bot (Abril 2026)

### Modo de operación
- `PRODUCTION=true` → órdenes reales en Bybit con $90 USD
- `REAL_CAPITAL=75` (config base), balance real ~$90.64 en Bybit Unified Trading Account
- `SIGNAL_SCORE_THRESHOLD=72` (threshold dinámico por régimen de mercado)
- `PREFERRED_PAIR=ETHUSDT` (orden mínima ~$20 vs BTC ~$67)
- `MAX_LEVERAGE=1` (sin apalancamiento al inicio)
- `RISK_PER_TRADE=0.01` (1% capital, ajustado dinámicamente por Kelly Criterion)

### Trades reales ejecutados
- **Wyckoff Spring**: 0 trades reales (mercado lateral desde activación, BTC ~$67K, ETH ~$2K)
- **Paper trades** (simulación): 350+ trades, PnL +$9.87, WR 66%, duración ~28s promedio
- Todas las estrategias excepto Wyckoff Spring operan en papel

## Arquitectura del bot

### Flujo principal (main.py)
```
aggregator.stream() → tick de precio
  ├─ 1. update_trades() — gestionar trades abiertos
  ├─ 2. actualizar motores de datos (CVD, correlación, grid, OFI, etc.)
  │   ├─ regime_det.on_price()    ← NUEVO: detecta régimen
  │   └─ range_trader.on_price()  ← NUEVO: opera en lateral sin spring
  ├─ 3. monitor.process() → Wyckoff Spring detection
  │   └─ if signal is None: continue  (la mayoría de ticks)
  ├─ 4. construir ExtendedContext (14 capas)
  │   ├─ CVD combinado, Liquidation Map, Fear&Greed, News Filter
  │   ├─ On-Chain, Order Book, Correlación BTC-ETH, Session Volume
  │   ├─ Deribit Options (PCR + IV + sweeps)  ← NUEVO
  │   └─ Macro Agent (pausa si evento macro inminente) ← NUEVO
  ├─ 5. ScoringEngine.score() → 0-100
  │   ├─ 5b. macro sentiment adj (-20 a +15)
  │   └─ 5c. ML XGBoost filter
  ├─ 6. Threshold dinámico = 72 + regime_adj
  │   └─ LATERAL→62 / TRENDING_UP→72 / TRENDING_DOWN→87 / HIGH_VOL→92
  └─ 7. executor.open_trade() → Bybit REAL si pasa todo
```

### Módulos implementados (por sprint)

#### Core (siempre activos)
| Archivo | Descripción |
|---|---|
| `aggregator.py` | WebSocket multi-exchange (Bybit, OKX, Kraken) |
| `spring_detector.py` | Detección Wyckoff Spring (caída brusca + rebote) |
| `cascade_detector.py` | Stop cascade detection |
| `cvd_real.py` | CVD velocity 3s/10s/30s + aceleración |
| `bybit_executor.py` | Ejecución órdenes Bybit (testnet/real) + Kelly Criterion |
| `context_engine.py` | Funding rate, Open Interest, sesión de mercado |
| `scoring_engine.py` | Score 0-100 con 14 capas, ExtendedContext |
| `multi_pair.py` | Monitor BTC/ETH/SOL/BNB simultáneo |
| `config.py` | Variables de entorno centralizadas |

#### Sprint 4 (análisis avanzado)
| Archivo | Descripción |
|---|---|
| `cvd_combined.py` | CVD de 3 exchanges simultáneos |
| `liquidation_map.py` | Mapa de liquidaciones OKX |
| `fear_greed.py` | Fear & Greed Index (alternative.me) |
| `news_filter.py` | NewsAPI keyword filter |
| `onchain.py` | Whale Alert — inflows/outflows exchanges |
| `orderbook.py` | Order book imbalance multi-nivel |
| `correlation.py` | Correlación BTC-ETH en tiempo real |
| `session_volume.py` | Volumen histórico por sesión |
| `ml_model.py` | XGBoost — último filtro antes de ejecutar |
| `dashboard.py` | Reporte diario Telegram |

#### Sprint 6 — Estrategias avanzadas
| Archivo | Descripción |
|---|---|
| `arb_engine.py` | Orquestador de 4 estrategias arb |
| `cross_exchange_arb.py` | Arb Bybit↔OKX (detecta, no ejecuta — falta OKX API) |
| `triangular_arb.py` | Arb triangular BTC/ETH/USDT |
| `lead_lag_arb.py` | Lead-lag BTC→ETH |
| `funding_arb.py` | Funding rate arbitrage |
| `mean_reversion.py` | Mean reversion con cascade |
| `grid_trading.py` | Grid trading con BB dinámico |
| `ofi_strategy.py` | Order Flow Imbalance (paper only — fees negativos con real) |
| `momentum_scaling.py` | Momentum scaling multi-tramo |
| `delta_neutral.py` | Delta neutral funding capture |
| `db_writer.py` | Escritores Supabase para todas las estrategias |

#### Sprint 6 — Nuevos módulos (Abril 2026)
| Archivo | Descripción |
|---|---|
| `macro_agent.py` | Macrofundamentales: Forex Factory + CryptoPanic + Reddit + X/Nitter |
| `market_regime.py` | Detector régimen: LATERAL/TRENDING_UP/TRENDING_DOWN/HIGH_VOL |
| `range_trader.py` | RSI+BB Bounce — opera en lateral sin spring |
| `deribit_options.py` | Options flow: PCR + IV spike + sweeps (Deribit gratis) |

### Tablas Supabase
| Tabla | Contenido |
|---|---|
| `paper_trades` | Historial unificado Wyckoff Spring (estrategia="wyckoff") |
| `ofi_trades` | OFI strategy trades |
| `grid_cycles` | Grid trading ciclos buy/sell |
| `mr_trades` | Mean reversion trades |
| `momentum_trades` | Momentum scaling trades |
| `delta_neutral_trades` | Delta neutral positions |
| `range_trades` | Range trader (RSI+BB) trades |

## Protecciones de capital (producción)
- `DAILY_LOSS_LIMIT=0.05` — para si pierde >5% del capital en el día
- `MAX_TRADES_OPEN=2` — máximo 2 trades abiertos simultáneos
- `MAX_ORDER_PCT_CAPITAL=0.35` — si orden mínima > 35% capital → cambia a ETHUSDT
- `MAX_LEVERAGE=1` — sin apalancamiento hasta acumular historial
- Kelly Criterion — ajusta tamaño dinámicamente (0.5x-2.5x del 1% base)
- Gestión activa: breakeven en 1x riesgo, parciales en 1.5x, trailing en 2x

## Variables de entorno críticas (Railway)
```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=5985169283
SUPABASE_URL=https://dunlkxbcsxzwnencrklx.supabase.co
SUPABASE_KEY=...
BYBIT_API_KEY=FvuTfWOpEDEIf6lELK
BYBIT_API_SECRET=...
PRODUCTION=true
REAL_CAPITAL=75
SIGNAL_SCORE_THRESHOLD=72
PREFERRED_PAIR=ETHUSDT
MAX_ORDER_PCT_CAPITAL=0.35
MAX_LEVERAGE=1
MAX_TRADES_OPEN=2
RISK_PER_TRADE=0.01
DAILY_LOSS_LIMIT=0.05
NEWS_API_KEY=eb7d21ed65a64d1ba6b98a84fd920986
WHALE_ALERT_KEY=mbWgj1MgRCl2YLgIQLWXnWUlf1V5XLlc
```

## Problemas conocidos y resueltos
- **Bug crítico resuelto**: `_place_order` tenía `symbol: "BTCUSDT"` hardcodeado → corregido a `trade.pair`
- **OFI con dinero real**: EV negativo (fees 0.11% RT > TP promedio 0.25%). OFI solo paper.
- **BTC orden mínima**: 0.001 BTC = ~$67 con BTC a $67K = 74% del capital. Solucionado con auto-sustitución a ETHUSDT.
- **Railway token expirado**: el token del config.json expira. Para logs usar `railway logs --tail 50` en terminal.
- **Cross-arb sin OKX**: detecta spreads Bybit↔OKX pero no ejecuta (usuario no tiene cuenta OKX).

## Cómo deployar
```bash
git add . && git commit -m "descripción" && git push origin main
# Railway auto-deploya desde GitHub push
# Si no: railway dashboard → proyecto → Redeploy
```

## Cómo ver logs en vivo
```bash
railway logs --tail 100
```

## Cómo ver trades en Supabase
```sql
-- Estado actual
SELECT strategy, pair, COUNT(*), 
  ROUND(SUM(pnl_usd) FILTER (WHERE status='closed')::numeric,4) as pnl,
  ROUND(100.0 * COUNT(*) FILTER (WHERE pnl_usd>0 AND status='closed') 
    / NULLIF(COUNT(*) FILTER (WHERE status='closed'),0),1) as wr
FROM paper_trades GROUP BY strategy, pair;

-- Trades abiertos
SELECT strategy, pair, side, entry_price, size_usd, created_at
FROM paper_trades WHERE status='open' ORDER BY created_at DESC;

-- PnL por hora hoy
SELECT DATE_TRUNC('hour', created_at) as hora,
  COUNT(*) FILTER (WHERE status='closed') as trades,
  ROUND(SUM(pnl_usd) FILTER (WHERE status='closed')::numeric,4) as pnl
FROM paper_trades WHERE created_at >= NOW() - INTERVAL '12 hours'
GROUP BY hora ORDER BY hora;
```

## Próximos pasos pendientes
- [ ] Activar OKX API para cross-arb real (usuario no tiene cuenta OKX aún)
- [ ] Primer trade real Wyckoff Spring en Bybit (mercado muy lateral)
- [ ] Considerar Kraken API como alternativa a OKX para cross-arb
- [ ] Bybit Spot vs Perp arb (no necesita segunda exchange)
- [ ] Cuando haya historial de 10+ trades reales: Kelly se auto-calibra

## Contacto del proyecto
- GitHub: `gcobras75-stack/whale-follower`
- Railway project: `gracious-consideration` (service ID: f36a79a5-3152-4689-a4b3-9c06df04778d)
- Supabase project: `dunlkxbcsxzwnencrklx`
- Telegram chat: `5985169283`
