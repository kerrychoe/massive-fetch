# massive-fetch

Fetch historical market data from [Massive.com](https://massive.com) (REST API &
flat files) and store it locally as Parquet for downstream backtesting and
research. See [`SPEC.md`](SPEC.md) for the full design contract.

## ⚠️ Survivorship Bias Disclosure

> This tool's default stocks universe is "today's S&P 500 ∪ Nasdaq-100". Companies
> that were members of these indexes historically but have since been delisted,
> acquired, or removed are NOT included. Any backtest run on this dataset will be
> subject to survivorship bias — historical strategy performance will appear better
> than reality, particularly for long-biased and mean-reversion strategies. This is
> a known limitation of the v1 universe definition. To eliminate this bias, see
> Phase 2 plans for full Flat Files ingestion or point-in-time index membership.

The universe is rebuilt from a live Wikipedia scrape on `massive-fetch reference
update` (with a committed frozen-YAML fallback), so it always reflects *current*
membership — never point-in-time. `massive-fetch status` surfaces how stale the
stored universe is (SPEC §10.3).
