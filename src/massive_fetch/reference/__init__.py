"""Reference-data subpackage (SPEC §5, §10.3).

Static universe definitions and their freshness, kept separate from OHLCV
ingestion. Slice 4 ships the stocks universe (Wikipedia SP500 ∪ NDX scrape with
a frozen-YAML fallback) and a universe-freshness helper; futures contract
discovery (§5.2) and the market calendar (§5) arrive in later slices.
"""
