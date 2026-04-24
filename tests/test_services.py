import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from macro_stress_monitor.services.dashboard_config import get_quote_symbols
from macro_stress_monitor.services.delta import compute_preview_delta, compute_structural_delta
from macro_stress_monitor.services.health import freshness_for_snapshot
from macro_stress_monitor.services.landing import build_landing_payload
from macro_stress_monitor.services.market_context import build_market_context
from macro_stress_monitor.services.quotes import build_intraday_payload, build_quote_payload


class DeltaTests(unittest.TestCase):
    def test_structural_delta_reports_score_regime_and_component_changes(self):
        previous = {
            "run_id": "structural_old",
            "computed_at_utc": "2026-04-21T22:30:00+00:00",
            "composite_score": 40,
            "regime_label": "WATCH",
            "primary_drivers": ["Financials"],
            "component_subscores": {"xlf_spy": 45, "jpy_risk": 20},
            "component_states": {
                "xlf_spy": {"level": "watch"},
                "jpy_risk": {"level": "stable"},
            },
        }
        current = {
            "run_id": "structural_new",
            "computed_at_utc": "2026-04-22T22:30:00+00:00",
            "composite_score": 67,
            "regime_label": "ELEVATED",
            "primary_drivers": ["Financials", "JPY Risk"],
            "component_subscores": {"xlf_spy": 70, "jpy_risk": 55},
            "component_states": {
                "xlf_spy": {"level": "watch"},
                "jpy_risk": {"level": "watch"},
            },
        }

        delta = compute_structural_delta(current, previous)

        self.assertTrue(delta["available"])
        self.assertEqual(delta["score_change"], 27.0)
        self.assertTrue(delta["regime_changed"])
        self.assertEqual(delta["component_state_changes"]["jpy_risk"], {"from": "stable", "to": "watch"})
        self.assertEqual(delta["primary_drivers_added"], ["JPY Risk"])

    def test_preview_delta_reports_assessment_change(self):
        previous = {
            "run_id": "preview_old",
            "preview_spillover_assessment": "Contained to equities",
            "component_states": {"xlf_spy": {"level": "stable"}},
            "component_subscores": {"xlf_spy": 20},
        }
        current = {
            "run_id": "preview_new",
            "preview_spillover_assessment": "Early spillover: watch credit",
            "component_states": {"xlf_spy": {"level": "watch"}},
            "component_subscores": {"xlf_spy": 45},
        }

        delta = compute_preview_delta(current, previous)

        self.assertTrue(delta["assessment_changed"])
        self.assertEqual(delta["component_state_changes"]["xlf_spy"], {"from": "stable", "to": "watch"})
        self.assertEqual(delta["component_subscore_changes"]["xlf_spy"], 25.0)


class FreshnessTests(unittest.TestCase):
    def test_freshness_marks_old_structural_snapshot_stale(self):
        now = datetime(2026, 4, 22, 18, 0, tzinfo=timezone.utc)
        snapshot = {
            "computed_at_utc": (now - timedelta(hours=40)).isoformat(),
        }

        freshness = freshness_for_snapshot(snapshot, "structural", now=now)

        self.assertTrue(freshness["is_stale"])
        self.assertEqual(freshness["age_seconds"], 40 * 60 * 60)

    def test_freshness_marks_recent_preview_snapshot_fresh(self):
        now = datetime(2026, 4, 22, 18, 0, tzinfo=timezone.utc)
        snapshot = {
            "computed_at_utc": (now - timedelta(hours=2)).isoformat(),
        }

        freshness = freshness_for_snapshot(snapshot, "preview", now=now)

        self.assertFalse(freshness["is_stale"])
        self.assertIsNone(freshness["stale_reason"])


class MarketContextTests(unittest.TestCase):
    def test_market_context_builds_expected_sections(self):
        index = pd.bdate_range("2025-01-01", periods=320)

        def series(base, step):
            return pd.Series([base + i * step for i in range(len(index))], index=index, dtype=float)

        def frame(base, step):
            values = series(base, step)
            return pd.DataFrame(
                {
                    "Open": values,
                    "High": values,
                    "Low": values,
                    "Close": values,
                    "Volume": 1000.0,
                },
                index=index,
            )

        fred_data = {
            "BAMLC0A0CM": series(1.2, 0.001),
            "BAMLH0A0HYM2": series(4.2, 0.002),
            "DGS2": series(4.6, -0.001),
            "DGS5": series(4.4, -0.0007),
            "DGS10": series(4.2, -0.0005),
            "DGS30": series(4.1, -0.0002),
            "T10YIE": series(2.3, -0.0001),
            "UNRATE": series(4.0, 0.0001),
            "PAYEMS": series(158000, 2.0),
        }
        tickers = [
            "SPY", "QQQ", "IWM", "DIA", "RSP", "LQD", "HYG", "IEF", "SHY", "TLT",
            "BKLN", "XLF", "KRE", "XLK", "XLI", "XLE", "XLU", "XLP", "XLY", "XLV",
            "SMH", "GLD", "UUP", "VIXY", "JPY=X",
        ]
        market_data = {ticker: frame(100 + i, 0.05 + i * 0.001) for i, ticker in enumerate(tickers)}

        context = build_market_context(
            fred_data=fred_data,
            market_data=market_data,
            hy_ig_gap={"hy_ig_oas_gap": 3.0, "hy_ig_oas_gap_method": "test"},
        )

        self.assertIn("macro_rates", context)
        self.assertIn("credit_liquidity", context)
        self.assertIn("equity_index_state", context)
        self.assertIn("sector_state", context)
        self.assertIn("volatility_stress", context)
        self.assertIn("flight_to_safety", context)
        self.assertIn("cross_asset_relationships", context)
        self.assertIn("breadth_participation", context)
        self.assertIn("positioning_stretch", context)
        self.assertTrue(context["equity_index_state"]["SPY"]["available"])
        self.assertTrue(context["cross_asset_relationships"]["qqq_spy"]["available"])
        self.assertEqual(context["breadth_participation"]["method"], "tracked_etf_proxy")


class LandingPayloadTests(unittest.TestCase):
    def test_landing_payload_uses_structural_context_as_primary_source(self):
        structural_snapshot = {
            "composite_score": 64,
            "regime_label": "ELEVATED",
            "headline_summary": "Credit is weakening while financials remain under pressure.",
            "computed_at_utc": "2026-04-23T13:15:00+00:00",
            "confidence": "HIGH",
            "primary_drivers": ["High Yield Credit", "Financials"],
            "trigger_states": {
                "ig_widening": True,
                "hy_widening": True,
                "loans_below_200dma": False,
                "jpy_confirmed": False,
                "xlf_breakdown": True,
                "dgs30_sharp_move": False,
            },
            "data_quality": {"score": 0.91},
            "market_context": {
                "macro_rates": {
                    "rates": {
                        "2y": {"available": True, "latest": 4.75, "change_20d_bps": 14, "z_score_1y": 0.8, "state": "rising_fast"},
                        "10y": {"available": True, "latest": 4.35, "change_20d_bps": 8, "z_score_1y": 0.4, "state": "range_bound"},
                        "30y": {"available": True, "latest": 4.81, "change_20d_bps": 5, "z_score_1y": 0.5, "state": "range_bound"},
                    },
                    "curve_spreads": {
                        "2s10s": {"available": True, "latest_bps": -40, "change_20d_bps": 6, "z_score_1y": 0.1, "state": "inverted"},
                    },
                    "inflation_growth": {
                        "10y_breakeven_inflation": {"available": True, "latest": 2.31, "change_20d_bps": 2, "z_score_1y": -0.2, "state": "range_bound"},
                        "unemployment_rate": {"available": True, "latest": 4.1, "change_60d": 0.1, "percentile_1y": 60, "state": "rising"},
                        "nonfarm_payrolls": {"available": True, "latest": 158500, "change_60d": 22, "percentile_1y": 52, "state": "rising"},
                    },
                    "dollar_proxy": {
                        "state": {
                            "available": True,
                            "latest": 29.4,
                            "return_20d": 0.012,
                            "distance_50dma": 0.01,
                            "trend_state": "uptrend",
                            "vol_state": "normal_vol",
                        },
                    },
                },
                "credit_liquidity": {
                    "ig_oas": {"available": True, "latest": 1.41, "change_20d_bps": 9, "z_score_1y": 1.2, "state": "rising_fast"},
                    "hy_oas": {"available": True, "latest": 4.62, "change_20d_bps": 21, "z_score_1y": 1.5, "state": "rising_fast"},
                    "hy_minus_ig_gap": {"hy_ig_oas_gap": 3.21, "hy_ig_oas_gap_method": "snapshot"},
                    "loan_proxy": {
                        "available": True,
                        "latest": 20.2,
                        "return_20d": -0.018,
                        "distance_50dma": -0.012,
                        "trend_state": "downtrend",
                        "vol_state": "elevated_vol",
                    },
                },
                "flight_to_safety": {
                    "treasury_proxies": {
                        "TLT": {"available": True, "latest": 92.5, "return_20d": 0.023, "distance_50dma": 0.011, "trend_state": "uptrend", "vol_state": "normal_vol"},
                        "IEF": {"available": True, "latest": 95.3, "return_20d": 0.008, "distance_50dma": 0.004, "trend_state": "uptrend", "vol_state": "normal_vol"},
                    },
                    "gold_proxy": {"available": True, "latest": 221.5, "return_20d": 0.017, "distance_50dma": 0.02, "trend_state": "uptrend", "vol_state": "normal_vol"},
                    "dollar_proxy": {"available": True, "latest": 29.4, "return_20d": 0.012, "distance_50dma": 0.01, "trend_state": "uptrend", "vol_state": "normal_vol"},
                    "jpy_proxy": {"available": True, "latest": 145.1, "return_20d": -0.016, "distance_50dma": -0.01, "trend_state": "downtrend", "vol_state": "elevated_vol"},
                    "defensive_vs_cyclical": {
                        "xlu_xly": {"available": True, "return_20d": 0.021, "distance_200dma": 0.03, "z_score_1y": 1.1, "state": "outperforming"},
                        "xlp_xly": {"available": True, "return_20d": 0.016, "distance_200dma": 0.02, "z_score_1y": 0.8, "state": "outperforming"},
                        "gld_spy": {"available": True, "return_20d": 0.01, "distance_200dma": 0.01, "z_score_1y": 0.5, "state": "outperforming"},
                    },
                },
                "breadth_participation": {
                    "above_50dma_pct": 0.42,
                    "above_50dma_count": 8,
                    "above_200dma_pct": 0.58,
                    "above_200dma_count": 11,
                    "positive_20d_trend_pct": 0.47,
                    "positive_20d_trend_count": 9,
                    "advancers_1d_count": 10,
                    "advancers_5d_count": 8,
                    "tracked_count": 19,
                    "equal_weight_vs_cap_weight": {"available": True, "return_20d": -0.013, "distance_200dma": -0.015, "z_score_1y": -0.7, "state": "underperforming"},
                },
                "sector_state": {
                    "XLU": {
                        "absolute": {"return_20d": 0.03},
                        "relative_to_spy": {"return_20d": 0.022, "state": "outperforming"},
                    },
                    "XLF": {
                        "absolute": {"return_20d": -0.025},
                        "relative_to_spy": {"return_20d": -0.031, "state": "underperforming"},
                    },
                    "XLK": {
                        "absolute": {"return_20d": 0.015},
                        "relative_to_spy": {"return_20d": 0.01, "state": "outperforming"},
                    },
                },
                "volatility_stress": {
                    "vix_proxy": {
                        "state": {"available": True, "latest": 16.8, "return_20d": 0.062, "distance_50dma": 0.04, "trend_state": "uptrend", "vol_state": "high_vol"},
                    },
                    "move_proxy": {
                        "state": {"available": True, "latest": 89.1, "return_20d": 0.014, "distance_50dma": 0.01, "trend_state": "uptrend", "vol_state": "elevated_vol"},
                    },
                },
                "cross_asset_relationships": {
                    "xlf_spy": {"available": True, "return_20d": -0.031, "distance_200dma": -0.027, "z_score_1y": -1.2, "state": "underperforming"},
                    "kre_spy": {"available": True, "return_20d": -0.042, "distance_200dma": -0.034, "z_score_1y": -1.4, "state": "underperforming"},
                },
            },
        }
        preview_snapshot = {
            "preview_spillover_assessment": "Early spillover: watch credit",
            "session_context": {"market_session": "regular"},
            "component_states": {
                "xlf_spy": {"level": "watch"},
                "leveraged_loans": {"level": "stable"},
                "jpy_risk": {"level": "watch"},
                "kre_spy": {"level": "watch"},
            },
        }
        history = [
            {"date": "2026-04-23", "composite_score": 64},
            {"date": "2026-04-22", "composite_score": 58},
            {"date": "2026-04-21", "composite_score": 54},
        ]

        payload = build_landing_payload(structural_snapshot, preview_snapshot, history)

        self.assertEqual(payload["hero"]["score"], 64)
        self.assertEqual(payload["hero"]["preview"]["assessment"], "Early spillover: watch credit")
        self.assertEqual([tab["id"] for tab in payload["tabs"]], ["macro", "stress", "safety", "breadth"])
        self.assertEqual(len(payload["trigger_map"]), 6)
        self.assertEqual(payload["leadership"]["leaders"][0]["label"], "Utilities")
        self.assertTrue(payload["tabs"][1]["cards"][0]["label"].startswith("IG"))


class DashboardConfigTests(unittest.TestCase):
    def test_quote_symbols_merge_watchlist_and_baskets_without_duplicates(self):
        config = {
            "quote_watchlist": ["spy", "qqq", "SPY"],
            "baskets": [
                {"name": "growth", "stocks": [{"ticker": "NVDA"}, {"ticker": "QQQ"}]},
                {"name": "macro", "stocks": [{"ticker": "TLT"}]},
            ],
        }

        symbols = get_quote_symbols(config)

        self.assertEqual(symbols, ["SPY", "QQQ", "NVDA", "TLT"])


class QuotePayloadTests(unittest.TestCase):
    def test_quote_payload_prefers_intraday_session_and_computes_performance(self):
        daily_index = pd.bdate_range("2026-04-15", periods=8)
        daily = pd.DataFrame(
            {
                "Open": [100, 101, 102, 103, 104, 105, 106, 107],
                "High": [101, 102, 103, 104, 105, 106, 108, 111],
                "Low": [99, 100, 101, 102, 103, 104, 105, 106],
                "Close": [100, 102, 103, 104, 105, 106, 107, 109],
                "Volume": [1000] * 8,
            },
            index=daily_index,
        )
        intraday_index = pd.date_range("2026-04-24 13:30:00", periods=4, freq="1min")
        intraday = pd.DataFrame(
            {
                "Open": [108.5, 109.0, 109.6, 110.0],
                "High": [109.1, 109.8, 110.4, 110.8],
                "Low": [108.4, 108.9, 109.5, 109.9],
                "Close": [109.0, 109.7, 110.1, 110.5],
                "Volume": [120, 150, 180, 210],
            },
            index=intraday_index,
        )

        payload = build_quote_payload("spy", daily, intraday)

        self.assertEqual(payload["symbol"], "SPY")
        self.assertEqual(payload["price"], 110.5)
        self.assertEqual(payload["open"], 108.5)
        self.assertEqual(payload["previous_close"], 107.0)
        self.assertEqual(payload["volume"], 660)
        self.assertAlmostEqual(payload["performance"]["1d"], 3.27, places=2)

    def test_intraday_payload_serializes_points(self):
        intraday_index = pd.date_range("2026-04-24 13:30:00", periods=2, freq="1min")
        intraday = pd.DataFrame(
            {
                "Open": [10.0, 10.2],
                "High": [10.3, 10.4],
                "Low": [9.9, 10.1],
                "Close": [10.2, 10.35],
                "Volume": [100, 110],
            },
            index=intraday_index,
        )

        payload = build_intraday_payload("qqq", intraday)

        self.assertEqual(payload["symbol"], "QQQ")
        self.assertEqual(len(payload["points"]), 2)
        self.assertEqual(payload["points"][1]["close"], 10.35)


if __name__ == "__main__":
    unittest.main()
