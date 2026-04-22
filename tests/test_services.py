import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from macro_stress_monitor.services.delta import compute_preview_delta, compute_structural_delta
from macro_stress_monitor.services.health import freshness_for_snapshot
from macro_stress_monitor.services.market_context import build_market_context


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


if __name__ == "__main__":
    unittest.main()
