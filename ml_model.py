# -*- coding: utf-8 -*-
"""
ml_model.py -- Whale Follower Bot
XGBoost model that learns from historical trade outcomes to filter low-quality signals.
Falls back gracefully if xgboost is not installed.
"""
from __future__ import annotations

import os
import pickle
import random
from pathlib import Path
from typing import Dict, List, Tuple

from loguru import logger

# ── Optional XGBoost import ───────────────────────────────────────────────────

try:
    import xgboost as xgb
    import numpy as np
    _XGBOOST_AVAILABLE = True
    logger.info("[ml_model] XGBoost available, ML filtering enabled")
except ImportError:
    _XGBOOST_AVAILABLE = False
    logger.warning("[ml_model] XGBoost not installed, ML filtering disabled (all signals pass)")

# ── Constants ─────────────────────────────────────────────────────────────────

_MODEL_PATH = Path(__file__).parent / "models" / "xgboost_model.pkl"
_BLOCK_THRESHOLD = 0.65
_RETRAIN_EVERY = 10
_RECENT_WINDOW = 50
_RECENT_WEIGHT = 3

_FEATURE_KEYS = [
    "cvd_velocity_3s",
    "cvd_velocity_10s",
    "cvd_velocity_30s",
    "cvd_acceleration",
    "volume_ratio",
    "cascade_intensity",
    "funding_rate",
    "oi_change_pct",
    "fear_greed_index",
    "session_multiplier",
    "orderbook_imbalance",
    "spring_drop_pct",
    "spring_bounce_pct",
    "price_vs_vwap",
    "hour_of_day",
]


def _features_to_array(features: Dict[str, float]):  # type: ignore[return]
    """Convert feature dict to numpy array in canonical order."""
    return np.array(
        [features.get(k, 0.0) for k in _FEATURE_KEYS], dtype=np.float32
    ).reshape(1, -1)


def _generate_warmstart_data() -> Tuple[list, list]:
    """
    Synthesize 51 trades reflecting the backtest distribution:
    33% winners (17), 67% losers (34).
    """
    rng = random.Random(42)
    X: List[List[float]] = []
    y: List[int] = []

    def _rand_winner() -> List[float]:
        return [
            rng.uniform(0.005, 0.030),   # cvd_velocity_3s
            rng.uniform(0.012, 0.040),   # cvd_velocity_10s  (>0.01)
            rng.uniform(0.010, 0.035),   # cvd_velocity_30s
            rng.uniform(0.001, 0.010),   # cvd_acceleration
            rng.uniform(1.6, 3.0),       # volume_ratio       (>1.5)
            rng.uniform(0.5, 1.0),       # cascade_intensity
            rng.uniform(-0.01, 0.02),    # funding_rate
            rng.uniform(0.005, 0.030),   # oi_change_pct
            rng.uniform(30.0, 70.0),     # fear_greed_index
            rng.uniform(1.0, 1.5),       # session_multiplier
            rng.uniform(0.60, 0.90),     # orderbook_imbalance
            rng.uniform(0.006, 0.015),   # spring_drop_pct    (>0.005)
            rng.uniform(0.004, 0.012),   # spring_bounce_pct
            rng.uniform(-0.002, 0.002),  # price_vs_vwap
            float(rng.randint(8, 20)),   # hour_of_day
        ]

    def _rand_loser() -> List[float]:
        return [
            rng.uniform(-0.005, 0.005),  # cvd_velocity_3s
            rng.uniform(-0.005, 0.010),  # cvd_velocity_10s  (<=0.01)
            rng.uniform(-0.010, 0.008),  # cvd_velocity_30s
            rng.uniform(-0.005, 0.001),  # cvd_acceleration
            rng.uniform(0.8, 1.4),       # volume_ratio       (<=1.5)
            rng.uniform(0.0, 0.4),       # cascade_intensity
            rng.uniform(-0.03, 0.01),    # funding_rate
            rng.uniform(-0.010, 0.005),  # oi_change_pct
            rng.uniform(20.0, 80.0),     # fear_greed_index
            rng.uniform(0.8, 1.2),       # session_multiplier
            rng.uniform(0.30, 0.59),     # orderbook_imbalance
            rng.uniform(0.001, 0.004),   # spring_drop_pct    (<=0.005)
            rng.uniform(0.001, 0.003),   # spring_bounce_pct
            rng.uniform(-0.010, 0.010),  # price_vs_vwap
            float(rng.randint(0, 23)),   # hour_of_day
        ]

    for _ in range(17):
        X.append(_rand_winner())
        y.append(1)

    for _ in range(34):
        X.append(_rand_loser())
        y.append(0)

    return X, y


class MLModel:
    """
    XGBoost classifier that predicts spring trade success probability.
    When xgboost is unavailable all methods return passive (non-blocking) values.
    """

    def __init__(self) -> None:
        self._model = None
        self._warmstart_X: List[List[float]] = []
        self._warmstart_y: List[int] = []
        self._new_samples: List[Tuple[List[float], int]] = []

        if not _XGBOOST_AVAILABLE:
            return

        _MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

        if _MODEL_PATH.exists():
            self._load_model()
        else:
            logger.info("[ml_model] No saved model found, training with warm-start data")
            self._train_warmstart()

    # ── Public API ─────────────────────────────────────────────────────────────

    def predict(self, features: Dict[str, float]) -> float:
        """
        Returns win probability 0.0-1.0.
        Returns 1.0 (pass all signals) if model is not available.
        """
        if not _XGBOOST_AVAILABLE or self._model is None:
            return 1.0

        try:
            arr = _features_to_array(features)
            prob = float(self._model.predict_proba(arr)[0][1])
            logger.debug("[ml_model] predict prob={:.3f}", prob)
            return prob
        except Exception as exc:
            logger.warning("[ml_model] predict error: {}", exc)
            return 1.0

    def should_block(self, features: Dict[str, float]) -> Tuple[bool, float]:
        """
        Returns (block, probability).
        block=True if probability < 0.65.
        """
        if not _XGBOOST_AVAILABLE or self._model is None:
            return (False, 1.0)

        prob = self.predict(features)
        block = prob < _BLOCK_THRESHOLD
        if block:
            logger.info("[ml_model] Blocking signal, prob={:.3f} < {}", prob, _BLOCK_THRESHOLD)
        return (block, prob)

    def record_outcome(self, features: Dict[str, float], won: bool) -> None:
        """
        Record a real trade outcome and retrain when 10 new samples accumulate.
        """
        if not _XGBOOST_AVAILABLE:
            return

        row = [features.get(k, 0.0) for k in _FEATURE_KEYS]
        self._new_samples.append((row, 1 if won else 0))
        logger.debug(
            "[ml_model] Recorded outcome won={} total_new_samples={}", won, len(self._new_samples)
        )

        if len(self._new_samples) >= _RETRAIN_EVERY:
            self._retrain()

    # ── Internal Logic ─────────────────────────────────────────────────────────

    def _train_warmstart(self) -> None:
        X, y = _generate_warmstart_data()
        self._warmstart_X = X
        self._warmstart_y = y
        self._fit(X, y)
        self._save_model()

    def _retrain(self) -> None:
        logger.info(
            "[ml_model] Retraining with {} warm-start + {} new samples",
            len(self._warmstart_X), len(self._new_samples),
        )
        base_X = list(self._warmstart_X) + [s[0] for s in self._new_samples]
        base_y = list(self._warmstart_y) + [s[1] for s in self._new_samples]

        recent = self._new_samples[-_RECENT_WINDOW:]
        if recent:
            base_X += [s[0] for s in recent] * (_RECENT_WEIGHT - 1)
            base_y += [s[1] for s in recent] * (_RECENT_WEIGHT - 1)

        self._fit(base_X, base_y)
        self._save_model()
        self._new_samples.clear()

    def _fit(self, X: List[List[float]], y: List[int]) -> None:
        try:
            arr_X = np.array(X, dtype=np.float32)
            arr_y = np.array(y, dtype=np.int32)
            self._model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=42,
                verbosity=0,
            )
            self._model.fit(arr_X, arr_y)
            logger.info("[ml_model] Model trained on {} samples", len(y))
        except Exception as exc:
            logger.warning("[ml_model] Training error: {}", exc)
            self._model = None

    def _save_model(self) -> None:
        try:
            with open(_MODEL_PATH, "wb") as fh:
                pickle.dump(self._model, fh)
            logger.info("[ml_model] Model saved to {}", _MODEL_PATH)
        except Exception as exc:
            logger.warning("[ml_model] Could not save model: {}", exc)

    def _load_model(self) -> None:
        try:
            with open(_MODEL_PATH, "rb") as fh:
                self._model = pickle.load(fh)
            logger.info("[ml_model] Loaded model from {}", _MODEL_PATH)
        except Exception as exc:
            logger.warning("[ml_model] Could not load model, retraining: {}", exc)
            self._train_warmstart()
