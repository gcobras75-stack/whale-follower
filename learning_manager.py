from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple

import config
from loguru import logger


@dataclass
class LearningStatus:
    learning_mode: bool
    trades_total: int
    window_size: int
    win_rate_pct: float
    score_threshold: int


class LearningManager:
    def __init__(self) -> None:
        self._threshold: int = int(getattr(config, "SIGNAL_SCORE_THRESHOLD", 65))
        self._outcomes: Deque[bool] = deque(maxlen=200)
        self._trades_total: int = 0
        self._learning_mode: bool = True
        self._announced_learning: bool = False
        self._announced_completed: bool = False

    def get_threshold(self) -> int:
        return int(self._threshold)

    def status(self) -> LearningStatus:
        wr, n = self._win_rate_pct(window=50)
        return LearningStatus(
            learning_mode=self._learning_mode,
            trades_total=self._trades_total,
            window_size=n,
            win_rate_pct=wr,
            score_threshold=int(self._threshold),
        )

    def ensure_announced(self) -> None:
        if not self._learning_mode or self._announced_learning:
            return
        self._announced_learning = True
        try:
            from alerts import send_system_message

            msg = (
                "🧠 MODO APRENDIZAJE ACTIVADO\n"
                f"Score threshold: {self._threshold}\n"
                "Objetivo: acumular 50 trades reales\n"
                "Win rate mínimo aceptado: 45%\n"
                "El sistema se auto-optimizará después"
            )
            asyncio.create_task(send_system_message(msg))
        except Exception as exc:
            logger.warning("[learning] could not announce learning mode: {}", exc)

    def record_trade_close(self, won: bool, ml_accuracy: Optional[float] = None) -> LearningStatus:
        self._trades_total += 1
        self._outcomes.append(bool(won))

        self.ensure_announced()

        if self._trades_total % 20 == 0:
            win_rate_pct, n = self._win_rate_pct(window=20)
            if n >= 20:
                self._threshold = self._threshold_from_win_rate(win_rate_pct)
                logger.info(
                    "[learning] auto-threshold trades={} win_rate_20={:.1f}% -> threshold={} ",
                    self._trades_total,
                    win_rate_pct,
                    self._threshold,
                )
                self._save_learning_history(win_rate_pct=win_rate_pct, ml_accuracy=ml_accuracy)

        if self._learning_mode and self._trades_total >= 50:
            wr50, n50 = self._win_rate_pct(window=50)
            if n50 >= 50 and wr50 >= 55.0:
                self._learning_mode = False
                if not self._announced_completed:
                    self._announced_completed = True
                    try:
                        from alerts import send_system_message

                        msg = (
                            "🎓 APRENDIZAJE COMPLETADO\n"
                            f"Trades reales: {self._trades_total}\n"
                            f"Win rate: {wr50:.1f}%\n"
                            f"Score optimizado: {self._threshold}\n"
                            "Cambiando a modo rendimiento"
                        )
                        asyncio.create_task(send_system_message(msg))
                    except Exception as exc:
                        logger.warning("[learning] could not announce completion: {}", exc)

        return self.status()

    def _win_rate_pct(self, window: int) -> Tuple[float, int]:
        if not self._outcomes:
            return 0.0, 0
        items = list(self._outcomes)[-window:]
        if not items:
            return 0.0, 0
        return (sum(1 for x in items if x) / len(items) * 100.0, len(items))

    @staticmethod
    def _threshold_from_win_rate(win_rate_pct: float) -> int:
        if win_rate_pct < 45.0:
            return 80
        if win_rate_pct < 50.0:
            return 75
        if win_rate_pct < 55.0:
            return 72
        if win_rate_pct < 60.0:
            return 68
        if win_rate_pct < 65.0:
            return 65
        if win_rate_pct < 70.0:
            return 62
        return 60

    def _save_learning_history(self, win_rate_pct: float, ml_accuracy: Optional[float]) -> None:
        try:
            from db_writer import save_learning_history

            asyncio.create_task(
                save_learning_history(
                    trade_number=self._trades_total,
                    score_threshold_at_time=int(self._threshold),
                    win_rate_at_time=float(win_rate_pct),
                    ml_accuracy=float(ml_accuracy) if ml_accuracy is not None else None,
                    timestamp=time.time(),
                )
            )
        except Exception as exc:
            logger.warning("[learning] could not save learning_history: {}", exc)


_manager = LearningManager()


def get_manager() -> LearningManager:
    return _manager


def get_threshold() -> int:
    return _manager.get_threshold()
