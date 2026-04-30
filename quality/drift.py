"""7-day-MAD drift detection over `quality/history.db` measurements.

For each (check_name, court, metric) we compute the median absolute
deviation over the last 7 days. A new value outside median ± k×MAD is
flagged as drift. MAD is robust to single-day outliers (unlike σ),
which makes it a good default for daily corpus metrics.

Caught classes:
- SG anomaly: row count drops 1450 in one nightly → MAD = 0 over the
  prior 7 stable days, k×MAD = 0, any change > 0 trips. Handled by
  `_min_threshold`: if MAD is too tight, fall back to absolute floor.
- OCR-quality drop on a court: avg garbage_score doubles → flagged.
- Courtless rows appear (registry break): flagged.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass

from quality.baseline import historical_values

K_MAD = 5.0          # number of MAD widths outside which we flag
MIN_BAND_FRACTION = 0.05  # absolute fallback band when MAD is degenerate


@dataclass
class DriftBand:
    median: float
    mad: float
    lower: float
    upper: float
    n_samples: int


def compute_band(values: list[float], k: float = K_MAD) -> DriftBand | None:
    """Compute the (median ± k×MAD) band over a list of values.

    Returns None when fewer than 3 samples are available — drift
    detection requires a population.
    """
    if len(values) < 3:
        return None
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    # Floor the band width: if MAD is degenerate (e.g. constant series),
    # use 5% of the median as the absolute tolerance.
    width = max(k * mad, abs(median) * MIN_BAND_FRACTION)
    return DriftBand(
        median=median, mad=mad,
        lower=median - width, upper=median + width,
        n_samples=len(values),
    )


def detect(
    check_name: str, court: str | None, current_value: float,
    metric: str = "value", days: int = 7, db=None,
) -> tuple[bool, DriftBand | None]:
    """Return (is_drift, band). is_drift is True iff current_value falls
    outside median ± k×MAD over the prior `days` days. band is None when
    insufficient history exists (always returns False in that case).

    `db` defaults to `baseline.HISTORY_DB` at call time (so test
    monkeypatching of the module attr works)."""
    from quality import baseline as _baseline
    if db is None:
        db = _baseline.HISTORY_DB
    values = historical_values(check_name, court, metric, days, db=db)
    if not values:
        return False, None
    band = compute_band(values)
    if band is None:
        return False, None
    is_drift = (current_value < band.lower) or (current_value > band.upper)
    return is_drift, band
