"""
Optional precipitation-regime classification.

The cleaned records retain the raw present-weather groups (au, aw, mw), so
users may define their own schemes. These helpers reproduce the project's
convective definition and can be applied to any frame carrying those columns.

Definition
----------
A record is convective if any token in its au, aw, or mw group matches the
corresponding convective code set (thunderstorm, shower, hail, funnel, or
squall descriptors). The regime is then:

    convective   prec > 0 and convective flag set
    stratiform   prec > 0 and convective flag not set
    none         prec == 0
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .schema import CONVECTIVE_CODES, WEATHER_COLUMNS


def convective_mask(
    df: pd.DataFrame,
    codes: Optional[dict[str, frozenset[str]]] = None,
) -> pd.Series:
    """Boolean convective flag from the au, aw, and mw present-weather groups.

    Parameters
    ----------
    df : DataFrame
        Must contain the columns in ``WEATHER_COLUMNS``.
    codes : dict[str, frozenset[str]], optional
        Per-group convective code sets. Defaults to ``CONVECTIVE_CODES``.

    Returns
    -------
    Series[bool]
    """
    codes = codes or CONVECTIVE_CODES
    mask = np.zeros(len(df), dtype=bool)
    for group in WEATHER_COLUMNS:
        if group not in df.columns:
            continue
        group_codes = codes.get(group, frozenset())
        if not group_codes:
            continue
        # Present-weather groups repeat a small vocabulary over many rows, so
        # each distinct string is tested once and the result broadcast.
        labels, uniques = pd.factorize(df[group].fillna("").astype(str))
        hit = np.fromiter(
            (not group_codes.isdisjoint(u.split()) for u in uniques),
            dtype=bool,
            count=len(uniques),
        )
        mask |= hit[labels]
    return pd.Series(mask, index=df.index)


def precip_type(
    df: pd.DataFrame,
    codes: Optional[dict[str, frozenset[str]]] = None,
    precip_col: str = "prec",
) -> pd.Series:
    """Categorical regime label (convective | stratiform | none) per record."""
    conv = convective_mask(df, codes=codes)
    wet = df[precip_col] > 0
    out = pd.Series("none", index=df.index, dtype=object)
    out[wet & conv] = "convective"
    out[wet & ~conv] = "stratiform"
    return out


def add_precip_type(
    df: pd.DataFrame,
    codes: Optional[dict[str, frozenset[str]]] = None,
    precip_col: str = "prec",
) -> pd.DataFrame:
    """Return a copy of ``df`` with a 'prec_type' column added."""
    out = df.copy()
    out["prec_type"] = precip_type(out, codes=codes, precip_col=precip_col)
    return out
