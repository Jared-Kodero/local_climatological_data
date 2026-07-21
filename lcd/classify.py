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
    mask = pd.Series(False, index=df.index)
    for group in WEATHER_COLUMNS:
        if group not in df.columns:
            continue
        group_codes = codes.get(group, frozenset())
        tokens = df[group].fillna("").astype(str).str.split()
        mask |= tokens.apply(lambda toks: not group_codes.isdisjoint(toks))
    return mask


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
