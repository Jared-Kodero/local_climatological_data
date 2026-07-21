"""
Optional city assignment from station coordinates using Natural Earth
populated-place shapefiles accessed through cartopy.

The nearest populated place to each unique (lat, lon) supplies the city and
state (first-order administrative unit). cartopy downloads the shapefile from
Natural Earth on first use and caches it; if cartopy is unavailable or the
download fails, the frame is returned unchanged.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger("lcd")


def add_city_names(df: pd.DataFrame, resolution: str = "10m") -> pd.DataFrame:
    """Assign 'city' and 'state' by nearest Natural Earth populated place.

    Parameters
    ----------
    df : DataFrame
        Must contain 'lat' and 'lon'.
    resolution : {"10m", "50m", "110m"}, default "10m"
        Natural Earth resolution; 10m has the most places.

    Returns
    -------
    DataFrame
        Copy with 'city' and 'state' overwritten from geography. Returned
        unchanged if cartopy or the shapefile is unavailable.
    """
    try:
        import cartopy.io.shapereader as shpreader
    except Exception:
        logger.warning("cartopy not installed; skipping city assignment")
        return df

    try:
        shp = shpreader.natural_earth(
            resolution=resolution, category="cultural", name="populated_places"
        )
        names, states, plon, plat = [], [], [], []
        for rec in shpreader.Reader(shp).records():
            geom = rec.geometry
            if geom is None:
                continue
            names.append(str(rec.attributes.get("NAME", "")))
            states.append(str(rec.attributes.get("ADM1NAME", "")))
            plon.append(geom.x)
            plat.append(geom.y)
        plon = np.asarray(plon)
        plat = np.asarray(plat)
        names = np.asarray(names, dtype=object)
        states = np.asarray(states, dtype=object)
    except Exception:
        logger.exception("Natural Earth populated_places unavailable")
        return df

    uniq = df[["lat", "lon"]].drop_duplicates().reset_index(drop=True)
    cos = np.cos(np.deg2rad(uniq["lat"].to_numpy()))
    city = np.empty(len(uniq), dtype=object)
    state = np.empty(len(uniq), dtype=object)
    for i, (la, lo, c) in enumerate(zip(uniq["lat"], uniq["lon"], cos)):
        d = ((plon - lo) * c) ** 2 + (plat - la) ** 2
        j = int(np.argmin(d))
        city[i] = names[j]
        state[i] = states[j]
    uniq["city"] = city
    uniq["state"] = state

    out = df.drop(columns=[c for c in ("city", "state") if c in df.columns])
    return out.merge(uniq, on=["lat", "lon"], how="left")
