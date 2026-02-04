#!/usr/bin/env python3
"""RRF Licence Search scraper + HTML map generator.

Requirements:
  pip install requests
Optional (recommended for TM2000 -> lat/lon conversion):
  pip install pyproj

Run:
  python rrf_map.py --page-size 200 --max-pages 50

HTML-only (no API calls; regenerates ./rrf_licences.html from existing JSON):
  python rrf_map.py --html-only

Outputs:
  ./rrf_licences.json
  ./rrf_map.html
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from pyproj import Transformer  # type: ignore
except Exception:
    Transformer = None  # pyproj is optional


API_URL = "https://rrf.rsm.govt.nz/api/public_search/licence"


# ---- Band classification -----------------------------------------------------
# Note: refFrequency appears to be in MHz (can include decimals).
BAND_DEFS = [
    # code, label, (min_mhz, max_mhz)
    ("b28", "LTE B28 (700)", (703, 803)),
    ("b5", "LTE B5 (850)", (824, 894)),
    ("b8", "LTE B8 (900)", (880, 960)),
    ("b3", "LTE B3 (1800)", (1710, 1880)),
    ("b1", "LTE/UMTS B1 (2100)", (1920, 2170)),
    ("b40", "LTE B40 (2300)", (2300, 2400)),
    ("b7", "LTE B7 (2600)", (2500, 2690)),
    ("n78", "NR n78 (3500)", (3300, 3800)),
    ("n258", "NR n258 (26GHz)", (24250, 27500)),
]


def classify_band(mhz: Optional[float]) -> str:
    if mhz is None:
        return "unknown"
    for code, _label, (lo, hi) in BAND_DEFS:
        if lo <= mhz <= hi:
            return code
    return "other"


def carrier_key_from_licensee(licensee: Optional[str]) -> str:
    if not licensee:
        return "unknown"
    s = licensee.upper()
    if "TWO DEGREES" in s:
        return "2degrees"
    if "SPARK" in s:
        return "spark"
    if "ONE NEW ZEALAND" in s or "ONE NZ" in s or "VODAFONE" in s:
        return "one"
    if "RURAL" in s:
        return "rcg"
    if "TŪ ĀTEA" in s or "TU ATEA" in s:
        return "tuatea"
    if "UBER" in s:
        return "uber"
    return "unknown"


# ---- HTTP + pagination -------------------------------------------------------


def build_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://rrf.rsm.govt.nz",
        "Referer": "https://rrf.rsm.govt.nz/ui/app/search/licence",
        "User-Agent": "rrf_map.py (requests)",
    }


def post_page(
    session: requests.Session,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout: int = 30,
    retries: int = 5,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            #proxies = {
            #    "http": "http://localhost:8888",
            #    "https": "http://localhost:8888",  # still use http unless your proxy supports https CONNECT
            #}

            r = session.post(
                API_URL,
                headers=headers,
                json=payload,
                timeout=timeout,
                #proxies=proxies,
                #verify=False,
            )
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized (endpoint may now require auth or request was blocked).")
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = min(2**attempt, 20)
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed after retries: {last_err}")


def fetch_all(
    base_payload: Dict[str, Any],
    page_size: int,
    max_pages: int = 0,
    sleep_between: float = 0.0,
) -> List[Dict[str, Any]]:
    session = requests.Session()
    headers = build_headers()

    payload = dict(base_payload)
    payload["pageSize"] = page_size
    payload["page"] = 1

    first = post_page(session, headers, payload)
    total_pages = int(first.get("totalPages", 1))
    if max_pages and max_pages > 0:
        total_pages = min(total_pages, max_pages)

    results: List[Dict[str, Any]] = []
    results.extend(first.get("results") or [])

    print(
        f"Page 1 fetched. totalPages={first.get('totalPages')} totalItems={first.get('totalItems')}"
    )

    for page in range(2, total_pages + 1):
        payload["page"] = page
        data = post_page(session, headers, payload)
        page_results = data.get("results") or []
        results.extend(page_results)
        print(
            f"Page {page} fetched. items={len(page_results)} total_accumulated={len(results)}"
        )
        if sleep_between > 0:
            time.sleep(sleep_between)

    return results


# ---- Geo handling ------------------------------------------------------------


def pick_lat_lon(
    geo_refs: List[Dict[str, Any]],
    transformer_2193_to_4326: Optional[Any],
) -> Tuple[Optional[float], Optional[float], str]:
    """Returns (lat, lon, sourceType).

    Preference:
      1) D or D2000: easting ~ lon, northing ~ lat (as in sample)
      2) TM2000: EPSG:2193 (NZTM2000) -> WGS84
    """

    if not geo_refs:
        return None, None, "None"

    for t in ("D", "D2000"):
        for g in geo_refs:
            if (g.get("type") or "").upper() == t:
                try:
                    lon = float(g["easting"])
                    lat = float(g["northing"])
                    if -60 < lat < -20 and 150 < lon < 190:
                        return lat, lon, t
                except Exception:
                    pass

    for g in geo_refs:
        if (g.get("type") or "").upper() == "TM2000":
            if transformer_2193_to_4326 is None:
                return None, None, "TM2000(no-pyproj)"
            try:
                e = float(g["easting"])
                n = float(g["northing"])
                lon, lat = transformer_2193_to_4326.transform(e, n)  # always_xy=True
                return float(lat), float(lon), "TM2000"
            except Exception:
                pass

    return None, None, "Unknown"


# ---- Normalisation -----------------------------------------------------------


def iso_date_or_none(s: Any) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    return s


def normalise_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformer = None
    if Transformer is not None:
        try:
            transformer = Transformer.from_crs("EPSG:2193", "EPSG:4326", always_xy=True)
        except Exception:
            transformer = None

    out: List[Dict[str, Any]] = []
    for r in records:
        if (r.get("configType") or "").upper() == "RCV":
            continue

        geo = r.get("locationGeoReferences") or []
        lat, lon, geo_src = pick_lat_lon(geo, transformer)

        lower = r.get("lowerBound")
        upper = r.get("upperBound")
        bandwidth = None
        try:
            if lower is not None and upper is not None:
                bandwidth = float(upper) - float(lower)
        except Exception:
            bandwidth = None

        ref_mhz = None
        try:
            if r.get("refFrequency") is not None:
                ref_mhz = float(r["refFrequency"])
        except Exception:
            ref_mhz = None

        district_codes = r.get("locationDistrictCodes") or []
        if isinstance(district_codes, str):
            district_codes = [district_codes]

        out.append(
            {
                "id": r.get("id"),
                "licenceNo": r.get("licenceNo"),
                "licensee": r.get("licensee"),
                "location": r.get("location"),
                "locationDistrictCodes": district_codes,
                "refFrequencyMHz": ref_mhz,
                "bandCode": classify_band(ref_mhz),
                "lowerBoundMHz": lower,
                "upperBoundMHz": upper,
                "bandwidthMHz": bandwidth,
                "power": r.get("power"),
                "configType": r.get("configType"),
                "licenceTypeCode": r.get("licenceTypeCode"),
                "licenceTypeDescription": r.get("licenceTypeDescription"),
                "licenceStatus": r.get("licenceStatus"),
                "suppressed": r.get("suppressed"),
                "commencementDate": iso_date_or_none(r.get("commencementDate")),
                "expiryDate": iso_date_or_none(r.get("expiryDate")),
                "certificationDate": iso_date_or_none(r.get("certificationDate")),
                "lastUpdatedDate": iso_date_or_none(r.get("lastUpdatedDate")),
                "lat": lat,
                "lon": lon,
                "geoSource": geo_src,
            }
        )
    return out


# ---- HTML generation (Bootstrap-first, minimal custom CSS) ------------------


def build_html(data: List[Dict[str, Any]]) -> str:
    bands_json = json.dumps(BAND_DEFS, ensure_ascii=False)

    # NOTE: placeholders + replace, so JS `${...}` doesn't conflict with Python.
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>RRF Licence Map</title>

  <!-- Bootstrap -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" />

  <!-- Leaflet -->
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
    crossorigin=""
  />

  <style>
    /* Minimal CSS: only what Bootstrap/Leaflet can't do */
    html, body { height: 100%; }
    
    :root { --nav-h: 0px; }

    /* Mobile: account for sticky navbar */
    @media (max-width: 991.98px) {
      #map { height: calc(100dvh - var(--nav-h)); }
    }
    @media (min-width: 992px) {
      #map, #filtersCanvas { height: calc(100dvh - var(--nav-h)); }
    }

    /* Desktop: full height */
    /*
    @media (min-width: 992px) {
      #map { height: 100vh; }
    }
    */
    
    .offcanvas-lg { overflow: scroll; }
    
    .swatch-dot { width: 10px; height: 10px; border-radius: 999px; display: inline-block; }

    /* make the filters pane scroll nicely on desktop */
    /*
    @media (min-width: 992px) {
      #filtersCanvas { height: 100vh; }
      #filtersCanvas .offcanvas-body { height: 100vh; overflow: auto; }
    }
    */

    .card { border-radius: unset; }

    .map-style-control {
      background: rgba(255, 255, 255, 0.95);
      border-radius: 0.25rem;
      box-shadow: 0 1px 4px rgba(0, 0, 0, 0.3);
      padding: 0.5rem;
    }

    .map-style-control select {
      min-width: 160px;
    }

    #regionSection {
      position: relative;
      z-index: 2;
    }

    #addressSuggestions {
      z-index: 1050;
    }
  </style>
</head>
<body class="bg-body-tertiary">

  <!-- Mobile top bar -->
  <nav class="navbar navbar-expand-lg bg-body border-bottom sticky-top" id="topbar">
    <div class="container-fluid">
      <span class="navbar-brand fw-semibold">RRF Licence Map</span>
      <div class="d-flex gap-2">
        <button class="btn btn-outline-secondary" id="geoLocateBtn" type="button">
          Locate me
        </button>
        <button class="btn btn-outline-primary d-lg-none" type="button" data-bs-toggle="offcanvas" data-bs-target="#filtersCanvas" aria-controls="filtersCanvas">
          Filters
        </button>
      </div>
    </div>
  </nav>

<div class="row g-0">

  <!-- Filters: offcanvas on mobile, static sidebar on lg+ -->
  <div class="col-12 col-lg-4 col-xl-3 border-end bg-body">
    <div class="offcanvas-lg offcanvas-start" tabindex="-1" id="filtersCanvas" aria-labelledby="filtersCanvasLabel">
      <div class="offcanvas-header d-lg-none">
        <h5 class="offcanvas-title" id="filtersCanvasLabel">Filters</h5>
        <button type="button"
                class="btn-close"
                data-bs-dismiss="offcanvas"
                data-bs-target="#filtersCanvas"
                aria-label="Close"></button>
      </div>
      <div class="offcanvas-body p-0">
        __FILTERS__
      </div>
    </div>
  </div>

  <!-- Map column -->
  <main class="col-12 col-lg-8 col-xl-9">
    <div id="map"></div>
  </main>

</div>

  <!-- libs -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script
    src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
    crossorigin=""
  ></script>

  <script>
    const DATA_URL = "https://raw.githubusercontent.com/codenui/rrf.codenui.co.nz/refs/heads/main/rrf_licences.json";
    const BAND_DEFS = __BANDS__; // [code,label,[lo,hi]]
    let DATA = [];

    async function init() {
      const response = await fetch(DATA_URL, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Failed to load data (${response.status}).`);
      }
      DATA = await response.json();

    // UI-only transforms (do not store in JSON)
    const DISTRICT_NAMES = {
      NL: "Northland",
      AK: "Auckland",
      WK: "Waikato",
      BP: "Bay of Plenty",
      GS: "Gisborne",
      TK: "Taranaki/King Country",
      TP: "Taupo",
      HB: "Hawke's Bay",
      MW: "Manawatu/Whanganui",
      WN: "Wellington",
      MB: "Marlborough",
      NT: "Nelson/Tasman",
      WC: "West Coast",
      CB: "Canterbury",
      OT: "Otago",
      SL: "Southland",
      NZ: "zzz Management Right",
    };

    const CARRIERS = {
      "2degrees": { color: "#009ED8", friendly: "2degrees" },
      "spark": { color: "rgb(64, 14, 125)", friendly: "Spark" },
      "one": { color: "#00A45F", friendly: "One NZ" },
      "rcg": { color: "#f68b1f", friendly: "RCG" },
      "tuatea": { color: "#000000", friendly: "Tu Atea" },
      "uber": { color: "#ec008c", friendly: "Uber" },
      "unknown": { color: "#666666", friendly: "Unknown" },
    };

    function carrierKeyFromLicensee(licensee) {
      if (!licensee) return "unknown";
      const s = String(licensee).toUpperCase();
      if (s.includes("TWO DEGREES")) return "2degrees";
      if (s.includes("SPARK")) return "spark";
      if (s.includes("ONE NEW ZEALAND") || s.includes("ONE NZ") || s.includes("VODAFONE")) return "one";
      if (s.includes("RURAL")) return "rcg";
      if (s.includes("TŪ ĀTEA") || s.includes("TU ATEA")) return "tuatea";
      if (s.includes("UBER")) return "uber";
      return "unknown";
    }

    function districtNamesFromCodes(codes) {
      const arr = Array.isArray(codes) ? codes : (codes ? [codes] : []);
      return arr.map(c => DISTRICT_NAMES[c] || c);
    }

    // Decorate records UI-side (still not changing the JSON file on disk)
    DATA.forEach(r => {
      const ck = carrierKeyFromLicensee(r.licensee);
      const meta = CARRIERS[ck] || CARRIERS.unknown;

      // cache derived values for UI convenience
      r.carrierKey = ck;
      r.carrierFriendly = meta.friendly;
      r.carrierColor = meta.color;

      r.locationDistrictNames = districtNamesFromCodes(r.locationDistrictCodes);
    });

    function parseDate(value) {
      if (!value) return null;
      const d = new Date(value + "T00:00:00");
      return isNaN(d.getTime()) ? null : d;
    }
    function parseISO(iso) {
      if (!iso) return null;
      const d = new Date(iso);
      return isNaN(d.getTime()) ? null : d;
    }
    function fmtDate(iso) {
      const d = parseISO(iso);
      if (!d) return "";
      return d.toISOString().slice(0, 10);
    }
    function safe(v) {
      return (v === null || v === undefined) ? "" : String(v);
    }

    // Map init
    const map = L.map("map");
    const baseLayers = {
      "OpenStreetMap": L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 19,
        attribution: "&copy; OpenStreetMap contributors"
      }),
      "Topographic": L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", {
        maxZoom: 17,
        attribution: "Map data: &copy; OpenStreetMap contributors, SRTM | Map style: &copy; OpenTopoMap (CC-BY-SA)"
      })
    };
    baseLayers.OpenStreetMap.addTo(map);
    map.setView([-41.2, 174.7], 5);

    const MapStyleControl = L.Control.extend({
      onAdd() {
        const div = L.DomUtil.create("div", "map-style-control");
        div.innerHTML = `
          <label class="form-label fw-semibold mb-1" for="mapStyleSelect">Map style</label>
          <select id="mapStyleSelect" class="form-select form-select-sm">
            ${Object.keys(baseLayers).map(name => `<option value="${name}">${name}</option>`).join("")}
          </select>
        `;
        L.DomEvent.disableClickPropagation(div);
        return div;
      }
    });
    const mapStyleControl = new MapStyleControl({ position: "topright" });
    map.addControl(mapStyleControl);

    const mapStyleSelect = document.getElementById("mapStyleSelect");
    if (mapStyleSelect) {
      mapStyleSelect.value = "OpenStreetMap";
      mapStyleSelect.addEventListener("change", (event) => {
        const selected = event.target.value;
        Object.entries(baseLayers).forEach(([name, layer]) => {
          if (map.hasLayer(layer)) {
            map.removeLayer(layer);
          }
          if (name === selected) {
            layer.addTo(map);
          }
        });
      });
    }

    let markersLayer = L.layerGroup().addTo(map);
    let addressLineLayer = L.layerGroup().addTo(map);
    let addressMarker = null;
    let addressSuggestTimer = null;
    let addressSuggestController = null;
    let addressSuggestionsCache = [];

    function zoomToAddress(lat, lon) {
      const coords = [lat, lon];
      map.setView(coords, 15);
      if (addressMarker) {
        addressMarker.remove();
      }
      addressMarker = L.marker(coords).addTo(map);
    }

    function clearAddressLines() {
      addressLineLayer.clearLayers();
    }

    function drawNearestCarrierLines(lat, lon) {
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
      clearAddressLines();
      const carriers = ["2degrees", "one", "spark"];
      const start = L.latLng(lat, lon);

      carriers.forEach((carrierKey) => {
        let best = null;
        let bestDist = Infinity;
        latestFiltered
          .filter(r => r.lat && r.lon && r.carrierKey === carrierKey)
          .forEach(r => {
            const dLat = r.lat - lat;
            const dLon = r.lon - lon;
            const dist = (dLat * dLat) + (dLon * dLon);
            if (dist < bestDist) {
              bestDist = dist;
              best = r;
            }
          });
        if (!best) return;
        const color = best.carrierColor || (CARRIERS[carrierKey]?.color ?? "#666666");
        const end = L.latLng(best.lat, best.lon);
        L.polyline([start, end], {
          color,
          weight: 3,
          opacity: 0.9,
          dashArray: "4 6"
        }).addTo(addressLineLayer);
      });
    }

    function hideAddressSuggestions() {
      if (!addressSuggestions) return;
      addressSuggestions.classList.add("d-none");
      addressSuggestions.innerHTML = "";
      addressSuggestionsCache = [];
    }

    function renderAddressSuggestions(results) {
      if (!addressSuggestions) return;
      addressSuggestions.innerHTML = "";
      addressSuggestionsCache = results;
      if (!results.length) {
        addressSuggestions.classList.add("d-none");
        return;
      }
      results.forEach((result, index) => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "list-group-item list-group-item-action";
        item.textContent = result.display_name;
        item.dataset.index = String(index);
        addressSuggestions.appendChild(item);
      });
      addressSuggestions.classList.remove("d-none");
    }

    async function fetchAddressSuggestions(query) {
      if (addressSuggestController) {
        addressSuggestController.abort();
      }
      addressSuggestController = new AbortController();
      const url = `https://nominatim.openstreetmap.org/search?format=json&limit=6&q=${encodeURIComponent(query)}`;
      const response = await fetch(url, {
        headers: { Accept: "application/json" },
        signal: addressSuggestController.signal
      });
      if (!response.ok) {
        throw new Error("Unable to reach search service.");
      }
      return response.json();
    }

    function parseLatLon(value) {
      if (!value) return null;
      const match = value.trim().match(/^(-?\\d+(?:\\.\\d+)?)\\s*,\\s*(-?\\d+(?:\\.\\d+)?)$/);
      if (!match) return null;
      const lat = Number(match[1]);
      const lon = Number(match[2]);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
      if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;
      return { lat, lon };
    }

    async function handleAddressSearch() {
      if (!qAddress) return;
      hideAddressSuggestions();
      const query = qAddress.value.trim();
      if (!query) {
        return;
      }

      const latLon = parseLatLon(query);
      if (latLon) {
        zoomToAddress(latLon.lat, latLon.lon);
        return;
      }

      try {
        const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(query)}`;
        const response = await fetch(url, { headers: { Accept: "application/json" } });
        if (!response.ok) {
          throw new Error("Unable to reach search service.");
        }
        const results = await response.json();
        if (!results || results.length === 0) {
          return;
        }
        const best = results[0];
        const lat = Number(best.lat);
        const lon = Number(best.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          return;
        }
        zoomToAddress(lat, lon);
      } catch (err) {
      }
    }

    let userMarker = null;
    
    function updateNavHeight() {
      const bar = document.getElementById("topbar");
      const h = bar ? bar.offsetHeight : 0;
      document.documentElement.style.setProperty("--nav-h", `${h}px`);
    }

    // Run now + on resize/orientation change
    updateNavHeight();
    window.addEventListener("resize", updateNavHeight);
    window.addEventListener("orientationchange", updateNavHeight);
    
    // UI refs
    const qDistrict = document.getElementById("qDistrict");
    const qLocation = document.getElementById("qLocation");
    const qAddress = document.getElementById("qAddress");
    const qCommFrom = document.getElementById("qCommFrom");
    const qCommTo = document.getElementById("qCommTo");
    const qExpFrom = document.getElementById("qExpFrom");
    const qExpTo = document.getElementById("qExpTo");
    const addressSuggestions = document.getElementById("addressSuggestions");

    const carrierBtns = document.getElementById("carrierBtns");
    const bandBtns = document.getElementById("bandBtns");

    const detailCard = document.getElementById("detailCard");
    const recentList = document.getElementById("recentList");
    const coordWarn = document.getElementById("coordWarn");
    const recentSection = document.getElementById("recentSection");
    const regionSection = document.getElementById("regionSection");
    const geoLocateBtn = document.getElementById("geoLocateBtn");

    function geolocateUser() {
      if (!navigator.geolocation) {
        window.alert("Geolocation is not supported by this browser.");
        return;
      }

      navigator.geolocation.getCurrentPosition(
        (position) => {
          const { latitude, longitude } = position.coords;
          if (userMarker) {
            userMarker.setLatLng([latitude, longitude]);
          } else {
            userMarker = L.circleMarker([latitude, longitude], {
              radius: 7,
              color: "#0d6efd",
              fillColor: "#0d6efd",
              fillOpacity: 0.7,
              weight: 2
            }).addTo(map);
          }
          map.setView([latitude, longitude], 12);
        },
        (error) => {
          window.alert(`Unable to fetch your location: ${error.message}`);
        },
        {
          enableHighAccuracy: true,
          timeout: 10000,
          maximumAge: 60000
        }
      );
    }

    geoLocateBtn?.addEventListener("click", geolocateUser);
    const carrierSection = document.getElementById("carrierSection");
    const bandSection = document.getElementById("bandSection");
    
    // District dropdown options
    function populateDistricts() {
      const mapD = new Map(); // code -> name
      DATA.forEach(r => {
        const codes = r.locationDistrictCodes || [];
        const names = r.locationDistrictNames || [];
        codes.forEach((c, i) => mapD.set(c, names[i] || c));
      });
      [...mapD.entries()].sort((a,b) => a[1].localeCompare(b[1])).forEach(([code, name]) => {
        const opt = document.createElement("option");
        opt.value = code;
        opt.textContent = name;
        qDistrict.appendChild(opt);
      });
    }
    populateDistricts();

    if (qAddress) {
      qAddress.addEventListener("keydown", event => {
        if (event.key === "Enter") {
          event.preventDefault();
          handleAddressSearch();
        }
      });
      qAddress.addEventListener("input", () => {
        clearAddressLines();
        const query = qAddress.value.trim();
        if (addressSuggestTimer) {
          clearTimeout(addressSuggestTimer);
        }
        if (query.length < 3) {
          hideAddressSuggestions();
          return;
        }
        addressSuggestTimer = setTimeout(async () => {
          try {
            const results = await fetchAddressSuggestions(query);
            renderAddressSuggestions(results || []);
          } catch (err) {
            if (err?.name === "AbortError") return;
            hideAddressSuggestions();
          }
        }, 250);
      });
      qAddress.addEventListener("blur", () => {
        setTimeout(() => hideAddressSuggestions(), 150);
      });
    }
    if (addressSuggestions) {
      addressSuggestions.addEventListener("click", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) return;
        const index = target.dataset.index;
        if (index === undefined) return;
        const result = addressSuggestionsCache[Number(index)];
        if (!result) return;
        const lat = Number(result.lat);
        const lon = Number(result.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          return;
        }
        qAddress.value = result.display_name || qAddress.value;
        hideAddressSuggestions();
        zoomToAddress(lat, lon);
        drawNearestCarrierLines(lat, lon);
      });
    }

    // -------------------------------------------------------------------------
    // Carriers: "show all by default"
    // - selected set empty => show all
    // - click when empty => select only that one
    // - click additional => add
    // - click selected => remove
    // - removing last => back to show all
    // -------------------------------------------------------------------------
    const carrierSelected = new Set(); // empty => all
    let carrierAllKeys = [];

    function uniqueCarriers() {
      const m = new Map(); // key -> {friendly,color}
      DATA.forEach(r => {
        const k = r.carrierKey || "unknown";
        if (k === "uber") return;
        
        const friendly = r.carrierFriendly || k;
        const color = r.carrierColor || "#666";
        if (!m.has(k)) m.set(k, { friendly, color });
      });
      return [...m.entries()].sort((a,b) => a[1].friendly.localeCompare(b[1].friendly));
    }
    
    function updateAvailability(baseFiltered) {
      // Carriers that still have matches given current BAND selection
      const availCarriers = new Set(
        baseFiltered
          .filter(r => bandSelected.size === 0 || bandSelected.has(r.bandCode || "unknown"))
          .map(r => r.carrierKey || "unknown")
          .filter(k => k !== "uber") // keep your existing exclusion consistent
      );

      // Bands that still have matches given current CARRIER selection
      const availBands = new Set(
        baseFiltered
          .filter(r => carrierSelected.size === 0 || carrierSelected.has(r.carrierKey || "unknown"))
          .map(r => r.bandCode || "unknown")
      );

      // Disable carrier buttons that would yield zero results,
      // BUT never disable ones that are currently selected (so user can unselect).
      carrierBtns.querySelectorAll("button").forEach(btn => {
        const k = btn.dataset.key;
        const isSelected = carrierSelected.has(k);
        const ok = availCarriers.has(k);
        btn.disabled = (!ok && !isSelected);
        btn.classList.toggle("opacity-50", btn.disabled);
      });

      // Same for band buttons
      bandBtns.querySelectorAll("button").forEach(btn => {
        const code = btn.dataset.code;
        const isSelected = bandSelected.has(code);
        const ok = availBands.has(code);
        btn.disabled = (!ok && !isSelected);
        btn.classList.toggle("opacity-50", btn.disabled);
      });
    }

    function syncCarrierButtons() {
      const allMode = carrierSelected.size === 0;
      carrierBtns.querySelectorAll("button").forEach(b => {
        const k = b.dataset.key;
        const on = allMode ? true : carrierSelected.has(k);

        b.classList.toggle("btn-dark", on);
        b.classList.toggle("btn-outline-secondary", !on);
        b.setAttribute("aria-pressed", on ? "true" : "false");
      });
    }

    function buildCarrierUI() {
      carrierBtns.innerHTML = "";

      // Desired display order
      const primary = ["2degrees", "one", "spark"];
      const secondary = ["rcg", "tuatea"];

      const carriers = uniqueCarriers(); // [[key, {friendly,color}], ...]

      carrierSelected.clear(); // start in "all" mode

      function makeBtn(key, meta) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn btn-dark btn-sm";
        btn.setAttribute("aria-pressed", "true");
        btn.dataset.key = key;
        btn.innerHTML = `<span class="swatch-dot me-2" style="background:${meta.color}"></span>${meta.friendly}`;

        btn.addEventListener("click", () => {
          if (carrierSelected.size === 0) {
            carrierSelected.add(key);
          } else {
            if (carrierSelected.has(key)) carrierSelected.delete(key);
            else carrierSelected.add(key);
          }
          syncCarrierButtons();
          refresh({ preserveView: true });
        });

        return btn;
      }

      const map = new Map(carriers); // key → meta

      // First row: 2degrees, one, spark
      primary.forEach(k => {
        if (map.has(k)) carrierBtns.appendChild(makeBtn(k, map.get(k)));
      });

      // Force new line
      const br = document.createElement("div");
      br.className = "w-100";
      carrierBtns.appendChild(br);

      // Second row: rcg, tu atea
      secondary.forEach(k => {
        if (map.has(k)) carrierBtns.appendChild(makeBtn(k, map.get(k)));
      });

      syncCarrierButtons();
    }
    buildCarrierUI();

    // -------------------------------------------------------------------------
    // Bands: same selection behaviour as carriers
    // -------------------------------------------------------------------------
    const bandSelected = new Set(); // empty => all
    let bandAllCodes = [];

    function syncBandButtons() {
      const allMode = bandSelected.size === 0;
      bandBtns.querySelectorAll("button").forEach(b => {
        const code = b.dataset.code;
        const on = allMode ? true : bandSelected.has(code);

        b.classList.toggle("btn-dark", on);
        b.classList.toggle("btn-outline-secondary", !on);
        b.setAttribute("aria-pressed", on ? "true" : "false");
      });
    }

    function buildBandUI() {
      bandBtns.innerHTML = "";
      const present = new Set(DATA.map(r => r.bandCode || "unknown"));
      const defs = BAND_DEFS
        .filter(([code]) => present.has(code))
        .map(([code, label]) => ({ code, label }));
      if (present.has("unknown")) defs.push({ code: "unknown", label: "Unknown" });
      if (present.has("other")) defs.push({ code: "other", label: "Other" });

      bandAllCodes = defs.map(d => d.code);
      bandSelected.clear(); // start in "all" mode

      defs.forEach(({ code, label }) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn btn-dark btn-sm";
        btn.setAttribute("aria-pressed", "true");
        btn.dataset.code = code;

        const mhz = label.split("(")[1]?.split(")")[0] ?? "";
        btn.textContent = `${code.toLowerCase()} / ${mhz}`;

        btn.addEventListener("click", () => {
          if (bandSelected.size === 0) {
            bandSelected.add(code);
          } else {
            if (bandSelected.has(code)) bandSelected.delete(code);
            else bandSelected.add(code);
          }

          syncBandButtons();
          refresh({ preserveView: true });
        });

        bandBtns.appendChild(btn);
      });

      syncBandButtons();
    }
    buildBandUI();

    // Debounced refresh to avoid glitches when typing quickly
    let refreshTimer = null;
    function refreshDebounced() {
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = setTimeout(() => {
        refreshTimer = null;
        refresh();
      }, 120);
    }

     // Filter events
     [qLocation, qDistrict, qCommFrom, qCommTo, qExpFrom, qExpTo].forEach(el => {
      el.addEventListener("input", refreshDebounced);
      el.addEventListener("change", refreshDebounced);
     });

    // -------------------------------------------------------------------------
    // CONSOLIDATED DETAIL VIEW
    // - single and multi licence views are now one view
    // - if there is exactly one licence, its details auto-expand when clicked
    // -------------------------------------------------------------------------

    function renderDetailSelection(sel) {
      if (!sel || !sel.items || sel.items.length === 0) {
        detailCard.className = "text-secondary";
        detailCard.innerHTML = "Click a marker (or a recent item) to see details.";
        if (recentSection) recentSection.style.display = "";
        if (regionSection) regionSection.style.display = "";
        if (carrierSection) carrierSection.style.display = "";
        if (bandSection) bandSection.style.display = "";
        return;
      }

      if (recentSection) recentSection.style.display = "none";
      if (regionSection) regionSection.style.display = "none";
      if (carrierSection) carrierSection.style.display = "none";
      if (bandSection) bandSection.style.display = "none";

      // stable sorting: frequency asc then licenceNo
      const items = [...sel.items].sort((a, b) => {
        const fa = Number(a.refFrequencyMHz);
        const fb = Number(b.refFrequencyMHz);
        const hasFa = Number.isFinite(fa);
        const hasFb = Number.isFinite(fb);
        if (hasFa && hasFb && fa !== fb) return fa - fb;
        if (hasFa !== hasFb) return hasFa ? -1 : 1;
        return safe(a.licenceNo).localeCompare(safe(b.licenceNo));
      });

      const first = items[0];
      const districts = (first.locationDistrictNames || first.locationDistrictCodes || []).join(", ");
      const siteLabel = safe(first.location) || "Site";
      const count = items.length;

      const accId = `acc_${safe(first.carrierKey)}_${Number(sel.lat ?? first.lat).toFixed(6)}_${Number(sel.lon ?? first.lon).toFixed(6)}`
        .replace(/[^a-zA-Z0-9_]/g, "_");
      const lat = Number(sel.lat ?? first.lat);
      const lon = Number(sel.lon ?? first.lon);
      const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);
      const latStr = hasCoords ? lat.toFixed(6) : "";
      const lonStr = hasCoords ? lon.toFixed(6) : "";
      const zoomLevel = 17;
      const googleSatelliteUrl = hasCoords
        ? `https://www.google.com/maps/@${latStr},${lonStr},${zoomLevel}z/data=!3m1!1e3`
        : "";
      const googleStreetUrl = hasCoords
        ? `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${latStr},${lonStr}`
        : "";
      const linzUrl = hasCoords
        ? `https://basemaps.linz.govt.nz/@${latStr},${lonStr},z${zoomLevel}?`
        : "";
      const linkSection = hasCoords
        ? `
          <div class="mt-2 d-flex flex-wrap gap-2">
            <a class="btn btn-sm btn-outline-secondary" href="${googleSatelliteUrl}" target="_blank" rel="noreferrer">Google Maps (Satellite)</a>
            <a class="btn btn-sm btn-outline-secondary" href="${googleStreetUrl}" target="_blank" rel="noreferrer">Google Maps (Street View)</a>
            <a class="btn btn-sm btn-outline-secondary" href="${linzUrl}" target="_blank" rel="noreferrer">LINZ Basemaps</a>
          </div>
        `
        : "";

      function renderRow(r, idx, autoOpen) {
        const rid = safe(r.id);
        const href = `https://rrf.rsm.govt.nz/ui/licence/spectrum/view/${rid}`;
        const headId = `${accId}_h_${idx}`;
        const bodyId = `${accId}_b_${idx}`;

        const compact = `
          <div class="d-flex flex-wrap gap-2 align-items-center">
            <span class="badge text-bg-dark">${safe(r.bandCode)}</span>
            <span class="badge text-bg-light">${safe(r.refFrequencyMHz)} MHz</span>
            <span class="badge text-bg-light">${Math.round(safe(r.bandwidthMHz))} MHz</span>
            <span class="ms-auto text-secondary small">${fmtDate(r.commencementDate)}</span>
          </div>
        `;

        const expanded = `
          <dl class="row mb-0 small mt-3">
            <dt class="col-5 text-secondary">Licence #</dt><dd class="col-7">${safe(r.licenceNo)}</dd>
            <dt class="col-5 text-secondary">Record ID</dt><dd class="col-7"><a href="${href}" target="_blank" rel="noreferrer">${rid}</a></dd>
            <dt class="col-5 text-secondary">Location</dt><dd class="col-7">${safe(r.location)}</dd>
            <dt class="col-5 text-secondary">District</dt><dd class="col-7">${safe(districts)}</dd>
            <dt class="col-5 text-secondary">Ref (MHz)</dt><dd class="col-7">${safe(r.refFrequencyMHz)}</dd>
            <dt class="col-5 text-secondary">Band</dt><dd class="col-7">${safe(r.bandCode)}</dd>
            <dt class="col-5 text-secondary">Bounds (MHz)</dt><dd class="col-7">${safe(r.lowerBoundMHz)} – ${safe(r.upperBoundMHz)}</dd>
            <dt class="col-5 text-secondary">Bandwidth</dt><dd class="col-7">${safe(r.bandwidthMHz)}</dd>
            <dt class="col-5 text-secondary">Power</dt><dd class="col-7">${safe(r.power)}</dd>
            <dt class="col-5 text-secondary">Commencement</dt><dd class="col-7">${fmtDate(r.commencementDate)}</dd>
            <dt class="col-5 text-secondary">Expiry</dt><dd class="col-7">${fmtDate(r.expiryDate)}</dd>
          </dl>
        `;

        const btnClass = autoOpen ? "accordion-button" : "accordion-button collapsed";
        const collapseClass = autoOpen ? "accordion-collapse collapse show" : "accordion-collapse collapse";
        const ariaExpanded = autoOpen ? "true" : "false";

        return `
          <div class="accordion-item">
            <h2 class="accordion-header" id="${headId}">
              <button class="${btnClass}" type="button"
                      data-bs-toggle="collapse" data-bs-target="#${bodyId}"
                      aria-expanded="${ariaExpanded}" aria-controls="${bodyId}">
                ${compact}
              </button>
            </h2>
            <div id="${bodyId}" class="${collapseClass}"
                 aria-labelledby="${headId}" ${count > 1 ? `data-bs-parent="#${accId}"` : ""}>
              <div class="accordion-body">
                ${expanded}
              </div>
            </div>
          </div>
        `;
      }

      const rows = items
        .map((r, i) => renderRow(r, i, count === 1)) // auto-expand only when single licence
        .join("");

      detailCard.className = "";
      detailCard.innerHTML = `
        <div class="card border-0 shadow-sm">
          <div class="card-body">
            <div class="d-flex align-items-start gap-3">
              <div class="rounded-4 p-2 text-white"
                   style="background:${safe(first.carrierColor)}; height:44px; display:flex; align-items:center; justify-content:center;">
                <span class="fw-bold" style="text-align:center;">${safe(first.carrierFriendly)}</span>
              </div>

              <div class="flex-grow-1">
                <div class="fw-semibold fs-6 mb-0">${siteLabel}</div>
                <div class="text-secondary small">${safe(districts)}</div>
              </div>

              <div class="text-end">
                <span class="badge text-bg-primary">${count} licence(s)</span>
              </div>
            </div>

            <div class="mt-3 d-flex gap-2">
              <button class="btn btn-sm btn-outline-secondary" id="clearDetailBtn" type="button">Back</button>
              <button class="btn btn-sm btn-dark" id="zoomSiteBtn" type="button" ${first.lat && first.lon ? "" : "disabled"}>Zoom</button>
            </div>
            ${linkSection}
          </div>
        </div>

        <div class="accordion mt-3" id="${accId}">
          ${rows}
        </div>
      `;

      document.getElementById("clearDetailBtn")?.addEventListener("click", () => renderDetailSelection(null));
      document.getElementById("zoomSiteBtn")?.addEventListener("click", () => {
        const r0 = items[0];
        if (r0.lat && r0.lon) map.setView([r0.lat, r0.lon], 12);
      });

      // wire per-row zoom buttons
      detailCard.querySelectorAll("button[data-zoom]").forEach(btn => {
        btn.addEventListener("click", () => {
          const idx = Number(btn.getAttribute("data-zoom"));
          const rr = items[idx];
          if (rr && rr.lat && rr.lon) map.setView([rr.lat, rr.lon], 12);
        });
      });
    }

    function getFilters() {
      return {
        locationText: (qLocation.value || "").trim().toLowerCase(),
        district: qDistrict.value || "",
        commFrom: parseDate(qCommFrom.value),
        commTo: parseDate(qCommTo.value),
        expFrom: parseDate(qExpFrom.value),
        expTo: parseDate(qExpTo.value),
      };
    }
    
    function passesBaseFilters(r, f) {
      if (f.locationText) {
        const loc = (r.location || "").toLowerCase();
        if (!loc.includes(f.locationText)) return false;
      }

      if (f.district) {
        const ds = r.locationDistrictCodes || [];
        if (!ds.includes(f.district)) return false;
      }

      const c = parseISO(r.commencementDate);
      if ((f.commFrom || f.commTo) && !c) return false;
      if (f.commFrom && c < f.commFrom) return false;
      if (f.commTo && c > f.commTo) return false;

      const e = r.expiryDate ? parseISO(r.expiryDate + "T00:00:00") : null;
      if ((f.expFrom || f.expTo) && !e) return false;
      if (f.expFrom && e < f.expFrom) return false;
      if (f.expTo && e > f.expTo) return false;

      return true;
    }

    function passesFilters(r, f) {
      if (!passesBaseFilters(r, f)) return false;

      // Carriers: selected empty => show all
      const k = r.carrierKey || "unknown";
      if (carrierSelected.size > 0 && !carrierSelected.has(k)) return false;

      // Bands: selected empty => show all
      const b = r.bandCode || "unknown";
      if (bandSelected.size > 0 && !bandSelected.has(b)) return false;

      return true;
    }

    function passesFilters(r, f) {
      if (f.locationText) {
        const loc = (r.location || "").toLowerCase();
        if (!loc.includes(f.locationText)) return false;
      }

      if (f.district) {
        const ds = r.locationDistrictCodes || [];
        if (!ds.includes(f.district)) return false;
      }

      // Carriers: selected empty => show all
      const k = r.carrierKey || "unknown";
      if (carrierSelected.size > 0 && !carrierSelected.has(k)) return false;

      // Bands: selected empty => show all
      const b = r.bandCode || "unknown";
      if (bandSelected.size > 0 && !bandSelected.has(b)) return false;

      const c = parseISO(r.commencementDate);
      if ((f.commFrom || f.commTo) && !c) return false;
      if (f.commFrom && c < f.commFrom) return false;
      if (f.commTo && c > f.commTo) return false;

      const e = r.expiryDate ? parseISO(r.expiryDate + "T00:00:00") : null;
      if ((f.expFrom || f.expTo) && !e) return false;
      if (f.expFrom && e < f.expFrom) return false;
      if (f.expTo && e > f.expTo) return false;

      return true;
    }
    
    function openFiltersIfMobile() {
      // lg breakpoint is 992px in Bootstrap 5
      if (!window.matchMedia("(max-width: 991.98px)").matches) return;

      const el = document.getElementById("filtersCanvas");
      if (!el) return;

      // If it’s already open, do nothing
      if (el.classList.contains("show")) return;

      const oc = bootstrap.Offcanvas.getOrCreateInstance(el);
      oc.show();
      setTimeout(() => map.invalidateSize(), 150);
    }

    function renderRecentList(filtered) {
      const sorted = [...filtered].sort((a, b) => {
        const da = parseISO(a.commencementDate);
        const db = parseISO(b.commencementDate);
        const ta = da ? da.getTime() : -Infinity;
        const tb = db ? db.getTime() : -Infinity;
        return tb - ta;
      });

      const top = sorted.slice(0, 10);
      recentList.innerHTML = "";

      top.forEach((r) => {
        const div = document.createElement("button");
        div.type = "button";
        div.className = "list-group-item list-group-item-action";
        div.innerHTML = `
          <div class="d-flex align-items-center gap-2">
            <span class="swatch-dot" style="background:${safe(r.carrierColor)}"></span>
            <div class="fw-semibold text-truncate">${safe(r.location)}</div>
            <span class="ms-auto badge text-bg-light">${safe(r.bandCode)}</span>
          </div>
          <div class="text-secondary small">${fmtDate(r.commencementDate)} • ${safe(r.refFrequencyMHz)} MHz</div>
        `;

        div.addEventListener("click", () => {
          // Show *all* licences at that carrier+coordinate, and auto-open the clicked one.
          const ck = r.carrierKey || "unknown";
          const lat = Number(r.lat);
          const lon = Number(r.lon);
          const key = `${ck}|${lat.toFixed(6)}|${lon.toFixed(6)}`;
          const g = currentGroups.get(key);

          const sel = g
            ? { ...g, activeId: String(r.id) }
            : { lat: r.lat, lon: r.lon, items: [r], activeId: String(r.id) };

          renderDetailSelection(sel);
          openFiltersIfMobile();
          if (r.lat && r.lon) map.setView([r.lat, r.lon], 12);
        });

        recentList.appendChild(div);
      });
    }
    
    // stash latest grouping so other UI (eg recent list clicks) can open the full group
    let currentGroups = new Map();
    let latestFiltered = [];
    
    function inMapView(r) {
      if (!r.lat || !r.lon) return false;
      const b = map.getBounds();
      return b.contains([r.lat, r.lon]);
    }

    function refreshRecentList() {
      const visible = latestFiltered.filter(r => inMapView(r));
      renderRecentList(visible);
    }

    function refresh({ preserveView = false } = {}) {
      clearAddressLines();
      const f = getFilters();

      // Apply ONLY non-carrier/non-band filters first
      const baseFiltered = DATA.filter(r => passesBaseFilters(r, f));

      // Update button enable/disable based on current selection + base filters
      updateAvailability(baseFiltered);

      // Now apply carrier + band selections
      const filtered = baseFiltered.filter(r => {
        const k = r.carrierKey || "unknown";
        const b = r.bandCode || "unknown";
        if (carrierSelected.size > 0 && !carrierSelected.has(k)) return false;
        if (bandSelected.size > 0 && !bandSelected.has(b)) return false;
        return true;
      });

      /*
      const missing = filtered.filter(r => !(r.lat && r.lon)).length;
      if (missing > 0) {
        coordWarn.classList.remove("d-none");
        coordWarn.textContent = `${missing} record(s) match filters but have no usable coordinates (so they won't appear on the map).`;
      } else {
        coordWarn.classList.add("d-none");
        coordWarn.textContent = "";
      }
      */

      latestFiltered = filtered;
      refreshRecentList();

      // group markers by carrier + coordinate so one marker can represent multiple licences
      markersLayer.clearLayers();
      const withCoords = filtered.filter(r => r.lat && r.lon);

      const groups = new Map();
      withCoords.forEach(r => {
        const ck = r.carrierKey || "unknown";        
        const lat = Number(r.lat);
        const lon = Number(r.lon);

        // rounding avoids float equality issues
        const key = `${ck}|${lat.toFixed(6)}|${lon.toFixed(6)}`;

        if (!groups.has(key)) {
          groups.set(key, {
            carrierKey: ck,
            carrierFriendly: r.carrierFriendly,
            carrierColor: r.carrierColor,
            lat,
            lon,
            items: []
          });
        }
        groups.get(key).items.push(r);
      });

      // make available to other UI handlers (eg recent list click -> open full group)
      currentGroups = groups;

      const groupList = [...groups.values()];

      const locationBuckets = new Map();
      groupList.forEach(g => {
        const locKey = `${g.lat.toFixed(6)}|${g.lon.toFixed(6)}`;
        if (!locationBuckets.has(locKey)) {
          locationBuckets.set(locKey, []);
        }
        locationBuckets.get(locKey).push(g);
      });

      function offsetLatLon(lat, lon, index, total) {
        if (total <= 1) return { lat, lon };
        const angle = (2 * Math.PI * index) / total;
        const meters = 25;
        const latRad = (lat * Math.PI) / 180;
        const dLat = (meters / 111320) * Math.sin(angle);
        const dLon = (meters / (111320 * Math.cos(latRad))) * Math.cos(angle);
        return { lat: lat + dLat, lon: lon + dLon };
      }

      locationBuckets.forEach((entries) => {
        const ordered = [...entries].sort((a, b) => {
          const aLabel = safe(a.carrierFriendly || a.carrierKey);
          const bLabel = safe(b.carrierFriendly || b.carrierKey);
          return aLabel.localeCompare(bLabel);
        });
        ordered.forEach((g, idx) => {
          const color = g.carrierColor || "#666";
          const offset = offsetLatLon(g.lat, g.lon, idx, ordered.length);
          const m = L.circleMarker([offset.lat, offset.lon], {
            radius: 6,
            color: "#000000",
            fillColor: color,
            fillOpacity: 1,
            opacity: 1,
            weight: 3
          });
          m.on("click", () => {
            // consolidated view always; single auto-expands inside renderDetailSelection
            renderDetailSelection(g);
            openFiltersIfMobile();
          });
          m.addTo(markersLayer);
        });
      });

      if (!preserveView && withCoords.length > 0 && withCoords.length < 2000) {
        const b = L.latLngBounds(withCoords.map(r => [r.lat, r.lon]));
        map.fitBounds(b.pad(0.2));
      }
    }

    document.getElementById("recentUpdateButton")?.addEventListener("click", () => {
      refreshRecentList();
    });

    // Clear detail button in header
    document.getElementById("clearDetail")?.addEventListener("click", () => renderDetailSelection(null));

    renderDetailSelection(null);
    refresh();
  }

  init().catch(err => {
    console.error("Failed to initialize page data:", err);
    window.alert(err?.message || "Failed to load data.");
  });
  </script>
</body>
</html>
"""

    filters_html = """
<div class="d-grid">

  <div class="card d-none">
    <div class="card-body">
      <div class="text-secondary">
          <button class="btn btn-sm btn-outline-secondary" id="clearDetail" type="button">Clear selection</button>
      </div>
    </div>
  </div>

  <div class="card" id="carrierSection">
    <div class="card-body">
      <div class="fw-semibold mb-2">Carriers</div>
      <div class="btn-group gap-2 flex-wrap" role="group" aria-label="Carriers" id="carrierBtns"></div>
    </div>
  </div>

  <div class="card" id="bandSection">
    <div class="card-body">
        <div class="fw-semibold mb-2">Bands</div>
        <div class="btn-group gap-2 flex-wrap" role="group" aria-label="Bands" id="bandBtns"></div>
    </div>
  </div>

  <div class="card" id="regionSection">
    <div class="card-body">
      <div class="row g-2">
        <div class="col-12">
          <label class="form-label fw-semibold" for="qDistrict">District</label>
          <select class="form-select" id="qDistrict">
            <option value="">(any)</option>
          </select>
        </div>
        <div class="col-12">
          <label class="form-label fw-semibold" for="qLocation">Location</label>
          <input class="form-control" id="qLocation" type="text" placeholder="e.g. NAPIER, SYDENHAM…" />
        </div>
        <div class="col-12 position-relative">
          <label class="form-label fw-semibold" for="qAddress">Address search</label>
          <input class="form-control" id="qAddress" type="text" placeholder="Search address or lat,lon" autocomplete="off" />
          <div class="list-group position-absolute w-100 shadow-sm d-none" id="addressSuggestions"></div>
        </div>
      </div>
    </div>
  </div>

  <div class="card d-none">
    <div class="card-body">
      <div class="d-flex align-items-center justify-content-between">
        <div class="fw-semibold">Date filters</div>
        <button class="btn btn-sm btn-outline-secondary" type="button" data-bs-toggle="collapse" data-bs-target="#dateCollapse" aria-expanded="false" aria-controls="dateCollapse">Toggle</button>
      </div>

      <div class="collapse mt-3" id="dateCollapse">
        <div class="row g-2">
          <div class="col-6">
            <label class="form-label" for="qCommFrom">Comm from</label>
            <input class="form-control" id="qCommFrom" type="date" />
          </div>
          <div class="col-6">
            <label class="form-label" for="qCommTo">Comm to</label>
            <input class="form-control" id="qCommTo" type="date" />
          </div>
          <div class="col-6">
            <label class="form-label" for="qExpFrom">Expiry from</label>
            <input class="form-control" id="qExpFrom" type="date" />
          </div>
          <div class="col-6">
            <label class="form-label" for="qExpTo">Expiry to</label>
            <input class="form-control" id="qExpTo" type="date" />
          </div>
        </div>
      </div>

    </div>
  </div>

  <div class="card" id="recentSection">
    <div class="card-body">
      <div class="d-flex align-items-center justify-content-between mb-2 gap-2">
        <div class="fw-semibold">10 most recent changes</div>
        <button class="btn btn-sm btn-outline-primary" id="recentUpdateButton" type="button">Update</button>
      </div>
      <div class="list-group" id="recentList"></div>
    </div>
  </div>

  <div class="card">
    <div class="card-body">
      <div class="fw-semibold mb-2">Details</div>
      <div id="detailCard" class="text-secondary"></div>
    </div>
  </div>

  <div class="text-secondary alert alert-warning d-none" id="coordWarn" role="alert"></div>

</div>
"""

    return html.replace("__BANDS__", bands_json).replace("__FILTERS__", filters_html)


# ---- CLI + IO ----------------------------------------------------------------


def read_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, list):
        raise ValueError(f"Expected a JSON array in {path}")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--page-size",
        type=int,
        default=5000,
        help="Items per page (try 200; server may cap)",
    )
    ap.add_argument(
        "--max-pages", type=int, default=0, help="0 = all pages; else limit for testing"
    )
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between page requests (seconds)")
    ap.add_argument("--licence-type", type=int, default=178, help="licenceType filter (default 178)")
    ap.add_argument("--order-by", default="id desc", help="orderBy")
    ap.add_argument("--suppressed", action="store_true", help="Set suppressed=true (default false)")

    # HTML-only mode
    ap.add_argument(
        "--html-only",
        action="store_true",
        help="Regenerate HTML from existing JSON (skips API fetch)",
    )
    ap.add_argument(
        "--json-in",
        default="rrf_licences.json",
        help="Input JSON for --html-only (default: ./rrf_licences.json)",
    )
    ap.add_argument(
        "--html-out",
        default="rrf_map.html",
        help="HTML output path (default: ./rrf_map.html)",
    )
    ap.add_argument(
        "--json-out",
        default="rrf_licences.json",
        help="JSON output path when fetching (default: ./rrf_licences.json)",
    )

    args = ap.parse_args()

    # HTML-only fast path
    if args.html_only:
        try:
            data = read_json(args.json_in)
        except Exception as e:
            print(f"ERROR: --html-only failed to read {args.json_in}: {e}", file=sys.stderr)
            return 2

        html = build_html(data)
        with open(args.html_out, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"Read {args.json_in} ({len(data)} records)")
        print(f"Wrote {args.html_out}")
        return 0

    # Normal fetch path
    base_payload: Dict[str, Any] = {
        "searchText": "",
        "suppressed": bool(args.suppressed),
        "mapVisible": "false",
        "displayGeorefType": "T",
        "orderBy": args.order_by,
        "licenceType": [args.licence_type],
        "isSearchVisible": "true",
        "isRelevanceSort": "false",
    }

    raw = fetch_all(
        base_payload=base_payload,
        page_size=args.page_size,
        max_pages=args.max_pages,
        sleep_between=args.sleep,
    )

    normalised = normalise_records(raw)

    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump(normalised, f, ensure_ascii=False, indent=2)

    html = build_html(normalised)
    with open(args.html_out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nWrote {args.json_out} ({len(normalised)} records)")
    print(f"Wrote {args.html_out}")

    if Transformer is None:
        print("\nNOTE: pyproj not installed, so TM2000-only records won't map. Install with: pip install pyproj")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
