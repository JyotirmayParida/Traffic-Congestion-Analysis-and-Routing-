"""
firestore_client.py — Firebase Admin SDK wrapper for Firestore operations.

Initialises the Firebase app once from the GOOGLE_APPLICATION_CREDENTIALS
env var. All public functions raise HTTP 503 on Firestore connectivity failure.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger("firestore_client")

# ---------------------------------------------------------------------------
# SDK initialisation (once at import)
# ---------------------------------------------------------------------------
_db = None
_SERVER_TIMESTAMP = None

try:
    import firebase_admin  # type: ignore
    from firebase_admin import credentials, firestore  # type: ignore
    from google.api_core.exceptions import GoogleAPIError  # type: ignore

    import json

    _cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    _cred_json = os.getenv("FIREBASE_CREDENTIALS_JSON")

    if not firebase_admin._apps:
        if _cred_json:
            # Cloud deployment — credentials passed as JSON string
            cred = credentials.Certificate(json.loads(_cred_json))
        elif _cred_path:
            # Local development — credentials loaded from file path
            cred = credentials.Certificate(_cred_path)
        else:
            raise ValueError("No Firebase credentials found. Set either GOOGLE_APPLICATION_CREDENTIALS or FIREBASE_CREDENTIALS_JSON.")
        firebase_admin.initialize_app(cred)
    _db = firestore.client()
    _SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP
    logger.info("Firebase Admin SDK initialised successfully.")
except Exception as _init_err:
    logger.error(
        f"Firebase Admin SDK failed to initialise: {_init_err}. "
        "Firestore endpoints will return HTTP 503."
    )
    _db = None
    _SERVER_TIMESTAMP = None


def _require_db():
    if _db is None:
        raise HTTPException(
            status_code=503,
            detail="Firestore is not available. Check GOOGLE_APPLICATION_CREDENTIALS.",
        )


# ---------------------------------------------------------------------------
# Junctions
# ---------------------------------------------------------------------------

def get_junctions_by_city(city: str) -> list[dict[str, Any]]:
    """
    Query 'junctions' collection filtered by city.
    Returns list of dicts with all Firestore document fields plus 'junction_id'.
    Raises HTTP 503 on connectivity failure.
    """
    _require_db()
    try:
        docs = _db.collection("junctions").where("city", "==", city).stream()
        results = []
        for doc in docs:
            data = doc.to_dict() or {}
            data["junction_id"] = doc.id
            if "edges" not in data:
                data["edges"] = []
            results.append(data)
        logger.info(f"Fetched {len(results)} junctions for city='{city}'")
        return results
    except Exception as exc:
        logger.error(f"Firestore error fetching junctions for '{city}': {exc}")
        raise HTTPException(
            status_code=503,
            detail=f"Firestore unreachable while fetching junctions: {exc}",
        )


def get_all_junctions() -> list[dict[str, Any]]:
    """
    Query all documents in the 'junctions' collection.
    Raises HTTP 503 on connectivity failure.
    """
    _require_db()
    try:
        docs = _db.collection("junctions").stream()
        results = []
        for doc in docs:
            data = doc.to_dict() or {}
            data["junction_id"] = doc.id
            if "edges" not in data:
                data["edges"] = []
            results.append(data)
        logger.info(f"Fetched {len(results)} junctions (all cities)")
        return results
    except Exception as exc:
        logger.error(f"Firestore error fetching all junctions: {exc}")
        raise HTTPException(
            status_code=503,
            detail=f"Firestore unreachable: {exc}",
        )



# ---------------------------------------------------------------------------
# Route query persistence (fire-and-forget)
# ---------------------------------------------------------------------------

async def _write_route_query(
    source_id: str,
    dest_id: str,
    optimal_path: list[str],
    total_time_sec: float,
    model_version: str,
) -> None:
    """Async Firestore write — executed via asyncio.create_task."""
    if _db is None:
        return
    try:
        _db.collection("routes").add(
            {
                "source_id": source_id,
                "dest_id": dest_id,
                "optimal_path": optimal_path,
                "total_time_sec": total_time_sec,
                "model_version": model_version,
                "queried_at": _SERVER_TIMESTAMP,
            }
        )
    except Exception as exc:
        logger.warning(f"Non-blocking route save failed: {exc}")


def save_route_query(
    source_id: str,
    dest_id: str,
    result: Any,  # RouteResponse — avoid circular import
) -> None:
    """
    Fire-and-forget route persistence using asyncio.create_task.
    Never blocks the HTTP response.
    """
    asyncio.create_task(
        _write_route_query(
            source_id=source_id,
            dest_id=dest_id,
            optimal_path=result.optimal_path,
            total_time_sec=result.total_time_sec,
            model_version=result.model_version,
        )
    )


# ---------------------------------------------------------------------------
# Traffic snapshot write (called by scheduler)
# ---------------------------------------------------------------------------

def write_traffic_snapshot(
    junction_id: str,
    features: dict[str, Any],
    predicted_delay_sec: float,
    congestion_level: str,
    peak_hour: bool,
    monsoon_active: bool,
) -> None:
    """
    Write a TrafficSnapshot document and update the junction's current_features.
    Called synchronously from the scheduler coroutine.
    """
    if _db is None:
        return
    try:
        # Write snapshot
        _db.collection("snapshots").add(
            {
                "junction_id": junction_id,
                "features": features,
                "predicted_delay_sec": predicted_delay_sec,
                "congestion_level": congestion_level,
                "peak_hour": peak_hour,
                "monsoon_active": monsoon_active,
                "updated_at": _SERVER_TIMESTAMP,
            }
        )
        # Update junction's live state (triggers onSnapshot on frontend)
        _db.collection("junctions").document(junction_id).update(
            {
                "current_features": features,
                "last_updated": _SERVER_TIMESTAMP,
            }
        )
    except Exception as exc:
        logger.warning(f"Snapshot write failed for junction '{junction_id}': {exc}")
