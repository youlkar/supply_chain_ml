from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import requests
from datetime import datetime
from supabase import create_client, Client
import uvicorn
import uuid
from typing import Any, Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv
import logging
from pydantic import BaseModel, Field
import asyncio
import json
import importlib.util

# ---- Build-time debug: print dependency sizes in Vercel build logs ----
# Vercel sets VERCEL=1 in build/runtime. We also use a custom flag so you can turn it off.
if os.environ.get("VERCEL") == "1" and os.environ.get("PRINT_DEP_SIZES") == "1":
    try:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "print_dep_sizes.py"
        if not script_path.exists():
            raise FileNotFoundError(f"{script_path} not found")

        spec = importlib.util.spec_from_file_location("print_dep_sizes", script_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        module.main()
    except Exception as e:
        print("Dependency size debug failed:", repr(e))

app = FastAPI(title="SupplyLens AI Enterprise Engine")

# Load backend/.env so containerized or local deploys can keep shared secrets there
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("supplylens")

# --- CONFIGURATION ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service role key
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set via .env or env vars")

# Edge function settings
SUPABASE_EDGE_FUNCTION_NAME = os.getenv("SUPABASE_EDGE_FUNCTION_NAME", "ingest_job_function")

# IMPORTANT: for "true fire-and-forget demo", keep this VERY small
# so we don't hang a background worker for long (even though response is immediate).
SUPABASE_EDGE_FUNCTION_TIMEOUT_SECS = float(os.getenv("SUPABASE_EDGE_FUNCTION_TIMEOUT_SECS", "1.5"))

MODEL_VERSION_ID = os.getenv("MODEL_VERSION_ID", "demo-v1")

# Supabase client (admin)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def iso_now() -> str:
    return datetime.utcnow().isoformat()


# -----------------------------
# REQUEST MODELS
# -----------------------------
class StorageFilePayload(BaseModel):
    filename: str
    doc_type: str
    storage_path: str


class StoragePayload(BaseModel):
    provider: str = Field(..., description="supabase")
    bucket: str
    prefix: Optional[str] = Field(None)


class IngestPayload(BaseModel):
    buyer_id: Optional[str] = None
    uploaded_by: Optional[str] = None
    expected_po: Optional[str] = None
    batch_id: Optional[str] = None
    storage: StoragePayload
    files: List[StorageFilePayload]


class IngestKickoffResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


# -----------------------------
# Validation helpers
# -----------------------------
def _is_valid_uuid(val: Optional[str]) -> bool:
    if not val:
        return False
    try:
        uuid.UUID(str(val))
        return True
    except Exception:
        return False


def _validate_prefix_guard(payload: IngestPayload) -> None:
    if payload.storage.prefix:
        prefix = payload.storage.prefix.rstrip("/") + "/"
        for f in payload.files:
            if not f.storage_path.startswith(prefix):
                raise HTTPException(
                    status_code=400,
                    detail=f"File {f.filename} storage_path is outside prefix {payload.storage.prefix}",
                )


def _validate_ingest_payload(payload: IngestPayload) -> None:
    if payload.storage.provider.lower() != "supabase":
        raise HTTPException(status_code=400, detail="Only Supabase storage uploads are supported.")
    if not payload.files:
        raise HTTPException(status_code=400, detail="No files were referenced.")
    if not payload.storage.bucket:
        raise HTTPException(status_code=400, detail="storage.bucket is required.")
    _validate_prefix_guard(payload)


def _stringify_error(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


# -----------------------------
# DB-backed job tracking
# -----------------------------
def create_ingest_job_row(job_id: str, payload: IngestPayload) -> None:
    buyer_id = payload.buyer_id if _is_valid_uuid(payload.buyer_id) else None
    uploaded_by = payload.uploaded_by if _is_valid_uuid(payload.uploaded_by) else None

    row = {
        "job_id": job_id,
        "status": "QUEUED",
        "buyer_id": buyer_id,
        "expected_po": payload.expected_po,
        "bucket": payload.storage.bucket,
        "prefix": payload.storage.prefix,
        "file_manifest": [f.dict() for f in payload.files],
        "error": None,
        "result": None,
        "created_at": iso_now(),
        "updated_at": iso_now(),
    }

    # Upsert so retried calls with same job_id don't break
    supabase.table("ingest_jobs").upsert(row, on_conflict="job_id").execute()


def get_ingest_job_row(job_id: str) -> Optional[Dict[str, Any]]:
    resp = supabase.table("ingest_jobs").select("*").eq("job_id", job_id).limit(1).execute()
    if resp.data:
        return resp.data[0]
    return None


# -----------------------------
# Edge Function invocation (Option A: service-role bearer)
# -----------------------------
def _edge_function_url(function_name: str) -> str:
    return f"{SUPABASE_URL}/functions/v1/{function_name}"


async def invoke_edge_function_detached(job_id: str, payload: IngestPayload) -> None:
    """
    Truly detached call:
      - Runs in background task
      - Uses very small timeout
      - Does NOT mark job failed on timeout (Edge may still run)
    """
    url = _edge_function_url(SUPABASE_EDGE_FUNCTION_NAME)

    edge_payload = {
        "job_id": job_id,
        "buyer_id": payload.buyer_id,
        "uploaded_by": payload.uploaded_by,
        "expected_po": payload.expected_po,
        "batch_id": payload.batch_id,
        "storage": payload.storage.dict(),
        "files": [f.dict() for f in payload.files],
        "model_version_id": MODEL_VERSION_ID,
    }

    # Option A: Edge function accepts service-role bearer
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    def _do_call():
        try:
            # ultra-short timeout; if it times out, we assume Edge continues or will retry later
            resp = requests.post(
                url,
                headers=headers,
                json=edge_payload,
                timeout=SUPABASE_EDGE_FUNCTION_TIMEOUT_SECS,
            )

            if resp.status_code >= 400:
                msg = f"Edge function error {resp.status_code}: {resp.text}"
                logger.error("job_id=%s %s", job_id, msg)
                # mark failed ONLY when we have a real error response
                supabase.table("ingest_jobs").update(
                    {"status": "FAILED", "error": msg, "updated_at": iso_now()}
                ).eq("job_id", job_id).execute()
            else:
                logger.info("Edge invoked job_id=%s status=%s", job_id, resp.status_code)

        except requests.Timeout:
            # DO NOT FAIL JOB on timeout — edge may be running
            logger.info("Edge invoke timed out (expected for detached) job_id=%s", job_id)
        except Exception as e:
            # If we fail to even send, we can optionally mark job as FAILED for demo clarity
            msg = f"Failed to invoke edge function: {e}"
            logger.exception("job_id=%s %s", job_id, msg)
            supabase.table("ingest_jobs").update(
                {"status": "FAILED", "error": msg, "updated_at": iso_now()}
            ).eq("job_id", job_id).execute()

    await asyncio.to_thread(_do_call)


# -----------------------------
# API ENDPOINTS
# -----------------------------
@app.post("/api/edi/ingest", response_model=IngestKickoffResponse)
async def ingest_edi(payload: IngestPayload = Body(...)):
    """
    Demo-friendly async behavior:
      - Validate manifest
      - Create ingest_jobs row
      - Start detached Edge invocation
      - Return immediately (frontend continues UI)
    """
    _validate_ingest_payload(payload)

    job_id = str(uuid.uuid4())
    create_ingest_job_row(job_id, payload)

    # Start detached Edge invocation in background
    asyncio.create_task(invoke_edge_function_detached(job_id, payload))

    # Return immediately; frontend should NOT wait for processing
    return {"job_id": job_id, "status": "QUEUED"}


@app.get("/api/edi/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str):
    if not _is_valid_uuid(job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id")

    row = get_ingest_job_row(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": row.get("job_id"),
        "status": row.get("status"),
        "error": _stringify_error(row.get("error")),
        "result": row.get("result"),
    }


@app.get("/api/edi/dashboard")
async def get_dashboard_metrics():
    return {
        "kpis": {
            "openCount": 124,
            "atRisk": 42500,
            "exceptionRate": 0.12,
            "modelHealth": "Stable",
            "macroF1": 0.992,
        },
        "lastUpdatedAt": datetime.now().strftime("%H:%M:%S"),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)