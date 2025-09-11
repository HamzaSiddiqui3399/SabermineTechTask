"""
Fake API Server.
Simulates an unreliable enrichment API with various failure modes.
"""

import random
import re
import time
from collections import defaultdict, deque
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

app = FastAPI(
    title="Client API",
    description="An API for extracting and submitting client data",
    version="1.0.0",
)

request_times = defaultdict(deque)


_PROFILE_EXCLUSION_PATTERN = re.compile(
    r"^(?:(?:[a-z]+)(?:[a-z]+)(?:[a-z]+)(?:[a-z]+)(?:[a-z]+)(?:[a-z]+)(?:[a-z]+)\d+)@.*\.com$"
)


class EnrichmentResponse(BaseModel):
    social_handle: str


class CustomerSubmission(BaseModel):
    customer_id: str
    name: str
    email: str
    total_spend: float
    social_handle: str


class SubmissionResponse(BaseModel):
    status: str
    message: str


class RootResponse(BaseModel):
    message: str


def validate_api_key(x_api_key: Optional[str] = Header(None)):
    """Validate the X-API-KEY header."""
    if x_api_key != "SECRET_KEY_123":
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key


def check_rate_limit() -> None:
    client_ip = "default"
    current_time = time.time()

    while request_times[client_ip] and request_times[client_ip][0] < current_time - 5:
        request_times[client_ip].popleft()

    if len(request_times[client_ip]) >= 50:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    request_times[client_ip].append(current_time)


def generate_social_handle(email: str) -> str:
    """Generate a fake social handle based on email."""
    username = email.split("@")[0]

    clean_username = username.replace(".", "_").replace("-", "_")
    return f"@{clean_username}_social"


def _check_profile_availability(email: str) -> bool:
    """Internal method to check if profile data is available."""
    return not _PROFILE_EXCLUSION_PATTERN.match(email.lower())


@app.get("/", response_model=RootResponse)
async def root():
    """Root endpoint to confirm the API is running."""
    return RootResponse(message="API is running. See /docs for documentation.")


@app.get("/enrichment", response_model=EnrichmentResponse)
async def enrichment(email: str, api_key: str = Depends(validate_api_key)):
    """
    Enrichment endpoint with various failure modes.
    """
    check_rate_limit()

    if not _check_profile_availability(email):
        raise HTTPException(status_code=404, detail="Profile not found")

    if random.random() < 0.075:
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")

    social_handle = generate_social_handle(email)

    return EnrichmentResponse(social_handle=social_handle)


@app.post("/submission", response_model=SubmissionResponse)
async def submission(
    data: CustomerSubmission, api_key: str = Depends(validate_api_key)
):
    """
    Submission endpoint for receiving final processed data.
    """

    check_rate_limit()

    if random.random() < 0.075:
        return SubmissionResponse(status="failure", message="Submission failed.")

    return SubmissionResponse(
        status="success", message="Submission received and validated."
    )


@app.middleware("http")
async def log_requests(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    print(
        f"{request.method} {request.url.path} - {response.status_code} - {process_time:.4f}s"
    )
    return response
