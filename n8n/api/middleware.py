"""
FastAPI middleware for metrics collection
"""
from fastapi import Request, Response
from fastapi.responses import JSONResponse
import time
import traceback
import logging
from typing import Callable
from .metrics import (
    api_request_duration, api_request_total, 
    update_memory_metrics, update_cpu_metrics
)

logger = logging.getLogger(__name__)

async def metrics_middleware(request: Request, call_next: Callable) -> Response:
    """
    Middleware to collect API metrics
    """
    # Skip metrics endpoint to avoid recursion
    if request.url.path == "/metrics":
        return await call_next(request)
    
    start_time = time.time()
    method = request.method
    endpoint = request.url.path
    
    try:
        # Call the actual endpoint
        response = await call_next(request)
        status = response.status_code
        
    except Exception as e:
        # Handle exceptions
        logger.error(f"Request failed: {method} {endpoint} - {str(e)}")
        logger.error(traceback.format_exc())
        status = 500
        response = JSONResponse(
            status_code=status,
            content={
                "error": "Internal server error",
                "message": str(e)
            }
        )
    
    # Record metrics
    duration = time.time() - start_time
    
    # Record request duration
    api_request_duration.labels(
        method=method,
        endpoint=endpoint,
        status=str(status)
    ).observe(duration)
    
    # Record request count
    api_request_total.labels(
        method=method,
        endpoint=endpoint,
        status=str(status)
    ).inc()
    
    # Update resource metrics periodically (every 100th request)
    if hash(f"{method}{endpoint}{start_time}") % 100 == 0:
        update_memory_metrics()
        update_cpu_metrics()
    
    # Add response headers
    response.headers["X-Response-Time"] = f"{duration:.3f}"
    
    return response

def setup_middleware(app):
    """
    Setup all middleware for the FastAPI app
    """
    app.middleware("http")(metrics_middleware)