from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import uuid
import os
from job_processor import process_job
from utils import save_input_file, save_result_file, get_job_status, update_job_status, get_result_file_path

app = FastAPI()

@app.post("/v1/job/submit")
async def submit_job(background_tasks: BackgroundTasks, file_url: str):
    job_id = str(uuid.uuid4())
    update_job_status(job_id, "queued")
    background_tasks.add_task(process_job, job_id, file_url)
    return {"job_id": job_id, "status": "queued"}

@app.get("/v1/job/status/{job_id}")
async def job_status(job_id: str):
    status = get_job_status(job_id)
    return {"job_id": job_id, "status": status}

@app.get("/v1/job/result/{job_id}")
async def job_result(job_id: str):
    result_path = get_result_file_path(job_id)
    if not os.path.exists(result_path):
        raise HTTPException(status_code=404, detail="Result not found")
    return FileResponse(result_path, media_type="application/octet-stream", filename=os.path.basename(result_path))