import aioredis
import asyncio
from dask.distributed import as_completed, Client
from fastapi import FastAPI, BackgroundTasks
import logging
import os
from random import random
import time
from uuid import uuid4

# this will be mapchete.commands.execute
from .mapchete import execute as mapchete_execute

uvicorn_logger = logging.getLogger("uvicorn.access")
logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
sh.setFormatter(formatter)
if __name__ != "main":
    logger.setLevel(uvicorn_logger.level)
    sh.setLevel(uvicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)
    sh.setLevel(logging.DEBUG)
logger.addHandler(sh)


async def cancel_task(conn: aioredis.Connection, job_id: str):
    """ Send a task cancellation message. Does not check if the task is
        actually valid.
    """
    logger.info(f"cancel job {job_id}")
    # return await conn.lpush(f"cancel-{job_id}", "cancel")
    return await conn.set(f"status-{job_id}", "abort")


def get_redis():
    """ Helper to get a redis client.
    """
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    # logger.info(f"get redis from url {redis_url}")
    return aioredis.from_url(
        redis_url,
        encoding="utf-8",
        decode_responses=True
    )


async def task_wrapper(job_id: str):
    """ Create a Job iterator through the mapchete_execute function. On every new finished task,
        check whether the task already got the abort status.
    """
    logger.info(f"Starting task {job_id}")
    scheduler = os.environ.get("DASK_SCHEDULER")
    if scheduler is None:
        raise ValueError("DASK_SCHEDULER environment variable must be set")
    redis = get_redis()
    async with redis.client() as conn:
        logger.debug("starting mapchete_execute")
        await conn.set(f"status-{job_id}", "started")
        # Mapchete now will initialize the process and prepare all the tasks required.
        job = mapchete_execute(
            None,
            as_iterator=True,
            concurrency="dask",
            executor_kwargs=dict(dask_scheduler=scheduler)
        )
        logger.debug(f"created {job}")
        # By iterating through the Job object, mapchete will send all tasks to the dask cluster and
        # yield the results.
        for i, t in enumerate(job):
            logger.debug(f"job {job_id} task {i + 1}/{len(job)} finished")
            # determine if there is a cancel signal for this task
            status = await conn.get(f"status-{job_id}")
            if status == "abort":
                logger.debug(f"abort status caught: {status}")
                # By calling the job's cancel method, all pending futures will be cancelled.
                job.cancel()
                await conn.set(f"status-{job_id}", "cancelled")
                return
            # TODO update job state
        # task finished successfully
        conn.set(f"status-{job_id}", "finished")

app = FastAPI()


@app.get("/start/{job_id}")
def start(background_tasks: BackgroundTasks, job_id: str):
    job_id = job_id or uuid4().hex
    # send task to background to be able to quickly return a message
    background_tasks.add_task(task_wrapper, job_id)
    return {"job_id": job_id}


@app.get("/status/{job_id}")
async def status(job_id: str):
    redis = get_redis()
    async with redis.client() as conn:
        status = await conn.get(f"status-{job_id}")
    return {"job_id": job_id, "status": status or "unknown"}


@app.get("/cancel/{job_id}")
async def cancel(job_id: str):
    redis = get_redis()
    async with redis.client() as conn:
        status = await conn.get(f"status-{job_id}")
        if status == "started":
            await cancel_task(conn, job_id)
        status = await conn.get(f"status-{job_id}")
    return {"job_id": job_id, "status": status or "unknown"}
