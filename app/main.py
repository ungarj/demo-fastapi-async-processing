import aioredis
import asyncio
from dask.distributed import as_completed, Client
from fastapi import FastAPI, BackgroundTasks
from fastapi.logger import logger
import logging
import os
from random import random
import time
from uuid import uuid4

# this will be mapchete.commands.execute
from .mapchete import execute as mapchete_execute


uvicorn_logger = logging.getLogger("uvicorn.access")
logger.handlers = uvicorn_logger.handlers
if __name__ != "main":
    # logger.setLevel(uvicorn_logger.level)
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.DEBUG)


async def cancel_task(conn: aioredis.Connection, task_id: str):
    """ Send a task cancellation message. Does not check if the task is
        actually valid.
    """
    logger.info(f"cancel task {task_id}")
    # return await conn.lpush(f"cancel-{task_id}", "cancel")
    return await conn.set(f"status-{task_id}", "abort")


def get_redis():
    """ Helper to get a redis client.
    """
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    logger.info(f"get redis from url {redis_url}")
    return aioredis.from_url(
        redis_url,
        encoding="utf-8",
        decode_responses=True
    )


async def task_wrapper(task_id: str):
    """ Create a Job iterator through the mapchete_execute function. On every new finished task,
        check whether the task already got the abort status.
    """
    logger.info(f"Starting task {task_id}")
    scheduler = os.environ.get("DASK_SCHEDULER")
    if scheduler is None:
        raise ValueError("DASK_SCHEDULER environment variable must be set")
    redis = get_redis()
    async with redis.client() as conn:
        logger.debug("starting mapchete_execute")
        await conn.set(f"status-{task_id}", "started")
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
        for t in job:
            # determine if there is a cancel signal for this task
            logger.debug(f"looking for abort message")
            status = await conn.get(f"status-{task_id}")
            logger.debug(f"task status: {status}")
            if status == "abort":
                # By calling the job's cancel method, all pending futures will be cancelled.
                job.cancel()
                await conn.set(f"status-{task_id}", "cancelled")
                break
            # TODO update job state

app = FastAPI()


@app.get("/start/{task_id}")
def start(background_tasks: BackgroundTasks, task_id: str):
    task_id = task_id or uuid4().hex
    # send task to background to be able to quickly return a message
    background_tasks.add_task(task_wrapper, task_id)
    return {"task_id": task_id}


@app.get("/status/{task_id}")
async def status(task_id: str):
    redis = get_redis()
    async with redis.client() as conn:
        status = await conn.get(f"status-{task_id}")

    return {"task_id": task_id, "status": status or "unknown"}


@app.get("/cancel/{task_id}")
async def cancel(task_id: str):
    redis = get_redis()
    async with redis.client() as conn:
        status = await conn.get(f"status-{task_id}")

        if status == "started":
            await cancel_task(conn, task_id)

        status = await conn.get(f"status-{task_id}")

    return {"task_id": task_id, "status": status or "unknown"}
