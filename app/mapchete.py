import logging
import random
import time

from .executor import Executor, Job

logger = logging.getLogger(__name__)


def execute(
    mapchete_config,
    zoom=None,
    area=None,
    area_crs=None,
    bounds=None,
    bounds_crs=None,
    point=None,
    point_crs=None,
    tile=None,
    overwrite=False,
    mode="continue",
    concurrency="processes",
    executor_kwargs=None,
    multi=None,
    max_chunksize=None,
    multiprocessing_start_method=None,
    msg_callback=None,
    as_iterator=False,
):
    """Function which mimics mapchete.commands.execute"""
    executor_kwargs = executor_kwargs or {}
    # initializing takes 5 seconds
    logger.debug("initialize process")
    time.sleep(1)

    # find out it is 100 preprocessing tasks to be done
    preprocessing_tasks = list(range(100))

    # determine number of process tiles
    tiles_count = 512
    
    return Job(
        _execute,
        fargs=(
            preprocessing_tasks,
            tiles_count,
        ),
        executor_concurrency=concurrency,
        executor_kwargs=executor_kwargs,
        total=len(preprocessing_tasks) + tiles_count,
        as_iterator=as_iterator,
    )


def _execute(executor, preprocessing_tasks, tiles_count):
    # preprocessing 100 products
    for result in executor.as_completed(product_preprocess, preprocessing_tasks):
        logger.debug(result)
        yield result
    # wait for 10 seconds
    time.sleep(10)
    # process tiles
    for result in executor.as_completed(tile_process, range(tiles_count)):
        logger.debug(result)
        yield result


def product_preprocess(p):
    time.sleep(random.random())
    return p


def tile_process(i):
    time.sleep(2 * random.random())
    return i
