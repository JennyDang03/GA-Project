# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 15:14:06 2023

@author: mathe
"""

#!/usr/bin/env python3

# Background tutorial on async programming with Python
# https://realpython.com/async-io-python/

# Requires Python 3.7 or newer. Tested with 3.8 and 3.9.

# Installation:
# pip install opencage asyncio aiohttp backoff
# pip install sys random time 
import sys, random, time
import csv
import backoff
import asyncio
from opencage.geocoder import OpenCageGeocode, AioHttpError
# from opencage.geocoder import InvalidInputError, RateLimitExceededError
# from opencage.geocoder import UnknownError
#import aiohttp

###############################################################################
api_key = "68929b2fc4fd4bfba2ec7ab266f9c602"
key = "68929b2fc4fd4bfba2ec7ab266f9c602"

base_infile  = "/home/mcs038/Documents/Pix_regressions/csv/location/aux_address/30k_files/aux_address"
base_outfile = "/home/mcs038/Documents/Pix_regressions/csv/location/aux_address/30k_files_latlong/aux_address"
total_files = 356 
start_file = 32
max_items = 0         # Set to 0 for unlimited - it was 100
num_workers = 24        # For 10 requests per second try 2-5 - it was 3
timeout = 1            # For individual HTTP requests. In seconds, default is 1
retry_max_tries = 1    # How often to retry if a HTTP request times out - it was 10
retry_max_time = 1    # Limit in seconds for retries - it was 60
sleep_btw_files = 60*60*24
###############################################################################



async def write_one_geocoding_result(geocoding_results, address, address_id):
  if geocoding_results != None and len(geocoding_results):
    geocoding_result = geocoding_results[0]
    row = [
      address_id,
      geocoding_result['geometry']['lat'],
      geocoding_result['geometry']['lng'],
      geocoding_result['confidence'],
      geocoding_result['formatted']
    ]
  else:
    row = [
      address_id,
      0, # not to be confused with https://en.wikipedia.org/wiki/Null_Island
      0,
      -1, # confidence values are 1-10 (lowest to highest), use -1 for unknown
      ''
    ]
    sys.stderr.write("not found, writing empty result: %s\n" % address)
  csv_writer.writerow(row)

# Backing off 0.4 seconds afters 1 tries calling function <function geocode_one_address
# at 0x10dbf5e50> with args ('14464 3RD ST # 4, 91423, CA, USA', '1780245') and kwargs {}
def backoff_hdlr(details):
    sys.stderr.write("Backing off {wait:0.1f} seconds afters {tries} tries "
                     "calling function {target} with args {args} and kwargs "
                     "{kwargs}\n".format(**details))

# https://pypi.org/project/backoff/
@backoff.on_exception(backoff.expo,
                      (asyncio.TimeoutError),
                      max_time=retry_max_time, # seconds
                      max_tries=retry_max_tries,
                      on_backoff=backoff_hdlr)
async def geocode_one_address(address, address_id):
  async with OpenCageGeocode(api_key) as geocoder:
    #<- Here is where I put the , language="pt-BR", no_annotations="1", limit="1", countrycode="br"
    geocoding_result = await geocoder.geocode_async(address, language="pt-BR", no_annotations="1", limit="1", countrycode="br")
    try:
      await write_one_geocoding_result(geocoding_result, address, address_id)
    except Exception as e:
      sys.stderr.write(e)

async def run_worker(worker_name, queue):
  sys.stderr.write("Worker %s starts...\n" % worker_name)
  while True:
    work_item = await queue.get()
    address_id = work_item['id']
    address = work_item['address']
    await geocode_one_address(address, address_id)
    queue.task_done()

async def main():
  assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
  ## 1. Read CSV into a Queue
  ##    Each work_item is an address an id. The id will be part of the output,
  ##    easy to add more settings. Named 'work_item' to avoid the words
  ##    'address' or 'task' which are used elsewhere
  ##
  ## https://docs.python.org/3/library/asyncio-queue.html
  ##
  
  queue = asyncio.Queue(maxsize=max_items)

  csv_reader = csv.reader(open(infile, 'r'))

  for row in csv_reader:
    work_item = {'id': row[0], 'address': row[1]}
    await queue.put(work_item)
    if queue.full():
      break

  sys.stderr.write("%d work_items in queue\n" % queue.qsize())

  ## 2. Create tasks workers. That is coroutines, each taks take work_items
  ##    from the queue until it's empty. Tasks run in parallel
  ##
  ## https://docs.python.org/3/library/asyncio-task.html#creating-tasks
  ## https://docs.python.org/3/library/asyncio-task.html#coroutine
  ##
  sys.stderr.write("Creating %d task workers...\n" % num_workers)
  tasks = []
  for i in range(num_workers):
    task = asyncio.create_task(run_worker(f'worker {i}', queue))
    tasks.append(task)

  ## 3. Now workers do the geocoding
  ##
  sys.stderr.write("Now waiting for workers to finish processing queue...\n")
  await queue.join()

  ## 4. Cleanup
  ##
  for task in tasks:
    task.cancel()

  sys.stderr.write("All done.\n")

for i in range(start_file,total_files):
    infile = base_infile + str(i) + ".csv"
    outfile = base_outfile + str(i) + ".csv"
    csv_writer = csv.writer(open(outfile, 'w', newline=''))
    asyncio.run(main())
    time.sleep(sleep_btw_files)




