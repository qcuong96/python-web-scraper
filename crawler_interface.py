import asyncio
import numpy as np 
from aiohttp import ClientSession
from multiprocessing import cpu_count 
from concurrent.futures import ProcessPoolExecutor, wait


class CrawlerInterface():

    def __init__(
        self,
        parse_html_function,
        max_concurrency=10,
        num_cores=(cpu_count() - 1),
    ):
        self.num_cores = num_cores
        self.max_concurrency = max_concurrency
        self.parse_html_function = parse_html_function

    
    async def fetch(self, url, session, *arg, **kwargs):
        async with session.get(url, *arg, **kwargs) as response:
            html_body = await response.text()
            return self.parse_html_function(html_body)
        
    async def bound_fetch(self, sem, url, session, *arg, **kwargs):
        # Getter function with semaphore.
        async with sem:
            return await self.fetch(
                url, 
                session, 
                *arg, 
                **kwargs,
            )

    async def asynce_run(self, urls, *arg, **kwargs):
        tasks = []
        # create instance of Semaphore
        sem = asyncio.Semaphore(self.max_concurrency)

        # Create client session that will ensure we dont open new connection
        # per each request.
        async with ClientSession() as session:
            for url in urls:
                # pass Semaphore and session to every GET request
                task = asyncio.ensure_future(
                    self.bound_fetch(
                        sem, 
                        url, 
                        session,
                        *arg,
                        **kwargs
                    )
                )
                
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            
            return responses

    def asyncio_wrapper(self, urls): 
        return asyncio.run(self.asynce_run(urls))
    
    def run(self, urls): 
        print(f"Running on {self.num_cores} cores")
        executor = ProcessPoolExecutor(max_workers=self.num_cores)
        tasks = [ 
            executor.submit(self.asyncio_wrapper, pages_for_task) 
            for pages_for_task in np.array_split(urls, self.num_cores) 
        ]

        doneTasks, _ = wait(tasks) 
    
        results = [
            task.result() 
            for task in doneTasks
        ]
        
        return sum(results, [])