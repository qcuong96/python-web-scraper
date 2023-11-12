import os
import hashlib
import asyncio
import numpy as np
from aiohttp import ClientSession
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, wait


class CrawlerInterfaceV2():
    """
    A class used to represent a Crawler Interface.

    ...

    Attributes
    ----------
    num_cores : int
        The number of cores to use for the crawler.
    max_concurrency : int
        The maximum number of concurrent requests.
    parse_html_function : callable
        The function to parse the HTML.
    store_function : callable, optional
        The function to store the parsed data.
    store_function_kwargs : dict, optional
        The keyword arguments for the store function.
    fetch_function_kwargs : dict, optional
        The keyword arguments for the fetch function.

    Methods
    -------
    fetch(url, session):
        Fetches the HTML from the URL and parses it. If the status code is not 200, it returns an empty dictionary.
    bound_fetch(sem, url, session, *arg, **kwargs):
        Fetches the HTML from the URL with a semaphore.
    asynce_run(urls, *arg, **kwargs):
        Runs the asynchronous tasks for the URLs.
    asyncio_wrapper(urls):
        Wraps the asynchronous run function.
    run(urls):
        Runs the crawler on the URLs. It uses a process pool executor to run the tasks in parallel.
    """

    logger = None

    def __init__(
        self,
        parse_html_function: callable,
        store_function: callable = None,
        handle_exception_function: callable = None,
        max_concurrency: int = 10,
        num_cores: int = (cpu_count() - 1),
        store_function_kwargs: dict = {},
        fetch_function_kwargs: dict = {},
        handle_exception_function_kwargs: dict = {},
        **kwargs
    ):
        """
        Constructs all the necessary attributes for the CrawlerInterface object.

        Parameters
        ----------
            parse_html_function : callable
                The function to parse the HTML.
            store_function : callable, optional
                The function to store the parsed data.
            handle_exception_function : callable, optional
                The function to handle exceptions.
            max_concurrency : int, optional
                The maximum number of concurrent requests.
            num_cores : int, optional
                The number of cores to use for the crawler.
            store_function_kwargs : dict, optional
                The keyword arguments for the store function.
                Ex: db_name, collection_name, etc.
            fetch_function_kwargs : dict, optional
                The keyword arguments for the fetch function.
                Ex: headers, proxies, verify, etc.
            handle_exception_function_kwargs : dict, optional
                The keyword arguments for the handle exception function.
                Ex: db_name, collection_name, etc.
        """

        self.num_cores = num_cores
        self.max_concurrency = max_concurrency
        self.parse_html_function = parse_html_function
        self.store_function = store_function
        self.store_function_kwargs = store_function_kwargs
        self.fetch_function_kwargs = fetch_function_kwargs

        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def __store_html_locally(self, html_result, output_dir="output"):
        """
        Stores HTML content locally with a unique name.

        Parameters
        ----------
        html_result : dict
            The result containing the URL and its HTML content.
        output_dir : str, optional
            The directory to store the HTML files.

        Returns
        -------
        str
            The path to the stored HTML file.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        url_hash = hashlib.sha1(html_result["url"].encode()).hexdigest()
        filename = f"{url_hash}.html"
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(html_result["html"])

        return file_path
    
    async def __fetch_link(self, link, session):
        """
        Fetches the HTML content of a link.

        Parameters
        ----------
        link : str
            The link to fetch.

        Returns
        -------
        dict
            The result containing the link and its HTML content.
        """
        if self.logger:
            self.logger.info(f"Fetching link: {link}...")

        try:
            async with session.get(link, **self.fetch_function_kwargs) as response:
                status = response.status

                if status != 200:
                    if self.logger:
                        self.logger.error(
                            f"Failed to fetch link: {link} with status code {status} with reason {response.reason}"
                        )
                    return {"link": link, "html": ""}

                html_body = await response.text()
                return {"link": link, "html": html_body}

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to fetch link: {link} with error {e}")

            return {"link": link, "html": ""}

    async def __fetch_page(self, url, session):
        """
        Fetches the HTML content of a page and extracts 100 links.

        Parameters
        ----------
        url : str
            The URL to fetch.

        Returns
        -------
        dict
            The result containing the URL, its HTML content, and a list of link results.
        """
        if self.logger:
            self.logger.info(f"Fetching page: {url}...")

        try:
            async with session.get(url, **self.fetch_function_kwargs) as response:
                status = response.status

                if status != 200:
                    if self.logger:
                        self.logger.error(
                            f"Failed to fetch page: {url} with status code {status} with reason {response.reason}"
                        )
                    return {}

                html_body = await response.text()

                try:
                    data = self.parse_html_function(html_body)
                except Exception as e:
                    if self.logger:
                        self.logger.error(
                            f"Fail to parse {url} with error {e}"
                        )

                    return {}
                
                links = [item["url"] for item in data]

                # Fetch HTML content for each link concurrently
                link_results = await asyncio.gather(
                    *[self.__fetch_link(link, session) for link in links]
                )

                # Store HTML content locally
                for link_result in link_results:
                    if link_result["html"]:
                        self.__store_html_locally(html_result=link_result)

                return data

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to fetch page: {url} with error {e}")

            return {}

    async def __bound_fetch_page(self, sem, url, session):
        # Getter function with semaphore.
        async with sem:
            return await self.__fetch_page(url, session)

    async def asynce_run_pages(self, urls):
        """
        Runs the asynchronous tasks for fetching pages.

        Parameters
        ----------
        urls : list
            The list of URLs to fetch.

        Returns
        -------
        list
            The list of results, each containing the URL, its HTML content, and link results.
        """
        tasks = []
        sem = asyncio.Semaphore(self.max_concurrency)

        async with ClientSession() as session:
            for url in urls:
                task = asyncio.ensure_future(
                    self.__bound_fetch_page(sem, url, session)
                )
                tasks.append(task)

            responses = await asyncio.gather(*tasks)

            return responses

    def run_pages(self, urls):
        """
        Runs the crawler on the pages. It uses a process pool executor to run the tasks in parallel.

        Parameters
        ----------
        urls : list
            The list of URLs to fetch.

        Returns
        -------
        list
            The list of results, each containing the URL, its HTML content, and link results.
        """
        executor = ProcessPoolExecutor(max_workers=self.num_cores)
        tasks = [
            executor.submit(self.__asyncio_wrapper, pages_for_task)
            for pages_for_task in np.array_split(urls, self.num_cores)
        ]

        done_tasks, _ = wait(tasks)

        results = [task.result() for task in done_tasks]

        return sum(results, [])