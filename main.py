from crawler_interface import CrawlerInterface
from bs4 import BeautifulSoup


def parse_html(html_body):
    soup = BeautifulSoup(html_body, 'html.parser')
    title = soup.find_all('h1')

    title = ' '.join(title[0].text.split())

    return {
        'title': title
    }


if __name__ == '__main__':
    urls = ["https://crawler-test.com/titles/title_with_whitespace"]*100
    crawler = CrawlerInterface(parse_html_function=parse_html)
    results = crawler.run(urls)
    
    print(f"Results: {len(results)}")

    for result in results:
        print(result)