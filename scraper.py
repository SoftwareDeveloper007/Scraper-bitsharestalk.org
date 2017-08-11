from selenium import webdriver
import csv
import threading
import time

def download(url, num_retries=3):

    isSuccess = True
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        driver = webdriver.Chrome(chrome_options=options)
        driver.get(url)
    except:
        driver.close()
        if num_retries > 0:
            download_selenium(url, num_retries-1)
        else:
            isSuccess = False
            driver = None

    output = {
        'driver': driver,
        'isSuccess': isSuccess
    }

    return output


class scraper_1():
    def __init__(self, url='https://bitsharestalk.org/index.php/board,5.0/sort,last_post/desc.html'):
        self.start_url = url

    def extract_urls(self):
        self.total_data = []
        output = download(self.start_url)
        if output['isSuccess']:
            driver = output['driver']
            rows = driver.find_elements_by_xpath("//table[@class='table_grid']/tbody/tr")
            for i, row in enumerate(rows):
                if i==0:
                    continue
                tds = row.find_elements_by_tag_name('td')
                title = tds[2].find_element_by_tag_name('a').text
                date = tds[4].text.split('\n')[0]
                link = row.find_elements_by_tag_name('td')[2].find_element_by_tag_name('a').get_attribute('href')
                self.total_data.append({
                    'title': title,
                    'date': date,
                    'link': link,
                })

            driver.close()
        else:
            time.sleep(10)
            self.extract_urls()

    def extract_subpage(self):
        self.total_data.reverse()

        filename = 'result_1.csv'
        self.output = open(filename, 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(self.output)
        header = ['Title', 'Text', 'Date', 'Link']
        self.writer.writerow(header)

        self.threads = []
        self.max_threads = 10

        while self.threads or self.total_data:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_data:
                thread = threading.Thread(target=self.extract_onepage)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

        self.total_data = self.failed_data
        while self.threads or self.total_data:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_data:
                thread = threading.Thread(target=self.extract_onepage)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

        self.output.close()

    def extract_onepage(self):
        self.failed_data = []
        data = self.total_data.pop()
        output = download(data['link'])

        if output['isSuccess']:
            sub_driver = output['driver']
            txt = sub_driver.find_element_by_css_selector('div.inner').text

            while '\n' in txt:
                txt = txt.replace('\n', '')

            row = [data['title'], txt, data['date'], data['link']]
            self.writer.writerow(row)
            sub_driver.close()
            print(data['link'])
        else:
            self.failed_data.append(data)

if __name__ == '__main__':
    app = scraper_1()
    app.extract_urls()
    app.extract_subpage()
