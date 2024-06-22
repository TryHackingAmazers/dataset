import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import csv

def is_valid(url,):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_images(content, base_url, path):
    if not os.path.isdir(path):
        os.makedirs(path)
    soup = BeautifulSoup(content, "html.parser")
    urls = []
    csv_data = []
    for img in soup.find_all("img"):
        img_url = img.attrs.get("src")
        next = None
        if(parent := img.find_parent("a")):
            next = parent.attrs.get("href")
            if(next==None):continue
            next = urljoin(base_url, next)
        if not img_url or not next:
            # if img does not contain src attribute, just skip
            continue
        # make the URL absolute by joining domain with the URL that is just extracted
        img_url = urljoin(base_url, img_url)
        try:
            pos = img_url.index("?")
            img_url = img_url[:pos]
        except ValueError:
            pass
        if is_valid(img_url):
            urls.append(img_url)
        csv_data.append([img_url.split("/")[-1],next])
    with open(path+'/data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(csv_data)
    return urls

def download(url, pathname):
    response = requests.get(url, stream=True)
    # file_size = int(response.headers.get("Content-Length", 0))
    filename = os.path.join(pathname, url.split("/")[-1])
    with open(filename, "wb") as f:
        for data in response.iter_content(1024):
            f.write(data)
    time.sleep(1)

if __name__ == "__main__":
    url = "https://www.amazon.in/s?k=side+table+lamps&crid=214LPJM6IXQNP&sprefix=side+tablelamps%2Caps%2C197&ref=nb_sb_noss_2"
    path = "/home/rohan/hackonama/datasets/amazon/lamp"
    for i in range(1,200,1):
        time.sleep(1)
        response = requests.get(url)
        if(response.status_code == 200):
            content = response.content
            imgs = get_all_images(content, url, path)
            print(len(imgs),"images")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(download, img, path) for img in imgs]
                for future in futures:
                    future.result()
            break