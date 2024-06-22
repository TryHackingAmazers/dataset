import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor

def is_valid(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_images(content, base_url):
    soup = BeautifulSoup(content, "html.parser")
    urls = []
    for img in soup.find_all("img"):
        img_url = img.attrs.get("src")
        if not img_url:
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
    return urls

def download(url, pathname):
    if not os.path.isdir(pathname):
        os.makedirs(pathname)
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
            imgs = get_all_images(content, url)
            print(len(imgs),"images")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(download, img, path) for img in imgs]
                for future in futures:
                    future.result()
            break
    
