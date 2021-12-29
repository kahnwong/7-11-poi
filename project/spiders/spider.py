import json

import pandas as pd
import scrapy
from tqdm import tqdm


class SevenElevenSpider(scrapy.Spider):
    name = "seven-eleven"

    def start_requests(self):
        url = "https://www.7eleven.co.th/api/v1/Store/GetStoreByCurrentLocation"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.7eleven.co.th/find-store",
            "Content-Type": "application/json;charset=utf-8",
            "Origin": "https://www.7eleven.co.th",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": "connect.sid=s%3Ahsa4v6Citja4xUK-XKB257avWAxYTYbi.aKKWQvXJqP2io8G024Vg9YTvvSf5cnjS%2BF6fwjtOjUQ; AWSELB=0DAF053F1AF98F84C0FE0822B5EABDDECA25343B9B41C04FB0785D2FDB2FA2F092FF4C8B6141C4876FA1B5431DB3AF20E7BF65BEEAFFFE858085A803AB9C45CE1FC980F782; AWSELBCORS=0DAF053F1AF98F84C0FE0822B5EABDDECA25343B9B41C04FB0785D2FDB2FA2F092FF4C8B6141C4876FA1B5431DB3AF20E7BF65BEEAFFFE858085A803AB9C45CE1FC980F782; install_active=true; connect.sid=s%3A2jTKUY9hZuynAUKjvRB7hFZ9jRArz3sb.t3g1%2FEN6BoOwWGl0dWptlv8cAbC7v8sa67z41jvgmXo; AWSELB=0DAF053F1AF98F84C0FE0822B5EABDDECA25343B9B41C04FB0785D2FDB2FA2F092FF4C8B61DE6FA8BEC2BCCBB9528546D738E89E30BE557F2096E030E4BAFC2F7980E36F8F; AWSELBCORS=0DAF053F1AF98F84C0FE0822B5EABDDECA25343B9B41C04FB0785D2FDB2FA2F092FF4C8B61DE6FA8BEC2BCCBB9528546D738E89E30BE557F2096E030E4BAFC2F7980E36F8F",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        ### loop through centroids
        df = pd.read_csv("data/input/hex-centroid-thailand-2.5km.csv")

        INCREMENT = 0
        START_INDEX = 1000
        END_INDEX = START_INDEX + INCREMENT
        latitudes = df["Y"].tolist()[START_INDEX:END_INDEX]
        longitudes = df["X"].tolist()[START_INDEX:END_INDEX]

        print(f"START_INDEX: {START_INDEX}")

        for latitude, longitude in tqdm(zip(latitudes, longitudes), total=INCREMENT):
            payload = {"latitude": latitude, "longitude": longitude, "products": []}

            yield scrapy.Request(
                url=url,
                method="POST",
                body=json.dumps(payload),
                headers=headers,
                callback=self.parse,
            )

    def parse(self, response):
        r = response.json()

        for i in r:
            yield i
