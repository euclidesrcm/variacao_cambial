import requests, json
from datetime import datetime
from pathlib import Path

def get_api_response(url):
    return requests.get(url).json()


def main(file, url_api):
    data = get_api_response(url_api)
    
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    filename = f"{Path('storage/lnd')}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_usd_brl.json"
    url_api = 'https://economia.awesomeapi.com.br/json/daily/USD-BRL/'
    main(filename, url_api)