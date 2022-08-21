import requests

def pingLink(URL):
    response = requests.head(URL)
    assert(response.status_code == 200)





