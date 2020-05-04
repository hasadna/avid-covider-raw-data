import requests
from common import upload_file


def static_image_url(lat, lng, zoom, width, height):
    return f'https://api.mapbox.com/styles/v1/wios/ck9spo9jo02p71ip3m6xfchxk/static/' +\
           f'{lat},{lng},{zoom},0/{width}x{height}@2x' +\
           f'?access_token=pk.eyJ1Ijoid2lvcyIsImEiOiJjazh4ZXZ6Z24wejdtM3JvN2F1MHdlc2Z4In0.vz7knGcRWWGE4LGOLx8c7g'


def create_preview_image():
    lat = 34.785
    lng = 32.075
    zoom = 11
    size = 680

    url = static_image_url(lat, lng, zoom, size, size)
    print('Fetching from', url)
    upload_file(requests.get(url).content, 'data/map_preview.png')


if __name__ == '__main__':
    create_preview_image()