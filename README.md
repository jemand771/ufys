# ufys

The unnamed ffmpeg ytdl service

ufys is a microservice that takes in media urls from youtube, instagram, and all other platforms supported
by [yt-dlp](https://github.com/yt-dlp/yt-dlp) and returns a direct url to the video file.
If direct url extraction fails or doesn't yield any usable formats (ahem, reddit), ufys will download the video and
re-host it on an S3-compatible server (e.g.
minio)

check out the routes in [main.py](main.py) and the classes in [model.py](model.py) to see what endpoints and parameters
are supported
