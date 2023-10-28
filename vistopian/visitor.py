import json

import requests
from urllib.parse import urljoin
from urllib.request import urlretrieve, urlcleanup
from logging import getLogger
from functools import lru_cache
from typing import Optional

logger = getLogger(__name__)


class Visitor:
    def __init__(self, token: str):
        self.token = token

    def get_api_response(self, uri: str, params: Optional[dict] = None):

        url = urljoin("https://api.vistopia.com.cn/api/v1/", uri)

        if params is None:
            params = {}

        params.update({"api_token": self.token})

        logger.debug(f"Visiting {url}")

        response = requests.get(url, params=params).json()
        if response["status"] != "success":
            logger.error(f"访问 {url} 失败")
            logger.error(json.dumps(response, indent=4))
            raise RuntimeError
        if "data" not in response.keys():
            logger.error(f"访问 {url} 失败")
            logger.error(json.dumps(response, indent=4))
            raise RuntimeError

        return response["data"]

    @lru_cache()
    def get_catalog(self, id: int):
        response = self.get_api_response(f"content/catalog/{id}")
        return response

    @lru_cache()
    def get_user_subscriptions_list(self):
        data = []
        while True:
            response = self.get_api_response("user/subscriptions-list")
            data.extend(response["data"])
            break
        return data

    @lru_cache()
    def search(self, keyword: str) -> list:
        response = self.get_api_response("search/web", {'keyword': keyword})
        return response["data"]

    @lru_cache()
    def get_content_show(self, id: int):
        response = self.get_api_response(f"content/content-show/{id}")
        return response

    def save_show(self, id: int,
                  no_tag: bool = False, no_cover: bool = False,
                  episodes: Optional[set] = None):
        from pathlib import Path

        def download(url: str, fname: Path):
            import socket
            socket.setdefaulttimeout(30)
            try:
                urlretrieve(url, fname)
            except socket.timeout:
                count = 1
                while count <= 5:
                    try:
                        urlretrieve(url, fname)
                        break
                    except socket.timeout:
                        logger.warning(f"下载 {fname}，重试 {count} 次")
                        count += 1
                if count > 5:
                    logger.warning(f"下载 {fname} 失败")
                    fname.unlink()

        def download_m3u8(url: str, fname: Path):
            import m3u8
            import ffmpeg
            from concurrent.futures import ThreadPoolExecutor

            # 通过m3u8下载视频
            playlist = m3u8.load(url)
            ts_list = playlist.segments.uri
            ts_list = [urljoin(url, ts) for ts in ts_list]
            # 分段下载文件后进行合并
            folder = Path(fname).parent / Path(fname).stem
            folder.mkdir(exist_ok=True)
            # 并发下载ts文件
            logger.info(f"-->开始下载 {folder} ts 共 {len(ts_list)} ...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(download, ts_list, [folder / Path(ts).name for ts in ts_list])
            logger.info(f"-->下载完成 {folder} ts 文件")
            # 合并文件
            logger.info(f"-->开始合并 {fname} 文件...")
            with open(folder / 'filelist.txt', 'wb') as f:
                for ts in ts_list:
                    f.write(f"file '{(folder / Path(ts).name).absolute()}'\n".encode())
            # 使用ffmpeg合并ts文件, 生成mp4文件
            (ffmpeg.input(folder / 'filelist.txt', format='concat', safe=0)
             .output(filename=fname, codec='copy', loglevel='quite').run())
            logger.info(f"-->合并完成 {fname} 文件")
            # 删除临时文件
            for ts in ts_list:
                (folder / Path(ts).name).unlink(missing_ok=True)
            (folder / 'filelist.txt').unlink(missing_ok=True)
            folder.rmdir()

        catalog = self.get_catalog(id)
        series = self.get_content_show(id)
        logger.debug(f"catalog {json.dumps(catalog, indent=4)}")
        logger.debug(f"series {json.dumps(series, indent=4)}")

        show_dir = Path(catalog["title"])
        show_dir.mkdir(exist_ok=True)

        self.save_meta(catalog, download, series, show_dir)

        for part in catalog["catalog"]:
            for article in part["part"]:

                if episodes is not None and \
                        int(article["sort_number"]) not in episodes:
                    continue

                title = article["title"].replace("/", "\\")
                media_type = article["media_type_en"]
                if media_type == 'audio':
                    fname = show_dir / "{}.mp3".format(title)
                elif media_type == 'video':
                    fname = show_dir / "{}.mkv".format(title)
                else:
                    raise NotImplementedError

                if not fname.exists():
                    logger.info(f"开始下载 {fname} ...")
                    logger.debug(json.dumps(article, indent=2))

                    if media_type == 'audio':
                        download(article["media_key_full_url"], fname)
                        if not no_tag:
                            self.retag(str(fname), article, catalog, series)
                        if not no_cover:
                            self.retag_cover(str(fname), article, catalog, series)
                    else:
                        # 优选最高分辨率
                        best_quality = article['media_files'][0]['quality']
                        video_m3u8 = article['media_files'][0]['media_key_full_url']
                        for media_file in article['media_files']:
                            if int(media_file['quality']) > int(best_quality):
                                best_quality = media_file['quality']
                                video_m3u8 = media_file['media_key_full_url']
                        try:
                            download_m3u8(video_m3u8, fname)
                        except Exception as e:
                            import traceback
                            logger.error(f"下载 {fname} 失败")
                            logger.error(traceback.format_exc())
                            fname.unlink(missing_ok=True)
                            continue

                    logger.info(f"下载完成 {fname}")
                else:
                    logger.info(f"跳过已存在 {fname}")

    @staticmethod
    def save_meta(catalog, download, series, show_dir):
        # 下载 cover
        cover = show_dir / "cover.jpg"
        if not cover.exists():
            logger.info(f"开始下载 {cover} ...")
            download(catalog["background_img"], cover)
            logger.info(f"下载完成 {cover}")
        else:
            logger.info(f"跳过已存在 {cover}")
        # 生成简介
        desc = show_dir / "desc.txt"
        if not desc.exists():
            logger.info(f"开始生成 {desc} ...")
            with open(desc, "w") as f:
                f.write(series["share_desc"])
            logger.info(f"生成完成 {desc}")
        else:
            logger.info(f"跳过已存在 {desc}")
        # 生成演播者
        reader = show_dir / "reader.txt"
        if not reader.exists():
            logger.info(f"开始生成 {reader} ...")
            with open(reader, "w") as f:
                f.write(series["author"])
            logger.info(f"生成完成 {reader}")
        else:
            logger.info(f"跳过已存在 {reader}")

    def save_transcript(self, id: int, episodes: Optional[set] = None):

        from pathlib import Path

        catalog = self.get_catalog(id)

        show_dir = Path(catalog["title"])
        show_dir.mkdir(exist_ok=True)

        for part in catalog["catalog"]:
            for article in part["part"]:

                if episodes is not None and \
                        int(article["sort_number"]) not in episodes:
                    continue

                title = article["title"].replace("/", "\\")
                fname = show_dir / "{}.html".format(title)
                if not fname.exists():
                    logger.info(f"开始下载 {fname} ...")

                    urlretrieve(article["content_url"], fname)

                    with open(fname) as f:
                        content = f.read()

                    content = content.replace(
                        '="/assets/',
                        '="https://api.vistopia.com.cn/assets/'
                    )

                    with open(fname, "w") as f:
                        f.write(content)
                    logger.info(f"下载完成 {fname}")
                else:
                    logger.info(f"跳过已存在 {fname}")

                pdfname = show_dir / "{}.pdf".format(title)
                if not pdfname.exists():
                    logger.info(f"开始转换 {pdfname} ...")
                    import pdfkit
                    pdfkit.from_file(str(fname), str(pdfname))
                    logger.info(f"转换完成 {pdfname}")
                else:
                    logger.info(f"跳过已存在 {pdfname}")

    @staticmethod
    def retag(fname, article_info, catalog_info, series_info):

        import mutagen
        from mutagen.easyid3 import EasyID3

        try:
            track = EasyID3(fname)
        except mutagen.id3.ID3NoHeaderError:
            track = mutagen.File(fname, easy=True)
            track.add_tags()

        track["title"] = article_info["title"]
        track["album"] = series_info["title"]
        track["artist"] = series_info["author"]
        track["tracknumber"] = article_info["sort_number"]
        # track["tracksort"] = article_info["sort_number"]
        track["website"] = article_info["content_url"]
        track.save(v1=2)

    @staticmethod
    def retag_cover(fname, article_info, catalog_info, series_info):

        from mutagen.id3 import ID3, APIC

        @lru_cache()
        def _get_cover(url: str) -> bytes:
            cover_fname, _ = urlretrieve(url)
            with open(cover_fname, "rb") as fp:
                cover = fp.read()
            urlcleanup()
            return cover

        cover = _get_cover(catalog_info["background_img"])

        track = ID3(fname)
        track["APIC"] = APIC(encoding=3, mime="image/jpeg",
                             type=3, desc="Cover", data=cover)
        track.save()
