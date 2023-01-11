import datetime
import json
import logging
import random
import re
import time
from urllib import parse
import redis
import pymongo
import pytz
import scrapy
from lxml import etree
from scrapy.http import HtmlResponse
from FacebookContentImage.items import FacebookcontentimageItem, FacebookcontentphotoItem
from FacebookContentImage.settings import MONGO_HOST, MONGO_USERNAME, MONGO_PASSWORD, MONGO_PORT, IMAGES_STORE
from FacebookContentImage.tools import get_facebook_url


# REDIS_HOST = "192.168.3.47"
REDIS_HOST = "103.241.166.250"
REDIS_PORT = 6379


class FacebookcontentimageSpider(scrapy.Spider):
    name = 'facebookcontentimage'
    # allowed_domains = ['facebook.com']
    # start_urls = ['http://facebook.com/']
    beijing = pytz.timezone('Asia/Shanghai')
    date_str = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=8)
    times = date_str.astimezone(beijing).isoformat()

    # def start_requests(self):
    #     conn = f'mongodb://{parse.quote_plus(MONGO_USERNAME)}:{parse.quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin&authMechanism=SCRAM-SHA-256'
    #     self.client = pymongo.MongoClient(conn)
    #
    #     data_lst = self.client["potential_buffer"]["facebook_image"].find({"url": {"$regex": "story_fbid="}})
    #     print(data_lst.count())
    #     for data in data_lst[0:100]:
    #         # content_url = "http://httpbin.org/get"
    #         # content_url = "https://www.facebook.com/permalink.php?story_fbid=2787143381295641&id=100000000202403&substory_index=0"
    #         # content_url = "https://www.facebook.com/permalink.php?story_fbid=4572263482783613&id=100000000202403"
    #
    #         # content_url = "https://www.facebook.com/permalink.php?story_fbid=2646752812001366&id=100000000202403"
    #
    #         # content_url = "https://www.facebook.com/permalink.php?story_fbid=4727126483963978&id=100000000202403"
    #         content_url = data["url"]
    #         print(data_lst.index(data))
    #         print(content_url)
    #         headers = {
    #             'authority': 'www.facebook.com',
    #             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    #             'accept-language': 'zh-CN,zh;q=0.9',
    #             'cache-control': 'no-cache',
    #             'cookie': 'fr=04fG66KDD9PucCG48..BjmszE.nC.AAA.0.0.BjmszE.AWW5K2HgANg; sb=xMyaYxwbJxbKZMVK3SejXLbl; datr=xMyaY9jXBO3FXFImEHdQtZXx; wd=1009x969; fr=04fG66KDD9PucCG48..BjmszE.nC.AAA.0.0.Bjms1s.AWX8qZKQ-Jo',
    #             'pragma': 'no-cache',
    #             'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
    #             'sec-ch-ua-mobile': '?0',
    #             'sec-ch-ua-platform': '"Windows"',
    #             'sec-fetch-dest': 'document',
    #             'sec-fetch-mode': 'navigate',
    #             'sec-fetch-site': 'none',
    #             'sec-fetch-user': '?1',
    #             'upgrade-insecure-requests': '1',
    #             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    #         }
    #
    #         yield scrapy.Request(content_url, headers=headers, callback=self.parse_url, meta={
    #             "post_data": data
    #         }, dont_filter=True)

    def start_requests(self):

        # conn = f'mongodb://{parse.quote_plus(MONGO_USERNAME)}:{parse.quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin&authMechanism=SCRAM-SHA-256'
        # self.client = pymongo.MongoClient(conn)
        #
        # data_lst = self.client["potential_buffer"]["facebook_image"].find({"url": {"$regex": "story_fbid="}})
        data_lst = [
            {
                # "url": "https://www.facebook.com/moonly.wong/videos/703344290952102/"
                "url": "https://www.facebook.com/HappyAmyKitchen/videos/1420317241346710/"
            }
        ]
        # print(data_lst.count())
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;"
                      "q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "max-age=0",
            "sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            'sec-ch-ua-platform': '"Windows"',
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                          "KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
        }

        for data in data_lst[0:1]:
            logging.info(data['url'])

            # if '/photos/' in data["url"]:
            #
            #     story_fbid_lst = data['url'].split('/')
            #     story_fbid = story_fbid_lst[-1]
            #     if not story_fbid:
            #         story_fbid = story_fbid_lst[-2]
            #     id = re.findall(r'facebook.com/(\d+)/', data['url'])[0]
            #
            #     data['url'] = f"https://www.facebook.com/permalink.php?story_fbid={story_fbid}&id={id}"

            r_field, cookie_data = self.get_redis_data(0)

            params = get_facebook_url.GetFacebookUserUrl().deal_url(data['url'], cookie_data['cookie'])
            platform_id = params.get('value')

            logging.info('page_or_user is ' + params.get('name'))

            if platform_id:
                if "/photo" in data['url']:
                    r_field, cookie_data = self.get_redis_data(11)
                    logging.info(data['url'])
                    yield scrapy.Request(data['url'], headers=headers, callback=self.parse_photo_url,
                                         cookies=cookie_data['cookie'],
                                         meta={
                                             'cookie': cookie_data['cookie'], 'cookie_data': cookie_data,
                                             'user_id': params.get('value'),
                                         })
                elif "video" in data['url']:

                    r_field, cookie_data = self.get_redis_data(11)
                    logging.info(data['url'])
                    yield scrapy.Request(data['url'], headers=headers, callback=self.parse_content_video,
                                         cookies=cookie_data['cookie'],
                                         meta={
                                             'cookie': cookie_data['cookie'], 'cookie_data': cookie_data,
                                             'user_id': params.get('value'),
                                         })

                elif params.get('name') == 'user_id':
                    r_field, cookie_data = self.get_redis_data(11)
                    logging.info(data['url'])
                    yield scrapy.Request(data['url'], headers=headers, callback=self.parse_user_content_1,
                                         cookies=cookie_data['cookie'],
                                         meta={
                                             'cookie': cookie_data['cookie'], 'cookie_data': cookie_data,
                                             'user_id': params.get('value'),
                                         })

                elif params.get('name') == 'page_id':
                    r_field, cookie_data = self.get_redis_data(12)

                    yield scrapy.Request(data['url'], headers=headers, cookies=cookie_data['cookie'], meta={
                        'user_id': params.get('value')
                    }, callback=self.parse_page_content_1)

                else:
                    pass
                    # str_dict = {
                    #     'data_status': 'error',
                    #     'mm_status': 'error',
                    #     'crawl_remark': 'URL invalid'
                    # }
                    #
                    # myset_task = get_facebook_url.MongodbInit('34.150.113.172', 27017, 'crawler_config',
                    #                                           self.task_type, "devkooler01",
                    #                                           "Wyx!$rmf!g@k39r").get_myset()
                    # try:
                    #     get_facebook_url.MongodbProcess().process_update(myset_task,
                    #                                                      {'local_job_id': self.local_job_id,
                    #                                                       'task_id': job_uid},
                    #                                                      {'$set': str_dict})
                    #
                    #     logging.info('task is sucess !')
                    # except Exception as e:
                    #     logging.info('task is fail !' + e)
            else:
                pass
                # str_dict = {
                #     'data_status': 'error',
                #     'mm_status': 'error',
                #     'crawl_remark': 'URL invalid'
                # }
                #
                # myset_task = get_facebook_url.MongodbInit('34.150.113.172', 27017, 'crawler_config', self.task_type,
                #                                           "devkooler01",
                #                                           "Wyx!$rmf!g@k39r").get_myset()
                # try:
                #     get_facebook_url.MongodbProcess().process_update(myset_task, {'local_job_id': self.local_job_id,
                #                                                                   'task_id': job_uid},
                #                                                      {'$set': str_dict})
                #     logging.info(self.local_job_id)
                #     logging.info('task is sucess !')
                # except Exception as e:
                #     logging.info('task is fail !' + e)

    def parse_url(self, response):
        if "login" not in response.url:
            item = FacebookcontentimageItem()
            # image_url = response.xpath("//meta[@property='og:image']/@content").extract()
            #
            # print(image_url)

            # 寻找贴文是否有图片
            content_text = response.xpath("//div[@class='_4-u2 mbm _4mrt _5v3q _7cqq _4-u8']").extract()

            # 贴文时间戳
            publish_time = response.xpath("//abbr/@data-utime")

            html_obj = etree.HTML(content_text[0])
            image_url_lst = html_obj.xpath("//a[@rel='theater']/@data-ploi")
            print(image_url_lst, len(image_url_lst))
            self.client["potential_buffer"]["facebook_image"].update(
                {
                    "data.content_id": response.meta["post_data"]["data"]["content_id"],
                    "ref_date_time": {"$exists": 0}
                },
                {
                    "$set": {
                        "ref_date_time": datetime.datetime.now()
                    }
                }
            )
            if image_url_lst:

                beijing = pytz.timezone('Asia/Shanghai')
                date_1 = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=8)
                image_info_lst = list()

                if "potential" in response.meta["post_data"]["type"]:
                    bucket_name = "potential_buffer"
                elif "kooler" in response.meta["post_data"]["type"]:
                    bucket_name = "kooler_buffer"
                elif "brand" in response.meta["post_data"]["type"]:
                    bucket_name = "brand_buffer"
                else:
                    raise ValueError("spider parse_url bucket_name")

                for image in image_url_lst:
                    content_img_id = re.findall(r"_(.*?)_", image)[0]
                    content_img_url = image
                    image_path = IMAGES_STORE + bucket_name + '/facebook/' + 'content/img' + '/%s' % str(response.meta["post_data"]["data"]["content_id"]) + '/%s' % str(content_img_id) + '.jpg'
                    image_info_lst.append(
                        {
                            "id": content_img_id,
                            "url": content_img_url,
                            "path": image_path
                        }
                    )
                try:
                    time1 = datetime.datetime.utcfromtimestamp(int(publish_time)).replace(microsecond=0) + datetime.timedelta(hours=8)
                    p_time = time1.astimezone(beijing).isoformat()
                except Exception as e:
                    p_time = ""

                crawl_times = date_1.astimezone(beijing).isoformat()
                item["type"] = response.meta["post_data"]["type"]
                item["channel"] = "facebook"
                item["url"] = response.url
                item["publish_time"] = p_time
                item["platform_id"] = response.meta["post_data"]["platform_id"]
                item["crawl_time"] = crawl_times
                item["store_time"] = crawl_times
                item["crawl_time_log"] = [
                    crawl_times
                ]
                item["store_time_log"] = [
                    crawl_times
                ]
                brand_mention_str_lst = re.findall(r'@(\w+)', response.meta["post_data"]["data"]["content"])

                if brand_mention_str_lst:
                    brand_mention = [
                        {
                            "type": "string", "value": brand_mention_str_lst
                        },
                        {
                            "type": "link", "value": []
                        },
                        {
                            "type": "img", "value": []
                        }
                    ]
                else:
                    brand_mention = []

                item["data"] = {
                    "content_id": response.meta["post_data"]["data"]["content_id"],
                    "homepage_url": "",
                    "hash_tag": re.findall(r'#(\w+)', response.meta["post_data"]["data"]["content"]),
                    "lang": "",
                    "title": "",
                    "sub_title": "",
                    "content": response.meta["post_data"]["data"]["content"],
                    "other_content": response.meta["post_data"]["data"]["other_content"],
                    "brand_mention": brand_mention,
                    "content_level": "0",
                    "share_content": response.meta["post_data"]["data"]["share_content"],
                    "image_info": image_info_lst,
                    "video_info": response.meta["post_data"]["data"]["video_info"],
                    "sn_interact_num": response.meta["post_data"]["data"]["sn_interact_num"]
                }

                item['col'] = bucket_name

                # yield item

        else:
            self.crawler.engine.close_spider(self, "need cookie !")

    def parse_user_content_1(self, response):
        item = FacebookcontentimageItem()
        # with open('first_user.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        try:
            text_d = re.findall(r'"__bbox":{"complete":true,"result":{"data":{"node":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            print('---', e)
            text_d = []

        if not text_d:
            try:
                text_d = re.findall(r'"__bbox":{"complete":true,"result":{"data":{"nodes":(.*?)\}\}\]\]', response.text)
            except Exception as e:
                text_d = []

        try:
            text_d_1 = re.findall(r'"__bbox":{"complete":false,"result":{"data":{"node":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            text_d_1 = []

        try:
            text_d_2 = re.findall(r'{"__bbox":{"complete":false,"result":{"data":{"currMedia":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            text_d_2 = []

        text_d += text_d_1
        text_d += text_d_2

        print('text_d----->', text_d)

        for i in text_d:
            try:
                # j_text = re.findall(r'__bbox":(.*?)\}\}\]\],\[', i)[0] + '}'
                j_text = '{"complete":true,"result":{"data":{"node":' + i + '}'
            except Exception as e:
                j_text = ''

            # print("j_text------>", j_text)

            # with open('user_lst.json.', 'w', encoding='utf-8') as f:
            #     f.write(j_text)
            # --------解析第一页的内容 start----------
            first_data = json.loads(j_text)
            try:
                v_text = first_data["result"]["data"]["node"][0]
            except Exception as e:
                v_text = ''

            if not v_text:

                try:
                    v_text = first_data["result"]["data"]["node"]
                except Exception as e:
                    v_text = first_data["result"]["data"]["user"]["timeline_feed_units"]["edges"][0]["node"]

            # 贴文内容
            try:
                content_text = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"]["message"][
                        "story"]["message"]["text"]
            except Exception as e:
                content_text = ""
            # # print('content_text_1---------', content_text)

            # feedback_id 获取
            try:
                feedback_id = v_text["feedback"]["id"]
            except Exception as e:
                feedback_id = ''
            # with open('edge.json', 'w', encoding="utf-8") as f:
            #     f.write(json.dumps(first_data))
            # 贴文ID
            try:
                content_id = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "subscription_target_id"]
            except Exception as e:
                content_id = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                    "subscription_target_id"]

            # 贴文图片
            content_img_info_lst = list()
            try:
                media_temp = v_text["comet_sections"]["content"]["story"]["attachments"][0]["styles"]
            except Exception as e:
                media_temp = {}
            if not media_temp:
                try:
                    media_temp = v_text["comet_sections"]["content"]["story"]["attachments"][0]["style_type_renderer"]
                except Exception as e:
                    media_temp = {}
            if not media_temp:
                try:
                    media_temp = v_text["comet_sections"]["content"]["story"]["attached_story"]["attachments"][0][
                        "styles"]
                except Exception as e:
                    media_temp = {}

            try:
                content_img_lst = media_temp["attachment"]["all_subattachments"]["nodes"]
            except Exception as e:
                content_img_lst = []

            if content_img_lst:

                try:
                    for content_img in content_img_lst:
                        content_img_id = content_img["media"]["id"]
                        if content_img["media"].get("photo_image"):

                            content_img_url = content_img["media"]["photo_image"]["uri"]

                        elif content_img["media"].get("image"):
                            content_img_url = content_img["media"]["image"]["uri"]

                        image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                            content_id) + '/%s' % str(content_img_id) + '.jpg'
                        content_img_info_lst.append(
                            {
                                "id": content_img_id,
                                "url": content_img_url,
                                "path": image_path
                            }
                        )
                except Exception as e:
                    print('post_img is none !', e)
                    # content_img_info_lst.append({"id": "", "url": "", "path": ""})
            else:
                try:
                    content_img_id = media_temp["attachment"]["media"]["id"]
                    if media_temp["attachment"]["media"].get("photo_image"):

                        content_img_url = media_temp["attachment"]["media"]["photo_image"]["uri"]

                    elif media_temp["attachment"]["media"].get("image"):
                        content_img_url = media_temp["attachment"]["media"]["image"]["uri"]

                    image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                        content_id) + '/%s' % str(content_img_id) + '.jpg'
                    content_img_info_lst.append(
                        {
                            "id": content_img_id,
                            "url": content_img_url,
                            "path": image_path
                        }
                    )
                except Exception as e:
                    print('post_img is none !', e)
                    # content_img_info_lst.append({"content_img_id": "", "content_img_url": ""})

            # 转载链接
            try:
                share_link = media_temp["attachment"]["story_attachment_link_renderer"][
                    "attachment"]["url"]
            except Exception as e:
                share_link = ''

            if not share_link:
                try:
                    share_link = \
                        first_data["result"]["node"]["comet_sections"]["content"]["story"]["attachments"][0][
                            "style_type_renderer"]["attachment"]["url"]
                except Exception as e:
                    share_link = ''

            if not share_link:
                try:
                    share_link = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["url"]
                except Exception as e:
                    share_link = ""

            if not share_link:
                share_link = ""

            # 帖子视频
            content_video_info_lst = list()
            try:
                content_video_id = media_temp["attachment"]["media"]["videoId"]
            except Exception as e:
                # print(e)
                content_video_id = ''
            # print('content_video_id---->', content_video_id)
            try:
                content_video_url = media_temp["attachment"]["media"]["dash_prefetch_resources"]["video"][-1]["url"]
            except Exception as e:
                content_video_url = ''

            if not content_video_url:
                try:

                    content_video_url = media_temp["attachment"]["media"]["playable_url"]
                except Exception as e:
                    content_video_url = ''

            if content_video_id or content_video_url:
                content_video_info_lst.append(
                    {
                        "id": content_video_id,
                        "url": content_video_url,
                        "path": "E:\\FacebookKolImg\\content\\video\\{}\\{}.mp4".format(content_id,
                                                                                      content_video_id)
                    }
                )

            # 帖子创建时间
            try:
                content_time = \
                    v_text["comet_sections"]["context_layout"]["story"]["comet_sections"][
                        "metadata"][0][
                        "story"]["creation_time"]
            except Exception as e:
                content_time = ''
            # 帖子的url
            content_url = \
                v_text["comet_sections"]["context_layout"]["story"]["comet_sections"]["metadata"][
                    0][
                    "story"]["url"]

            # 帖子的点赞数
            try:
                content_like_num = \
                    v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "reaction_count"]["count"]
            except Exception as e:
                content_like_num = \
                    v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "reaction_count"]["count"]

            # 帖子的评论数
            try:
                comment_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "comments_count_summary_renderer"]["feedback"]["comment_count"]["total_count"]
            except Exception as e:
                comment_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                    "comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "comments_count_summary_renderer"]["feedback"]["comment_count"]["total_count"]
            # 帖子的分享数
            try:
                share_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "share_count"]["count"]
            except Exception as e:
                share_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                    "comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "share_count"]["count"]

            # 转发内容待定
            try:
                shard_text = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"][
                        "attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["message"]["story"]["message"]["text"]
            except Exception as e:
                shard_text = ''
                # print('shard_text----->', e)

            if not shard_text:
                try:
                    shard_text = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                        "attachment_target_renderer"]["attachment"]["target"]["message"]["text"]
                except Exception as e:
                    shard_text = ""

            # 转发的user_id
            try:
                sub_user_id = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"][
                        "attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["title"]["story"]["actors"][0]["id"]
            except Exception as e:
                sub_user_id = ""

            if not sub_user_id:
                try:
                    sub_user_id = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                        "attachment_target_renderer"]["attachment"]["target"]["comet_sections"]["attached_story_title"][
                        "story"]["actors"][0]["id"]
                except Exception as e:
                    sub_user_id = ""

            # 转发内容的图片
            share_content_img_info_lst = list()
            try:
                share_img_lst = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"][
                        "attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["attached_story_layout"]["story"][
                        "attachments"][0][
                        "style_type_renderer"]["attachment"]["all_subattachments"]["nodes"]
            except Exception as e:
                share_img_lst = []

            if not share_img_lst:
                try:
                    share_content_img = \
                        v_text["comet_sections"]["content"]["story"]["comet_sections"][
                            "attached_story"][
                            "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                            "attachments"][0][
                            "style_type_renderer"]["attachment"]
                    share_content_img_id = share_content_img["media"]["id"]
                    try:
                        share_content_img_url = share_content_img["media"]["viewer_image"]["uri"]
                    except Exception as e:
                        share_content_img_url = ''
                    if not share_content_img_url:
                        share_content_img_url = share_content_img["media"]["photo_image"]["uri"]
                    share_content_img_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                        content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                    share_content_img_info_lst.append(
                        {
                            "id": share_content_img_id,
                            "url": share_content_img_url,
                            "path": share_content_img_path
                        }
                    )
                except Exception as e:
                    print('share post_img is none !', e)
            else:
                try:

                    for share_content_img in share_img_lst:
                        share_content_img_id = share_content_img["media"]["id"]
                        try:
                            share_content_img_url = share_content_img["media"]["viewer_image"]["uri"]
                        except Exception as e:
                            share_content_img_url = ''
                        if not share_content_img_url:
                            share_content_img_url = share_content_img["media"]["photo_image"]["uri"]
                        share_content_img_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                            content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                        share_content_img_info_lst.append(
                            {
                                "id": share_content_img_id,
                                "url": share_content_img_url,
                                "path": share_content_img_path
                            }
                        )
                except Exception as e:
                    print('post_img is none !', e)

            try:
                share_content_img_id = \
                v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                    "attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"][
                    "media"]["preferred_thumbnail"]["id"]
                share_content_img_url = \
                v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                    "attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"][
                    "media"]["preferred_thumbnail"]["image"]["url"]
            except Exception as e:
                share_content_img_id = ""
                share_content_img_url = ""

            if share_content_img_url:
                share_content_img_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                share_content_img_info_lst.append(
                    {
                        "id": share_content_img_id,
                        "url": share_content_img_url,
                        "path": share_content_img_path
                    }
                )

            # 转发的视频
            share_content_video_info_lst = list()
            try:
                share_content_video_id = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"][
                        "attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["attached_story_layout"]["story"][
                        "attachments"][0][
                        "style_type_renderer"]["attachment"]["media"]["videoId"]
                share_content_video_url = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"][
                        "attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["attached_story_layout"]["story"][
                        "attachments"][0][
                        "style_type_renderer"]["attachment"]["media"]["dash_prefetch_resources"]["video"][
                        -1][
                        "url"]
                share_video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(share_content_video_id) + '.mp4'
                share_content_video_info_lst.append(
                    {
                        "id": share_content_video_id,
                        "url": share_content_video_url,
                        "path": share_video_path
                    }
                )
            except Exception as e:
                print("share video is download error !")

            if not share_content_video_url:
                try:
                    share_content_video_id = \
                        v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                            "attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"][
                            "attachment"][
                            "media"]["videoId"]
                    share_content_video_url = \
                        v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"][
                            "attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"][
                            "attachment"][
                            "media"]["playable_url"]
                except Exception as e:
                    share_content_video_id = ""
                    share_content_video_url = ""

            if share_content_video_url:
                share_video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(share_content_video_id) + '.mp4'
                share_content_video_info_lst.append(
                    {
                        "id": share_content_video_id,
                        "url": share_content_video_url,
                        "path": share_video_path
                    }
                )

            # 情绪反应人处理
            r_lst = list()
            reactions_count = dict()
            try:
                reactions_lst = \
                    v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "top_reactions"]["edges"]
            except Exception as e:
                reactions_lst = []

            if not reactions_lst:
                try:
                    reactions_lst = \
                        v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                            "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                            "comet_ufi_summary_and_actions_renderer"]["feedback"][
                            "top_reactions"]["edges"]
                except Exception as e:
                    reactions_lst = \
                        v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                            "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                            "comet_ufi_summary_and_actions_renderer"]["feedback"]['cannot_see_top_custom_reactions'][
                            "top_reactions"]["edges"]

            for reactions in reactions_lst:
                if reactions["node"].get('id'):
                    if reactions["node"].get("id") == '1635855486666999':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '1678524932434102':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '444813342392137':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '613557422527858':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '115940658764963':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '478547315650144':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '908563459236466':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )

                elif reactions["node"].get('reaction_type'):
                    if reactions["node"].get('reaction_type') == 'LIKE':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'LOVE':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'ANGRY':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'CARE':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'HAHA':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'WOW':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'SAD':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count

                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
            try:
                time1 = datetime.datetime.utcfromtimestamp(int(content_time)).replace(
                    microsecond=0) + datetime.timedelta(hours=8)

                p_time = time1.astimezone(self.beijing).isoformat()
            except Exception as e:
                p_time = ""
            # print('p_time---->', p_time)

            if sub_user_id:
                share_content = [
                    {
                        "type": "",
                        "platform_id": sub_user_id,
                        "publish_time": p_time,
                        "title": "",
                        "url": share_link,
                        "content": shard_text,
                        "other_content": [],
                        "hash_tag": [],
                        "image_info": share_content_img_info_lst,
                        "video_info": share_content_video_info_lst
                    }
                ]
            else:
                share_content = []

            update_info = {"update": True, "update_time": self.times}
            item["type"] = "post_content"
            item["channel"] = "facebook"
            item["url"] = content_url
            item["publish_time"] = p_time
            item["platform_id"] = response.meta['user_id']
            item["crawl_time"] = self.times
            item["store_time"] = self.times
            item["crawl_time_log"] = [
                self.times
            ]
            item["store_time_log"] = [
                self.times
            ]
            brand_mention_str_lst = re.findall(r'@(\w+)', content_text)

            if brand_mention_str_lst:
                brand_mention = [
                    {
                        "type": "string", "value": brand_mention_str_lst
                    },
                    {
                        "type": "link", "value": []
                    },
                    {
                        "type": "img", "value": []
                    }
                ]
            else:
                brand_mention = []

            item["data"] = {
                "content_id": content_id,
                "homepage_url": "",
                "hash_tag": re.findall(r'#(\w+)', content_text),
                "lang": "",
                "title": self.update_title(content_text),
                "sub_title": "",
                "content": content_text + share_link,
                "other_content": [],
                "brand_mention": brand_mention,
                "content_level": "0",
                "share_content": share_content,
                "image_info": content_img_info_lst + share_content_img_info_lst,
                "video_info": content_video_info_lst + share_content_video_info_lst,
                "sn_interact_num": [
                                       {
                                           "typecode": "comment",
                                           "detail": [
                                               {
                                                   "num": comment_count,
                                                   "updatetime": self.times
                                               }
                                           ]
                                       },
                                       {
                                           "typecode": "share",
                                           "detail": [
                                               {
                                                   "num": share_count,
                                                   "updatetime": self.times
                                               }
                                           ]
                                       }
                                   ] + r_lst

            }

            item["col"] = "potential_kol_list"

            yield item

    def parse_photo_url(self, response):
        item = FacebookcontentphotoItem()
        try:
            text_d = re.findall(r'{"__bbox":{"complete":false,"result":{"data":{"currMedia":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            text_d = []

        for i in text_d:
            try:
                # j_text = re.findall(r'__bbox":(.*?)\}\}\]\],\[', i)[0] + '}'
                j_text = '{"complete":true,"result":{"data":{"node":' + i + '}'
            except Exception as e:
                j_text = ''

            first_dict_data = json.loads(j_text)

            node = first_dict_data["result"]["data"]["node"]

            content_id = node["id"]

            content_img_info_lst = list()

            content_img_id = node["id"]

            content_img_url = node["image"]["uri"]

            image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                content_id) + '/%s' % str(content_img_id) + '.jpg'

            content_img_info_lst.append(
                {
                    "id": content_img_id,
                    "url": content_img_url,
                    "path": image_path
                }
            )

            # 情绪反应人处理
            r_lst = list()
            reactions_count = dict()

            try:
                reactions_lst = node["feedback"]["top_reactions"]["edges"]
            except Exception as e:
                reactions_lst = []

            for reactions in reactions_lst:
                # print(reactions["node"].get('reaction_type'), type(reactions["node"].get('reaction_type')))
                if reactions["node"].get('id'):
                    if reactions["node"].get("id") == '1635855486666999':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '1678524932434102':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '444813342392137':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '613557422527858':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '115940658764963':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '478547315650144':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '908563459236466':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                elif reactions["node"].get('reaction_type'):
                    if reactions["node"].get('reaction_type') == 'LIKE':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'LOVE':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'ANGRY':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'CARE':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'HAHA':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'WOW':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'SAD':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                elif reactions["node"].get('localized_name'):
                    if reactions["node"].get('localized_name') == '赞':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '大爱':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '怒':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '抱抱':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '笑趴':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '哇':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '心碎':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )

            item["data"] = {
                "content_id": content_id,
                "image_info": content_img_info_lst,
                "sn_interact_num": r_lst
            }
            item["col"] = "potential_kol_list"

            yield item

    def parse_page_content_1(self, response):
        item = FacebookcontentimageItem()
        # with open('page.txt', 'w', encoding='utf-8') as f:
        #     f.write(response.text)

        try:
            text_d = re.findall(r'"__bbox":{"complete":true,"result":{"data":{"node":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            text_d = []

        if not text_d:
            try:
                text_d = re.findall(r'"__bbox":{"complete":true,"result":{"data":{"nodes":(.*?)\}\}\]\]', response.text)
            except Exception as e:
                text_d = []
        try:
            text_d_1 = re.findall(r'"__bbox":{"complete":false,"result":{"data":{"node":(.*?)\}\}\]\]', response.text)
        except Exception as e:
            text_d_1 = []

        text_d += text_d_1

        # print('text_d----------->', text_d)

        for i in text_d:

            try:
                # j_text = re.findall(r'__bbox":(.*?)\}\}\]\],\[', i)[0] + '}'
                j_text = '{"complete":true,"result":{"data":{"node":' + i + '}'
            except Exception as e:
                j_text = ''
            # # print('j_text----------->', j_text)
            # with open('page_lst_1.json', 'w', encoding="utf-8") as f:
            #     f.write(j_text)
            try:
                user_id = re.findall(r'"pageID":"(.*?)",', response.text)[0]
            except Exception as e:
                user_id = ''
            # print('page_id----->', user_id)
            # --------解析第一页的内容 start----------
            first_data = json.loads(j_text)

            # 贴文内容
            try:
                v_text = first_data["result"]["data"]["node"][0]
            except Exception as e:
                v_text = ''

            if not v_text:

                try:
                    v_text = first_data["result"]["data"]["node"]
                except Exception as e:
                    v_text = first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]

            try:
                content_text = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"]["message"][
                        "story"]["message"]["text"]
            except Exception as e:
                content_text = ""

            if not content_text:
                try:
                    content_text = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "content"]["story"]["comet_sections"]["message"][
                        "story"]["message"]["text"]
                except Exception as e:
                    content_text = ""

            # # print('content_text_1---------', content_text)

            # feedback_id 获取
            try:
                feedback_id = v_text["feedback"]["id"]
            except Exception as e:
                feedback_id = ''

            if not feedback_id:
                try:
                    feedback_id = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["feedback"]["id"]
                except Exception as e:
                    feedback_id = ""

            # 贴文ID
            try:
                content_id = first_data["result"]["data"]["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "subscription_target_id"]
            except Exception as e:

                content_id = ""

            if not content_id:
                try:
                    content_id = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "subscription_target_id"]
                except Exception as e:
                    content_id = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "subscription_target_id"]

            # 贴文图片
            content_img_info_lst = list()

            try:
                media_temp = v_text["comet_sections"]["content"]["story"]["attachments"][0]["styles"]
            except Exception as e:
                media_temp = {}
            if not media_temp:
                try:
                    media_temp = v_text["comet_sections"]["content"]["story"]["attachments"][0][
                        "style_type_renderer"]
                except Exception as e:
                    media_temp = {}
            if not media_temp:
                try:
                    media_temp = \
                    v_text["comet_sections"]["content"]["story"]["attached_story"]["attachments"][0]["styles"]
                except Exception as e:
                    media_temp = {}
            try:
                content_img_lst = media_temp["attachment"]["all_subattachments"]["nodes"]
            except Exception as e:
                content_img_lst = []

            if content_img_lst:
                try:
                    for content_img in content_img_lst:
                        content_img_id = content_img["media"]["id"]
                        print()
                        if content_img["media"].get("photo_image"):

                            content_img_url = content_img["media"]["photo_image"]["uri"]

                        elif content_img["media"].get("image"):
                            content_img_url = content_img["media"]["image"]["uri"]

                        image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                            content_id) + '/%s' % str(content_img_id) + '.jpg'
                        content_img_info_lst.append(
                            {
                                "id": content_img_id,
                                "url": content_img_url,
                                "path": image_path
                            }
                        )
                except Exception as e:
                    print('post_img is none !', e)
                    # content_img_info_lst.append({"id": "", "url": "", "path": ""})
            else:
                try:

                    content_img_id = media_temp["attachment"]["media"]["id"]
                    if media_temp["attachment"]["media"].get("photo_image"):

                        content_img_url = media_temp["attachment"]["media"]["photo_image"]["uri"]

                    elif media_temp["attachment"]["media"].get("image"):
                        content_img_url = media_temp["attachment"]["media"]["image"]["uri"]

                    image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                        content_id) + '/%s' % str(content_img_id) + '.jpg'

                    content_img_info_lst.append(
                        {
                            "id": content_img_id,
                            "url": content_img_url,
                            "path": image_path
                        }
                    )
                except Exception as e:
                    print('post_img is none !', e)

            # 转载链接
            try:
                share_link = \
                    v_text["comet_sections"]["content"]["story"]["attachments"][0][
                        "style_type_renderer"]["attachment"]["story_attachment_link_renderer"][
                        "attachment"]["url"]
            except Exception as e:
                share_link = ''

            if not share_link:
                try:
                    share_link = \
                        first_data["result"]["node"]["comet_sections"]["content"]["story"]["attachments"][0][
                            "style_type_renderer"]["attachment"]["url"]
                except Exception as e:
                    share_link = ''

            if not share_link:
                try:
                    share_link = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "content"]["story"]["attachments"][0][
                        "style_type_renderer"]["attachment"]["url"]
                except Exception as e:
                    share_link = ""

            if not share_link:
                try:
                    share_link = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["url"]
                except Exception as e:
                    share_link = ""

            if not share_link:
                share_link = ""

            # 帖子视频
            content_video_info_lst = list()
            try:
                content_video_id = \
                    v_text["comet_sections"]["content"]["story"]["attachments"][0][
                        "style_type_renderer"][
                        "attachment"]["media"]["videoId"]
            except Exception as e:
                # print(e)
                content_video_id = ''

            if not content_video_id:
                try:
                    content_video_id = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "content"]["story"]["attachments"][0][
                        "style_type_renderer"][
                        "attachment"]["media"]["videoId"]
                except Exception as e:
                    content_video_id = ""

            # print('content_video_id---->', content_video_id)

            try:
                content_video_url = \
                    v_text["comet_sections"]["content"]["story"]["attachments"][0][
                        "style_type_renderer"][
                        "attachment"]["media"]["dash_prefetch_resources"]["video"][-1]["url"]
            except Exception as e:
                content_video_url = ''

            if not content_video_url:
                try:

                    content_video_url = \
                        v_text["comet_sections"]["content"]["story"]["attachments"][0][
                            "style_type_renderer"][
                            "attachment"]["media"]["playable_url"]
                except Exception as e:
                    content_video_url = ''

            if content_video_id or content_video_url:
                video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(content_video_id) + '.mp4'
                content_video_info_lst.append(
                    {
                        "id": content_video_id,
                        "url": content_video_url,
                        "path": video_path
                    }
                )

            # 帖子创建时间
            try:
                content_time = \
                    v_text["comet_sections"]["context_layout"]["story"]["comet_sections"]["metadata"][
                        0][
                        "story"]["creation_time"]
            except Exception as e:
                content_time = \
                    v_text["comet_sections"]["context_layout"]["story"]["comet_sections"][
                        "metadata"][
                        1][
                        "story"]["creation_time"]

            # 帖子的url
            try:
                content_url = \
                    v_text["comet_sections"]["context_layout"]["story"]["comet_sections"]["metadata"][
                        0][
                        "story"]["url"]
            except Exception as e:
                content_url = v_text["comet_sections"]["context_layout"]["story"]["comet_sections"]["metadata"][
                    1][
                    "story"]["url"]

            # # print()
            # 帖子的点赞数
            # try:
            #     content_like_num = f_edges["node"]["comet_sections"]["feedback"]["story"]["feedback_context"][
            #         "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
            #         "reaction_count"]["count"]
            # except Exception as e:
            #     content_like_num = f_edges["node"]["comet_sections"]["feedback"]["story"]["feedback_context"][
            #         "feedback_target_with_context"]["ufi_renderer"]["feedback"][
            #         "reaction_count"]["count"]

            # 帖子的评论数
            try:
                comment_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "comments_count_summary_renderer"]["feedback"]["comment_count"]["total_count"]
            except Exception as e:
                comment_count = ""

            if not comment_count:
                try:
                    comment_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"]["comment_count"]["total_count"]
                except Exception as e:
                    comment_count = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"]["comment_count"]["total_count"]
            # 帖子的分享数
            try:
                share_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                    "share_count"]["count"]
            except Exception as e:
                share_count = ""

            if not share_count:
                try:
                    share_count = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "share_count"]["count"]
                except Exception as e:
                    share_count = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "share_count"]["count"]

            # 转发内容待定
            try:
                shard_text = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["message"]["story"]["message"]["text"]
            except Exception as e:
                shard_text = ''

            if not shard_text:
                try:
                    shard_text = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["message"]["story"]["message"]["text"]
                except Exception as e:
                    shard_text = ""
                # print('shard_text----->', e)

            if not shard_text:
                try:
                    shard_text = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["message"]["text"]
                except Exception as e:
                    shard_text = ""
            # 转发的user_id
            try:
                sub_user_id = \
                    v_text["comet_sections"]["content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["title"]["story"]["actors"][0]["id"]
            except Exception as e:
                sub_user_id = ""

            if not sub_user_id:
                try:
                    sub_user_id = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]["comet_sections"][
                        "content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["title"]["story"]["actors"][0]["id"]
                except Exception as e:
                    sub_user_id = ""

            if not sub_user_id:
                try:
                    sub_user_id = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["comet_sections"]["attached_story_title"]["story"]["actors"][0]["id"]
                except Exception as e:
                    sub_user_id = ""

            # 转发内容的图片
            share_content_img_info_lst = list()
            try:
                s_img_text = v_text
            except Exception as e:
                s_img_text = first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]["node"]
            try:
                share_img_lst = \
                    s_img_text["comet_sections"]["content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"]["attached_story_layout"]["story"][
                        "attachments"][0][
                        "style_type_renderer"]["attachment"]["all_subattachments"]["nodes"]
            except Exception as e:
                share_img_lst = []

            if not share_img_lst:
                try:
                    share_content_img = \
                        s_img_text["comet_sections"]["content"]["story"]["comet_sections"][
                            "attached_story"][
                            "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                            "comet_sections"][0][
                            "style_type_renderer"]["attachment"]
                    share_content_img_id = share_content_img["media"]["id"]
                    try:
                        share_content_img_url = share_content_img["media"]["viewer_image"]["uri"]
                    except Exception as e:
                        share_content_img_url = ''
                    if not share_content_img_url:
                        share_content_img_url = share_content_img["media"]["photo_image"]["uri"]
                    share_image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                        content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                    share_content_img_info_lst.append(
                        {
                            "id": share_content_img_id,
                            "url": share_content_img_url,
                            "path": share_image_path
                        }
                    )
                except Exception as e:
                    print('share post_img is none !', e)

            else:
                try:

                    for share_content_img in share_img_lst:
                        share_content_img_id = share_content_img["media"]["id"]
                        try:
                            share_content_img_url = share_content_img["media"]["viewer_image"]["uri"]
                        except Exception as e:
                            share_content_img_url = ''
                        if not share_content_img_url:
                            share_content_img_url = share_content_img["media"]["photo_image"]["uri"]
                        share_content_img_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                            content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                        share_content_img_info_lst.append(
                            {
                                "id": share_content_img_id,
                                "url": share_content_img_url,
                                "path": share_content_img_path
                            }
                        )
                except Exception as e:
                    print('share post_img is none !', e)

            try:
                share_content_img_id = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["preferred_thumbnail"]["id"]
                share_content_img_url = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["preferred_thumbnail"]["image"]["url"]
            except Exception as e:
                share_content_img_id = ""
                share_content_img_url = ""

            print("share_content_img_id------>", share_content_img_id)

            if share_content_img_url:
                share_content_img_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(share_content_img_id) + '.jpg'
                share_content_img_info_lst.append(
                    {
                        "id": share_content_img_id,
                        "url": share_content_img_url,
                        "path": share_content_img_path
                    }
                )

            # 转发的视频
            share_content_video_info_lst = list()
            try:
                share_content_video_id = \
                    s_img_text["comet_sections"]["content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"][0][
                        "style_type_renderer"]["attachment"]["media"]["videoId"]
                share_content_video_url = \
                    s_img_text["comet_sections"]["content"]["story"]["comet_sections"]["attached_story"][
                        "story"]["attached_story"]["comet_sections"]["attached_story_layout"]["story"][
                        "comet_sections"][0][
                        "style_type_renderer"]["attachment"]["media"]["dash_prefetch_resources"]["video"][
                        -1][
                        "url"]

            except Exception as e:
                share_content_video_id = ""
                share_content_video_url = ""
                print("share video is download error !")

            if not share_content_video_url:
                try:
                    share_content_video_id = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["videoId"]
                    share_content_video_url = v_text["comet_sections"]["content"]["story"]["attachments"][0]["throwbackStyles"]["attachment_target_renderer"]["attachment"]["target"]["attachments"][0]["styles"]["attachment"]["media"]["playable_url"]
                except Exception as e:
                    share_content_video_id = ""
                    share_content_video_url = ""

            if share_content_video_url:
                share_video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
                    content_id) + '/%s' % str(share_content_video_id) + '.mp4'
                share_content_video_info_lst.append(
                    {
                        "id": share_content_video_id,
                        "url": share_content_video_url,
                        "path": share_video_path
                    }
                )

            # 情绪反应人处理
            r_lst = list()
            reactions_count = dict()

            try:
                reactions_lst = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                    "feedback_target_with_context"]["ufi_renderer"]["display_comments"]["feedback"][
                    "top_reactions"]["edges"]
            except Exception as e:
                reactions_lst = []

            if not reactions_lst:
                try:
                    reactions_lst = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "top_reactions"]["edges"]
                except Exception as e:
                    reactions_lst = []

            if not reactions_lst:
                try:
                    reactions_lst = v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "comet_ufi_summary_and_actions_renderer"]["feedback"][
                        "top_reactions"]["edges"]
                except Exception as e:
                    reactions_lst = []

            if not reactions_lst:
                try:
                    reactions_lst = \
                        v_text["comet_sections"]["feedback"]["story"]["feedback_context"][
                            "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                            "comet_ufi_summary_and_actions_renderer"]["feedback"]['cannot_see_top_custom_reactions'][
                            "top_reactions"]["edges"]
                except Exception as e:
                    reactions_lst = \
                    first_data["result"]["data"]["page"]["timeline_feed_units"]["edges"][0]['node']["comet_sections"][
                        "feedback"]["story"]["feedback_context"][
                        "feedback_target_with_context"]["ufi_renderer"]["feedback"][
                        "comet_ufi_summary_and_actions_renderer"]["feedback"]['cannot_see_top_custom_reactions'][
                        "top_reactions"]["edges"]

            for reactions in reactions_lst:
                # print(reactions["node"].get('reaction_type'), type(reactions["node"].get('reaction_type')))
                if reactions["node"].get('id'):
                    if reactions["node"].get("id") == '1635855486666999':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '1678524932434102':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '444813342392137':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '613557422527858':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '115940658764963':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '478547315650144':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("id") == '908563459236466':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                elif reactions["node"].get('reaction_type'):
                    if reactions["node"].get('reaction_type') == 'LIKE':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'LOVE':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'ANGRY':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'CARE':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'HAHA':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'WOW':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get("reaction_type") == 'SAD':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                elif reactions["node"].get('localized_name'):
                    if reactions["node"].get('localized_name') == '赞':
                        like_count = reactions["reaction_count"]
                        reactions_count['like_count'] = like_count
                        r_lst.append(
                            {
                                "typecode": "like",
                                "detail": [
                                    {
                                        "num": like_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '大爱':
                        love_count = reactions["reaction_count"]
                        reactions_count['love_count'] = love_count
                        r_lst.append(
                            {
                                "typecode": "love",
                                "detail": [
                                    {
                                        "num": love_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '怒':
                        angry_count = reactions["reaction_count"]
                        reactions_count['angry_count'] = angry_count
                        r_lst.append(
                            {
                                "typecode": "anger",
                                "detail": [
                                    {
                                        "num": angry_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '抱抱':
                        care_count = reactions["reaction_count"]
                        reactions_count['care_count'] = care_count
                        r_lst.append(
                            {
                                "typecode": "care",
                                "detail": [
                                    {
                                        "num": care_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '笑趴':
                        haha_count = reactions["reaction_count"]
                        reactions_count['haha_count'] = haha_count
                        r_lst.append(
                            {
                                "typecode": "happy",
                                "detail": [
                                    {
                                        "num": haha_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '哇':
                        wow_count = reactions["reaction_count"]
                        reactions_count['wow_count'] = wow_count
                        r_lst.append(
                            {
                                "typecode": "wow",
                                "detail": [
                                    {
                                        "num": wow_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
                    elif reactions["node"].get('localized_name') == '心碎':
                        sad_count = reactions["reaction_count"]
                        reactions_count['sad_count'] = sad_count
                        r_lst.append(
                            {
                                "typecode": "sad",
                                "detail": [
                                    {
                                        "num": sad_count,
                                        "updatetime": self.times
                                    }
                                ]
                            }
                        )
            update_info = {"update": True, "update_time": self.times}

            try:
                time1 = datetime.datetime.utcfromtimestamp(int(content_time)).replace(
                    microsecond=0) + datetime.timedelta(hours=8)
                p_time = time1.astimezone(self.beijing).isoformat()
            except Exception as e:
                p_time = ""
            # print(content_url)

            if sub_user_id:
                share_content = [
                    {
                        "type": "",
                        "platform_id": sub_user_id,
                        "publish_time": p_time,
                        "title": "",
                        "url": share_link,
                        "content": shard_text,
                        "other_content": [],
                        "hash_tag": [],
                        "image_info": share_content_img_info_lst,
                        "video_info": share_content_video_info_lst
                    }
                ]
            else:
                share_content = []

            item["type"] = "post_content"
            item["channel"] = "facebook"
            item["url"] = content_url
            item["publish_time"] = p_time
            item["platform_id"] = response.meta['user_id']
            item["crawl_time"] = self.times
            item["store_time"] = self.times
            item["crawl_time_log"] = [
                self.times
            ]
            item["store_time_log"] = [
                self.times
            ]
            # 判断brand_mention
            brand_mention_str_lst = re.findall(r'@(\w+)', content_text)

            if brand_mention_str_lst:
                brand_mention = [
                    {
                        "type": "string", "value": brand_mention_str_lst
                    },
                    {
                        "type": "link", "value": []
                    },
                    {
                        "type": "img", "value": []
                    }
                ]
            else:
                brand_mention = []
            item["data"] = {
                "content_id": content_id,
                "homepage_url": "",
                "hash_tag": re.findall(r'#(\w+)', content_text),
                "lang": "",
                "title": self.update_title(content_text),
                "sub_title": "",
                "content": content_text + share_link,
                "content_level": "0",
                "brand_mention": brand_mention,
                "other_content": [],
                "share_content": share_content,
                "image_info": content_img_info_lst + share_content_img_info_lst,
                "video_info": content_video_info_lst + share_content_video_info_lst,
                "sn_interact_num": [
                                       {
                                           "typecode": "comment",
                                           "detail": [
                                               {
                                                   "num": comment_count,
                                                   "updatetime": self.times
                                               }
                                           ]
                                       },
                                       {
                                           "typecode": "share",
                                           "detail": [
                                               {
                                                   "num": share_count,
                                                   "updatetime": self.times
                                               }
                                           ]
                                       },
                                   ] + r_lst

            }
            item["col"] = "potential_kol_list"

            yield item

    def parse_content_video(self, response):
        """
        解析视频URL
        :param response:
        :return:
        """
        # 解析视频url
        item = FacebookcontentimageItem()
        data = {}
        # with open('video_text.txt', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        try:
            text_d_1 = re.findall(r'"__bbox":{"complete":false,"result":{"data":{"video":(.*?)\}\}\]\]',
                                response.text)
        except Exception as e:
            text_d_1 = []
        # try:
        #     text_d_1 = re.findall(r'{"__bbox":{"complete":false,"result":(.*?)\}\}\]\]', response.text)
        # except Exception as e:
        #     text_d_1 = []
        # print(len(text_d_1))
        # text_d += text_d_1

        for t in text_d_1:
            try:
                # j_text_1 = re.findall(r'__bbox":(.*?)\}\}\]\],\[', i)[0] + '}'
                j_text_1 = '{"complete":true,"result":{"data":{"node":' + t + '}'
                # j_text_1 = '{"complete":true,"result":' + t + '}'
            except Exception as e:
                j_text_1 = ''
            json_t = json.loads(j_text_1)

            if json_t.get("result"):
                if json_t.get("result").get("data"):
                    if json_t.get("result").get("data"):
                        print('---index---', text_d_1.index(t))
                        if json_t.get("result").get("data").get("tahoe_sidepane_renderer"):
                            print('index---', text_d_1.index(t))
                            json_t_1 = {}
                            for i in text_d_1:
                                try:
                                    # j_text = re.findall(r'__bbox":(.*?)\}\}\]\],\[', i)[0] + '}'
                                    j_text_2 = '{"complete":true,"result":{"data":{"node":' + i + '}'
                                except Exception as e:
                                    j_text_2 = ''

                                json_t_1 = json.loads(j_text_2)

                            data = self.parse_video_second(json_t, json_t_1)

        for i in text_d_1:
            try:
                j_text = '{"complete":true,"result":{"data":{"node":' + i + '}'
            except Exception as e:
                j_text = ''

            first_data = json.loads(j_text)

            try:
                v_text = first_data["result"]["data"]["node"][0]
            except Exception as e:
                v_text = ''

            if not v_text:

                try:
                    v_text = first_data["result"]["data"]["node"]
                except Exception as e:
                    v_text = first_data["result"]["data"]["user"]["timeline_feed_units"]["edges"][0]["node"]

            if v_text.get("story"):
                data = ""
                item_three = FacebookcontentphotoItem()
                data_three = self.parse_video_three(first_data)
                item_three["data"] = {
                    "content_id": data_three["content_id"],
                    "image_info": data_three["content_img_info_lst"],
                    "video_info": data_three["content_video_info_lst"]
                }
                item_three["col"] = "potential_kol_list"

                yield item_three

            elif v_text["creation_story"].get("feedback_context"):
                print("success----", text_d_1.index(i))
                data = self.parse_video_first(first_data)

        if data:
            item["type"] = "post_content"
            item["channel"] = "facebook"
            item["url"] = data["content_url"]
            item["publish_time"] = data["p_time"]
            item["platform_id"] = response.meta['user_id']
            item["crawl_time"] = self.times
            item["store_time"] = self.times
            item["crawl_time_log"] = [
                self.times
            ]
            item["store_time_log"] = [
                self.times
            ]
            brand_mention_str_lst = re.findall(r'@(\w+)', data["content_text"])

            if brand_mention_str_lst:
                brand_mention = [
                    {
                        "type": "string", "value": brand_mention_str_lst
                    },
                    {
                        "type": "link", "value": []
                    },
                    {
                        "type": "img", "value": []
                    }
                ]
            else:
                brand_mention = []

            item["data"] = {
                "content_id": data["content_id"],
                "homepage_url": "",
                "hash_tag": re.findall(r'#(\w+)', data["content_text"]),
                "lang": "",
                "title": self.update_title(data["content_text"]),
                "sub_title": "",
                "content": data["content_text"],
                "other_content": [],
                "brand_mention": brand_mention,
                "content_level": "0",
                "share_content": [],
                "image_info": data["content_img_info_lst"],
                "video_info": data["content_video_info_lst"],
                "sn_interact_num": [
                                       {
                                           "typecode": "comment",
                                           "detail": [
                                               {
                                                   "num": data["comment_count"],
                                                   "updatetime": self.times
                                               }
                                           ]
                                       }
                                   ] + data["r_lst"]

            }

            item["col"] = "potential_kol_list"

            yield item

    def parse_video_first(self, json_data):
        """
        解析视频第一种情况
        :param json_data:
        :return: dict_data
        """
        dict_data = dict()
        try:
            v_text = json_data["result"]["data"]["node"][0]
        except Exception as e:
            v_text = ''

        if not v_text:

            try:
                v_text = json_data["result"]["data"]["node"]
            except Exception as e:
                v_text = json_data["result"]["data"]["user"]["timeline_feed_units"]["edges"][0]["node"]
        try:
            feedback_context = v_text["creation_story"]["feedback_context"]
        except Exception as e:
            feedback_context = v_text
        try:
            attachments = v_text["creation_story"]["attachments"]
        except Exception as e:
            attachments = v_text
        # 贴文内容
        try:
            content_text = attachments[0]["media"]["savable_description"]["text"]
        except Exception as e:
            content_text = ""

        dict_data["content_text"] = content_text

        # feedback_id 获取

        # 贴文ID
        try:
            content_id = attachments[0]["media"]["id"]
        except Exception as e:
            content_id = ""

        dict_data["content_id"] = content_id

        # 贴文图片
        content_img_info_lst = list()
        content_img_id = attachments[0]["media"]["preferred_thumbnail"]["id"]
        content_img_url = attachments[0]["media"]["preferred_thumbnail"]["image"]["uri"]
        image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
            content_id) + '/%s' % str(content_img_id) + '.jpg'
        content_img_info_lst.append(
            {
                "id": content_img_id,
                "url": content_img_url,
                "path": image_path
            }
        )

        dict_data["content_img_info_lst"] = content_img_info_lst

        # 转载链接
        # 帖子视频
        content_video_info_lst = list()
        content_video_id = attachments[0]["media"]["id"]
        content_video_url = attachments[0]["media"]["playable_url"]
        video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/video' + '/%s' % str(
            content_id) + '/%s' % str(content_video_id) + '.mp4'
        content_video_info_lst.append(
            {
                "id": content_video_id,
                "url": content_video_url,
                "path": video_path
            }
        )

        dict_data["content_video_info_lst"] = content_video_info_lst

        # 帖子创建时间
        try:
            content_time = attachments[0]["publish_time"]
        except Exception as e:
            content_time = 0

        dict_data["content_time"] = content_time

        # 帖子的url
        content_url = feedback_context["feedback_target_with_context"]["url"]

        dict_data["content_url"] = content_url

        # 帖子的点赞数

        # 帖子的评论数
        comment_count = feedback_context["feedback_target_with_context"]["comment_count"]["total_count"]

        dict_data["comment_count"] = comment_count

        # 帖子的分享数

        # 转发内容待定

        # 转发的user_id

        # 转发内容的图片

        # 转发的视频

        # 情绪反应人处理
        r_lst = list()
        reactions_count = dict()
        try:
            ufi_action_renderers_lst = feedback_context["feedback_target_with_context"]["ufi_action_renderers"]
            for ufi_action_renderers in ufi_action_renderers_lst:
                if ufi_action_renderers.get("feedback"):
                    reactions_lst = ufi_action_renderers["feedback"]["top_reactions"]["edges"]
        except Exception as e:
            reactions_lst = []

        for reactions in reactions_lst:
            if reactions["node"].get('id'):
                if reactions["node"].get("id") == '1635855486666999':
                    like_count = reactions["reaction_count"]
                    reactions_count['like_count'] = like_count
                    r_lst.append(
                        {
                            "typecode": "like",
                            "detail": [
                                {
                                    "num": like_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '1678524932434102':
                    love_count = reactions["reaction_count"]
                    reactions_count['love_count'] = love_count
                    r_lst.append(
                        {
                            "typecode": "love",
                            "detail": [
                                {
                                    "num": love_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '444813342392137':
                    angry_count = reactions["reaction_count"]
                    reactions_count['angry_count'] = angry_count
                    r_lst.append(
                        {
                            "typecode": "anger",
                            "detail": [
                                {
                                    "num": angry_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '613557422527858':
                    care_count = reactions["reaction_count"]
                    reactions_count['care_count'] = care_count
                    r_lst.append(
                        {
                            "typecode": "care",
                            "detail": [
                                {
                                    "num": care_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '115940658764963':
                    haha_count = reactions["reaction_count"]
                    reactions_count['haha_count'] = haha_count
                    r_lst.append(
                        {
                            "typecode": "happy",
                            "detail": [
                                {
                                    "num": haha_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '478547315650144':
                    wow_count = reactions["reaction_count"]
                    reactions_count['wow_count'] = wow_count
                    r_lst.append(
                        {
                            "typecode": "wow",
                            "detail": [
                                {
                                    "num": wow_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '908563459236466':
                    sad_count = reactions["reaction_count"]
                    reactions_count['sad_count'] = sad_count
                    r_lst.append(
                        {
                            "typecode": "sad",
                            "detail": [
                                {
                                    "num": sad_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )

            elif reactions["node"].get('reaction_type'):
                if reactions["node"].get('reaction_type') == 'LIKE':
                    like_count = reactions["reaction_count"]
                    reactions_count['like_count'] = like_count
                    r_lst.append(
                        {
                            "typecode": "like",
                            "detail": [
                                {
                                    "num": like_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'LOVE':
                    love_count = reactions["reaction_count"]
                    reactions_count['love_count'] = love_count
                    r_lst.append(
                        {
                            "typecode": "love",
                            "detail": [
                                {
                                    "num": love_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'ANGRY':
                    angry_count = reactions["reaction_count"]
                    reactions_count['angry_count'] = angry_count
                    r_lst.append(
                        {
                            "typecode": "anger",
                            "detail": [
                                {
                                    "num": angry_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'CARE':
                    care_count = reactions["reaction_count"]
                    reactions_count['care_count'] = care_count
                    r_lst.append(
                        {
                            "typecode": "care",
                            "detail": [
                                {
                                    "num": care_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'HAHA':
                    haha_count = reactions["reaction_count"]
                    reactions_count['haha_count'] = haha_count
                    r_lst.append(
                        {
                            "typecode": "happy",
                            "detail": [
                                {
                                    "num": haha_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'WOW':
                    wow_count = reactions["reaction_count"]
                    reactions_count['wow_count'] = wow_count
                    r_lst.append(
                        {
                            "typecode": "wow",
                            "detail": [
                                {
                                    "num": wow_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'SAD':
                    sad_count = reactions["reaction_count"]
                    reactions_count['sad_count'] = sad_count

                    r_lst.append(
                        {
                            "typecode": "sad",
                            "detail": [
                                {
                                    "num": sad_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )

        dict_data["r_lst"] = r_lst

        try:
            time1 = datetime.datetime.utcfromtimestamp(int(content_time)).replace(
                microsecond=0) + datetime.timedelta(hours=8)

            p_time = time1.astimezone(self.beijing).isoformat()
        except Exception as e:
            p_time = ""

        dict_data["p_time"] = p_time

        return dict_data

    def parse_video_second(self, json_data, json_data_1=None):
        """
        解析视频的第二种格式
        :param json_data_1: 解析视频第一种参数
        :param json_data:解析视频第二种参数
        :return: dict_data
        """
        dict_data = dict()
        r_lst = list()
        try:
            v_text = json_data["result"]["data"]["tahoe_sidepane_renderer"]
        except Exception as e:
            v_text = ''

        try:
            v_text_1 = json_data_1["result"]["data"]["node"]
        except Exception as e:
            v_text_1 = ''

        # 贴文内容
        try:
            content_text = v_text["video"]["creation_story"]["comet_sections"]["message"]["story"]["message"]["text"]
        except Exception as e:
            content_text = ""

        dict_data["content_text"] = content_text

        # feedback_id 获取

        # 贴文ID
        try:
            content_id = v_text["video"]["id"]
        except Exception as e:
            content_id = ""

        dict_data["content_id"] = content_id

        # 贴文图片
        content_img_info_lst = list()

        content_img_id = v_text_1["preferred_thumbnail"]["id"]
        content_img_url = v_text_1["preferred_thumbnail"]["image"]["uri"]
        image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
            content_id) + '/%s' % str(content_img_id) + '.jpg'
        content_img_info_lst.append(
            {
                "id": content_img_id,
                "url": content_img_url,
                "path": image_path
            }
        )

        dict_data["content_img_info_lst"] = content_img_info_lst

        # 转载链接
        # 帖子视频
        content_video_info_lst = list()
        content_video_id = v_text_1["id"]
        content_video_url = v_text_1["playable_url"]
        video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/video' + '/%s' % str(
            content_id) + '/%s' % str(content_video_id) + '.mp4'
        content_video_info_lst.append(
            {
                "id": content_video_id,
                "url": content_video_url,
                "path": video_path
            }
        )

        dict_data["content_video_info_lst"] = content_video_info_lst

        # 帖子创建时间
        try:
            content_time = v_text_1["publish_time"]
        except Exception as e:
            content_time = 0

        dict_data["content_time"] = content_time

        # 帖子的url
        content_url = v_text_1["url"]

        dict_data["content_url"] = content_url

        # 帖子的点赞数

        # 帖子的评论数
        comment_count = v_text["video"]["feedback"]["comment_count"]["total_count"]

        dict_data["comment_count"] = comment_count

        # 帖子的分享数
        share_count = v_text["video"]["feedback"]["share_count"]["count"]

        r_lst.append(
            {
                "typecode": "share",
                "detail": [
                    {
                        "num": share_count,
                        "updatetime": self.times
                    }
                ]
            }
        )

        # 帖子观看数
        view_count = v_text["video"]["feedback"]["video_view_count"]

        r_lst.append(
            {
                "typecode": "video_view",
                "detail": [
                    {
                        "num": view_count,
                        "updatetime": self.times
                    }
                ]
            }
        )

        # 转发内容待定

        # 转发的user_id

        # 转发内容的图片

        # 转发的视频

        # 情绪反应人处理
        reactions_count = dict()
        try:

            reactions_lst = v_text["video"]["feedback"]["top_reactions"]["edges"]
        except Exception as e:
            reactions_lst = []
        if not reactions_lst:
            try:
                reactions_lst = v_text["video"]["feedback"]["cannot_see_top_custom_reactions"]["top_reactions"]["edges"]
            except Exception as e:
                reactions_lst = []

        for reactions in reactions_lst:
            if reactions["node"].get('id'):
                if reactions["node"].get("id") == '1635855486666999':
                    like_count = reactions["reaction_count"]
                    reactions_count['like_count'] = like_count
                    r_lst.append(
                        {
                            "typecode": "like",
                            "detail": [
                                {
                                    "num": like_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '1678524932434102':
                    love_count = reactions["reaction_count"]
                    reactions_count['love_count'] = love_count
                    r_lst.append(
                        {
                            "typecode": "love",
                            "detail": [
                                {
                                    "num": love_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '444813342392137':
                    angry_count = reactions["reaction_count"]
                    reactions_count['angry_count'] = angry_count
                    r_lst.append(
                        {
                            "typecode": "anger",
                            "detail": [
                                {
                                    "num": angry_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '613557422527858':
                    care_count = reactions["reaction_count"]
                    reactions_count['care_count'] = care_count
                    r_lst.append(
                        {
                            "typecode": "care",
                            "detail": [
                                {
                                    "num": care_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '115940658764963':
                    haha_count = reactions["reaction_count"]
                    reactions_count['haha_count'] = haha_count
                    r_lst.append(
                        {
                            "typecode": "happy",
                            "detail": [
                                {
                                    "num": haha_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '478547315650144':
                    wow_count = reactions["reaction_count"]
                    reactions_count['wow_count'] = wow_count
                    r_lst.append(
                        {
                            "typecode": "wow",
                            "detail": [
                                {
                                    "num": wow_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("id") == '908563459236466':
                    sad_count = reactions["reaction_count"]
                    reactions_count['sad_count'] = sad_count
                    r_lst.append(
                        {
                            "typecode": "sad",
                            "detail": [
                                {
                                    "num": sad_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )

            elif reactions["node"].get('reaction_type'):
                if reactions["node"].get('reaction_type') == 'LIKE':
                    like_count = reactions["reaction_count"]
                    reactions_count['like_count'] = like_count
                    r_lst.append(
                        {
                            "typecode": "like",
                            "detail": [
                                {
                                    "num": like_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'LOVE':
                    love_count = reactions["reaction_count"]
                    reactions_count['love_count'] = love_count
                    r_lst.append(
                        {
                            "typecode": "love",
                            "detail": [
                                {
                                    "num": love_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'ANGRY':
                    angry_count = reactions["reaction_count"]
                    reactions_count['angry_count'] = angry_count
                    r_lst.append(
                        {
                            "typecode": "anger",
                            "detail": [
                                {
                                    "num": angry_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'CARE':
                    care_count = reactions["reaction_count"]
                    reactions_count['care_count'] = care_count
                    r_lst.append(
                        {
                            "typecode": "care",
                            "detail": [
                                {
                                    "num": care_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'HAHA':
                    haha_count = reactions["reaction_count"]
                    reactions_count['haha_count'] = haha_count
                    r_lst.append(
                        {
                            "typecode": "happy",
                            "detail": [
                                {
                                    "num": haha_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'WOW':
                    wow_count = reactions["reaction_count"]
                    reactions_count['wow_count'] = wow_count
                    r_lst.append(
                        {
                            "typecode": "wow",
                            "detail": [
                                {
                                    "num": wow_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )
                elif reactions["node"].get("reaction_type") == 'SAD':
                    sad_count = reactions["reaction_count"]
                    reactions_count['sad_count'] = sad_count

                    r_lst.append(
                        {
                            "typecode": "sad",
                            "detail": [
                                {
                                    "num": sad_count,
                                    "updatetime": self.times
                                }
                            ]
                        }
                    )

        dict_data["r_lst"] = r_lst

        try:
            time1 = datetime.datetime.utcfromtimestamp(int(content_time)).replace(
                microsecond=0) + datetime.timedelta(hours=8)

            p_time = time1.astimezone(self.beijing).isoformat()
        except Exception as e:
            p_time = ""

        dict_data["p_time"] = p_time

        return dict_data

    def parse_video_three(self, json_data):
        """
        解析视频第三种情况
        :param json_data:
        :return: dict_data
        """
        dict_data = dict()
        try:
            v_text = json_data["result"]["data"]["node"][0]
        except Exception as e:
            v_text = ''

        if not v_text:

            try:
                v_text = json_data["result"]["data"]["node"]
            except Exception as e:
                v_text = json_data["result"]["data"]["user"]["timeline_feed_units"]["edges"][0]["node"]

        try:
            attachments = v_text["story"]["attachments"]
        except Exception as e:
            attachments = v_text

        # 贴文ID
        try:
            content_id = attachments[0]["media"]["id"]
        except Exception as e:
            content_id = ""

        dict_data["content_id"] = content_id

        # 贴文图片
        content_img_info_lst = list()
        content_img_id = attachments[0]["media"]["preferred_thumbnail"]["id"]
        content_img_url = attachments[0]["media"]["preferred_thumbnail"]["image"]["uri"]
        image_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/img' + '/%s' % str(
            content_id) + '/%s' % str(content_img_id) + '.jpg'
        content_img_info_lst.append(
            {
                "id": content_img_id,
                "url": content_img_url,
                "path": image_path
            }
        )

        dict_data["content_img_info_lst"] = content_img_info_lst

        # 帖子视频
        content_video_info_lst = list()
        content_video_id = attachments[0]["media"]["id"]
        content_video_url = attachments[0]["media"]["playable_url"]
        video_path = IMAGES_STORE + "potential_buffer" + '/facebook/' + 'content/video' + '/%s' % str(
            content_id) + '/%s' % str(content_video_id) + '.mp4'
        content_video_info_lst.append(
            {
                "id": content_video_id,
                "url": content_video_url,
                "path": video_path
            }
        )

        dict_data["content_video_info_lst"] = content_video_info_lst

        return dict_data

    def download_response(self, request, spider):
        response = super(FacebookcontentimageSpider, self).download_response(request, spider)
        if isinstance(response, HtmlResponse) and response.headers.get("Content-Type", "").startswith("video"):
            file_size = int(response.headers.get("Content-Length"))
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=response.url)
            for data in response.iter_content():
                pbar.update(len(data))
            pbar.close()
        return response

    @staticmethod
    def update_title(content):
        """
        从post_content中截取长度为50的字符串到tilte
        :param content: 贴文内容
        :return: title 截取的字符串
        """
        content_str = content.lstrip()
        if content_str:
            content_bytes = content_str.encode('utf-8')
            content_tmp = content_bytes[:100]  # 此处截取bytes长度50
            title = content_tmp.decode('utf-8', errors='ignore')  # 按bytes截取时有小部分无效的字节，传入errors='ignore'忽略错误
            # print('cut_res 长度 字节数', len(cut_res), len(cut_res.encode()))

            return title

        return ""

    # 使用REDIS中的cookie
    @staticmethod
    def get_redis_data(db):
        # 连接REDIS
        r_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=db, password='neal188')
        # 获取所有属性名
        data_lst = r_conn.hkeys('facebook')

        while True:
            if not data_lst:
                time.sleep(30)
                data_lst = r_conn.hkeys('facebook')
            else:
                break

        # 随机获取一个cookie_data
        r_field = random.choice(data_lst).decode()
        cookie_data = r_conn.hget('facebook', r_field)

        return r_field, json.loads(cookie_data)

    # 更新REDIS中的COOKIE
    @staticmethod
    def update_redis_data(r_field, cookie_data, db):
        # 连接REDIS
        r_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=db, password='neal188')

        r_conn.hset("facebook_error", r_field, str(cookie_data))

    @staticmethod
    def del_redis_data(r_field, db):
        r_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=db, password='neal188')

        r_conn.hdel('facebook', r_field)

