import math
import os
from enum import Enum
from peewee import *
from datetime import datetime
from loguru import logger
from utils.format_addon import string_similar, string_sequence, process_string
import re

source_db = os.path.join(os.path.abspath("."), "downloaded.db")
# print (source_db)
# memory_db = ":memory:"
# copyfile(source_db, memory_db)
# db = SqliteDatabase(memory_db)

db = SqliteDatabase(source_db)
db.execute_sql('PRAGMA journal_mode=WAL;')

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db

class MsgStatusDB(Enum):
    """Download status"""

    Downloaded = 1
    Downloading = 2
    SkipDownload = 3
    MissingDownload = 4

def get_similar_rate(msg_dict1, db_results, sizerange_min):

    if '.' in msg_dict1.get('filename'):
        filename1 = msg_dict1.get('filename').split(".")[-2]
    else:
        filename1 = msg_dict1.get('filename')
    filename1 = re.sub(r"^\[\d+\]?", '', filename1).replace(' ', '')

    if '.' in db_results.filename:
        filename2 = db_results.filename.split(".")[-2]
    else:
        filename2 = db_results.filename
    filename2 = re.sub(r"^\[\d+\]?", '', filename2).replace(' ', '')

    title1 = msg_dict1.get('title').replace(' ', '')
    title2 = db_results.title.replace(' ', '')

    similar = 0

    break_all = False
    if filename1 == title1:
        nameas = [filename1]
    else:
        nameas = [filename1, title1]
    if filename2 == title2:
        namebs = [filename2]
    else:
        namebs = [filename2, title2]
    for namea in nameas:
        if break_all:
            break
        for nameb in namebs:
            if string_sequence(namea, nameb):  # 是一个文件序列
                break_all = True
                similar = 0
                break

            sim_num = string_similar(namea, nameb)
            if sim_num == 1:
                similar = 1
                break_all = True
                break

            # 补充情况1 文件名是包含关系 且大小时长都很相似 文件名相似度可以打8折即 扩张为125%
            namea1 = process_string(namea).replace(' ', '')
            nameb1 = process_string(nameb).replace(' ', '')
            if namea1 != '' and nameb1 != '' and (namea1 in nameb1 or nameb1 in namea1) and msg_dict1.get(
                    'mime_type') == db_results.mime_type and msg_dict1.get('media_size') > 0 and math.isclose(
                msg_dict1.get('media_size'),
                db_results.media_size,
                rel_tol=sizerange_min) and msg_dict1.get('media_duration') > 0 and math.isclose(
                msg_dict1.get('media_duration'), db_results.media_duration,
                rel_tol=sizerange_min):  # 文件名是包含关系 且大小时长都很相似
                sim_num = sim_num * 1.25

            # 补充情况2 文件大小完全一致 文件类型完全一致 文件名相似度可以打8折即 扩张为125%
            elif msg_dict1.get('mime_type') == db_results.mime_type and msg_dict1.get(
                    'media_size') > 0 and msg_dict1.get('media_size') == db_results.media_size:
                sim_num = sim_num * 1.25

            if similar < sim_num:
                similar = sim_num

    return similar



class Downloaded(BaseModel):
    id = AutoField(primary_key=True, column_name='ID', null=True)
    chat_id = IntegerField(column_name='CHAT_ID', null=True)
    message_id = IntegerField(column_name='MESSAGE_ID', null=True)
    filename = CharField(max_length=200, column_name='FILENAME', null=True)
    caption = CharField(max_length=200, column_name='CAPTION', null=True)
    title = CharField(max_length=200, column_name='TITLE', null=True)
    mime_type = CharField(max_length=200, column_name='MIME_TYPE', null=True)
    media_size = IntegerField(column_name='MEDIA_SIZE', null=True)
    media_duration = IntegerField(column_name='MEDIA_DURATION', null=True)
    media_addtime = CharField(max_length=200, column_name='MEDIA_ADDTIME', null=True)
    chat_username = CharField(max_length=200, column_name='CHAT_USERNAME')
    chat_title = CharField(max_length=200, column_name='CHAT_TITLE', null=True)
    addtime = CharField(max_length=200, column_name='ADDTIME', null=True)
    msg_type = CharField(max_length=200, column_name='TYPE', null=True) #
    msg_link = CharField(max_length=200, column_name='LINK', null=True)  #
    status = IntegerField(column_name='STATUS') #

    class Meta:
        table_name = 'Downloaded'

    def getMsg(self, chat_id: str, message_id: int, status = 1):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(Downloaded.chat_id == chat_id,
                                        Downloaded.message_id == message_id, Downloaded.status == status)
            if downloaded:
                return downloaded  # 说明存在此条数据
        except DoesNotExist:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username == chat_id, Downloaded.message_id == message_id,
                                            Downloaded.status == status)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except DoesNotExist:
                return None
            return None
        return None # 0为不存在

    def get2Down(self, chat_username: str):
        if db.autoconnect == False:
            db.connect()
        if chat_username:
            try:
                downloaded = Downloaded.select(Downloaded.message_id).where(Downloaded.chat_username == chat_username, Downloaded.status ==2)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except DoesNotExist:
                return None
        return None # 0为不存在

    def getStatusById(self, id: int):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(id=id)
            if downloaded:
                return downloaded.status# 说明存在此条数据
        except DoesNotExist:
            return 0
        return 0 #0为不存在 1未已完成 2为下载中 3为跳过未系在 4为下载后丢失

    def getStatus(self, chat_id: int, message_id: int, chat_username = None ):
        if db.autoconnect == False:
            db.connect()
        if chat_id:
            try:
                downloaded = Downloaded.get(chat_id=chat_id, message_id=message_id)
                if downloaded:
                    return downloaded.status# 说明存在此条数据
            except DoesNotExist:
                if chat_username:
                    try:
                        downloaded = Downloaded.get(chat_username=chat_username, message_id=message_id)
                        if downloaded:
                            return downloaded.status  # 说明存在此条数据
                    except DoesNotExist:
                        return 0
        return 0 #0为不存在 1未已完成 2为下载中 3 暂时未使用 4为等效已下载

    def insert_into_db(self, media_dict: dict):
        try:
            db_status = self.getStatus(chat_id=media_dict.get('chat_id'), message_id=media_dict.get('message_id'))
            if db_status == 0:  # 不存在记录则插入
                self.msg_insert_to_db(media_dict)
            else:  # 存在记录则更新
                self.msg_update_to_db(media_dict)

        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
    def msg_insert_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                return False# 说明存在此条数据，无法插入
        except DoesNotExist:
            pass
        try:
            # 出错说明不存在此条数据，需写入
            downloaded = Downloaded()
            downloaded.chat_id = dictit['chat_id']
            downloaded.message_id = dictit['message_id']
            downloaded.filename = dictit['filename']
            downloaded.caption = dictit['caption']
            downloaded.title = dictit['title']
            downloaded.mime_type = dictit['mime_type']
            downloaded.media_size = dictit['media_size']
            downloaded.media_duration = dictit['media_duration']
            downloaded.media_addtime = dictit['media_addtime']
            if not dictit['chat_username']:
                dictit['chat_username'] = ''
            downloaded.chat_username = dictit['chat_username']
            downloaded.chat_title = dictit['chat_title']
            downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
            downloaded.msg_type = dictit['msg_type']
            downloaded.msg_link = dictit['msg_link']
            downloaded.status = dictit['status']
            downloaded.save()
            # db.close()
            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return False

    def msg_update_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                downloaded.chat_id = dictit['chat_id']
                downloaded.message_id = dictit['message_id']
                downloaded.filename = dictit['filename']
                downloaded.caption = dictit['caption']
                downloaded.title = dictit['title']
                downloaded.mime_type = dictit['mime_type']
                downloaded.media_size = dictit['media_size']
                downloaded.media_duration = dictit['media_duration']
                downloaded.media_addtime = dictit['media_addtime']
                if not dictit['chat_username']:
                    dictit['chat_username'] = ''
                downloaded.chat_username = dictit['chat_username']
                downloaded.chat_title = dictit['chat_title']
                downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
                downloaded.status = dictit['status']
                downloaded.msg_type = dictit['msg_type']
                downloaded.msg_link = dictit['msg_link']
                downloaded.save()
                # db.close()
                return True
        except DoesNotExist:
            return False
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return False

    def get_all_message_id(self):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select(Downloaded.id).where(Downloaded.status == 1)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_message(self):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.status == 1).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_finished_message_from(self, start_id):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.id >= start_id, Downloaded.status == 1).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_message_from(self, start_id):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.id >= start_id).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False




    def get_similar_files(self, msgdict, similar_min: float, sizerange_min: float, status: list = None): #返回结果均不包含自己
        similar_file_list = []
        if db.autoconnect == False:
            db.connect()
        try:
            if status is None or len(status) == 0:
                status_acc= [1] #只找完成下载的
            else:
                status_acc = status

            # if msgdict.get('msg_link') == 'https://t.me/TG672/2282':
            #     print ('debug')

            # 判断依据Step1.2： 找出类型一致 大小完全一致的文件记录
            result1 = Downloaded.select().where(Downloaded.mime_type == msgdict.get('mime_type'),
                                                   Downloaded.media_size== msgdict.get('media_size'),
                                                   Downloaded.status.in_(status_acc))

            # 判断依据Step1.3： 找出类型一致 文件名非常像 大小差别在10倍允许值范围的文件记录
            media_size_1 = math.floor(msgdict.get('media_size') * (1 - sizerange_min * 10))
            media_size_2 = math.floor(msgdict.get('media_size') * (1 + sizerange_min * 10))

            if not msgdict.get('title') or msgdict.get('title') =='':
                # 当文件标题不存在或为空时，无法进行相似度比较 返回空列表
                return []

            file_core_name = re.sub(r"[-_~～]", ' ', msgdict.get('title', ''))

            if file_core_name and len(file_core_name) >= 4:
                result2 = Downloaded.select().where(Downloaded.mime_type == msgdict.get('mime_type'),
                                                    (Downloaded.filename % f'*{file_core_name.replace(" ", "*")}*' | Downloaded.title % f'*{file_core_name.replace(" ", "*")}*'),
                                                    Downloaded.media_size.between(
                                                        media_size_1, media_size_2),
                                                    Downloaded.status.in_(status_acc))

                # 判断依据Step1.4： 找出文档类型 文件名完全一致的文件记录
                result3 = Downloaded.select().where(Downloaded.msg_type == msgdict.get('msg_type'),
                                                    (Downloaded.filename % f'*{file_core_name.replace(" ", "*")}*' | Downloaded.title % f'*{file_core_name.replace(" ", "*")}*'),
                                                    Downloaded.status.in_(status_acc))



                downloaded = result1.union(result2).union(result3)
            else:
                downloaded = result1
            if len(downloaded) > 20:
                return []
            for record in downloaded:
                if record.chat_id == msgdict.get('chat_id') and record.message_id == msgdict.get('message_id'): # 是自己
                    # if record.status == 1:  # 如果是已完成状态 直接加入列表
                    #     similar_file_list.append(record)
                    # else:
                    #     continue # 如果是未完成状态 则跳过
                    continue
                else: # 不是自己 判断文件名是否接近
                    similar = get_similar_rate(msgdict, record, sizerange_min)
                    if similar >= similar_min: # 名字高于相似度阈值
                        similar_file_list.append(record)
                        continue
            # db.close()
            if similar_file_list and len(similar_file_list) >= 1:
                return similar_file_list
            else:
                return []
        except DoesNotExist:
            # db.close()
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return None

    def get_last_read_message_id(self, chat_username: str):
        last_read_message_id = 1
        chat_username_qry = chat_username
        if db.autoconnect == False:
            db.connect()
        try:
            select_str = Downloaded.select(fn.Max(Downloaded.message_id)).where(
                Downloaded.chat_username == chat_username, Downloaded.status == 1)
            last_read_message_id = select_str.scalar()
            #print(f"==========={last_read_message_id}=============")
            # db.close()
            if last_read_message_id:
                return last_read_message_id
            else:
                return 1
        except DoesNotExist:
            logger.error(f"{chat_username}error")
            # db.close()
        except Exception as e:
            logger.error(
                f"[{chat_username}{e}].",
                exc_info=True,
            )

    def load_retry_msg_from_db(self):
        if db.autoconnect == False:
            db.connect()
        try:
            retry_ids = Downloaded.select(Downloaded.chat_id,Downloaded.chat_username,
                                          fn.GROUP_CONCAT(Downloaded.message_id).alias('retry_ids')).where(
                Downloaded.status == 2).group_by(
                Downloaded.chat_id, Downloaded.chat_username)

            dicts = []
            if retry_ids:
                for retry in retry_ids:
                    retryIds_str = str(retry.retry_ids).split(',')
                    retryIds = []
                    for retryId in retryIds_str:
                        retryIds.append(int(retryId))
                    dictit = {
                        'chat_id': retry.chat_id,
                        'chat_username' : retry.chat_username,
                        'ids_to_retry': set(retryIds)
                    }
                    dicts.append(dictit)
            # db.close()
            return dicts
        except DoesNotExist:
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return None

    def retry_msg_insert_to_db(self, retry_chat_username :str, retry_msg_ids: []):
        if not retry_msg_ids or not isinstance(retry_msg_ids, list):
            return False
        if db.autoconnect == False:
            db.connect()
        for msg_id in retry_msg_ids:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username==retry_chat_username, Downloaded.message_id==int(msg_id))
                downloaded.status = 2
                downloaded.save()
                # db.close()
                return True
            except DoesNotExist:
                continue
            except Exception as e:
                logger.error(
                    f"[{e}].",
                    exc_info=True,
                )
                # db.close()
                return False



class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
