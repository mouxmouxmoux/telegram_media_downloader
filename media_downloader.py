import asyncio
import logging
import os
import shutil
import time
from typing import List, Optional, Tuple, Union
import pyrogram
from loguru import logger
from typing import AsyncGenerator, Optional
import re
from rich.logging import RichHandler
from tqdm.asyncio import tqdm

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    upload_telegram_chat
)
from module.web import init_web

from utils.format import (
    validate_title,
)

from utils.format_addon import (
    process_string,
    find_files_in_dir,
    find_missing_files,
    merge_files_cat,
    merge_files_write,
    merge_files_shutil,
    get_folder_files_size,
    _get_msg_db_status,
    Msg_db_Status,
    Msg_file_Status,
    _get_msg_file_status,
    save_chunk_to_file,
)


from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

from module.sqlmodel import Downloaded

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue_maxsize = 1000
queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

RETRY_TIME_OUT = 3

CHUNK_MIN = 10

similar_set = 0.90
sizerange_min = 0.01

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)

db = Downloaded()

def need_skip_message(message, chat_download_config, app):
    try:
        # Case 1 不是媒体类型就跳过
        if not (message.audio or message.video or message.photo or message.document):
            return True

        # Case 2 不符合config文件条件就跳过 受 media_types file_formats 以及filter 控制
        meta_data = MetaData()
        set_meta_data(meta_data, message, '')
        if meta_data.file_extension and meta_data.file_extension != '' and (
                not 'all' in app.file_formats[meta_data.media_type]) and (
                not meta_data.file_extension.replace('.', '').lower() in app.file_formats[meta_data.media_type]):
            return True
        if not app.exec_filter(chat_download_config, meta_data):
            return True

        return False

    except Exception as e:
        logger.exception(f"{e}")

def check_download_finish(media_size: int, download_path: str, ui_file_name: str, chunk_count: int) -> bool:
    # 类型检查
    if not isinstance(media_size, int) or not isinstance(download_path, str) or not isinstance(ui_file_name,
                                                                                               str) or not isinstance(
        chunk_count, int):
        raise TypeError("Invalid argument types")

    # 边界条件检查
    if media_size <= 0 or chunk_count <= 0:
        return False

    try:
        files_count, total_size, files_size = get_folder_files_size(download_path)
    except Exception as e:
        print(f"Error while getting folder size: {e}")
        return False

    if files_count != chunk_count or total_size != media_size:
        return False
    return True

def merge_chunkfile(folder_path: str, output_file: str, chunk_count: int, file_size: int, method: str):
    # 验证路径有效性
    if not os.path.isdir(folder_path):
        raise ValueError(f"Folder path '{folder_path}' does not exist or is not a directory.")

    # 创建输出文件夹
    directory, _ = os.path.split(output_file)
    os.makedirs(directory, exist_ok=True)

    # 获取文件列表
    file_list = os.listdir(folder_path)

    # 检查文件数量是否匹配
    if chunk_count != len(file_list):
        return False

    # 根据方法选择合并方式
    if method == 'cat':
        merge_files_cat(folder_path, output_file)
    elif method == 'write':
        merge_files_write(folder_path, output_file)
    elif method == 'shutil':
        merge_files_shutil(folder_path, output_file)
    else:
        raise ValueError(f"Invalid method '{method}'. Supported methods are 'cat', 'write', and 'shutil'.")

    # 检查文件是否存在且大小正确
    while not _is_exist(output_file) and os.path.getsize(output_file) != file_size:
        time.sleep(RETRY_TIME_OUT)
    return True

def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    message_id: int
        Try to download message 's id

    """
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)




# pylint: disable = R0912
def _get_media_meta(
        message: pyrogram.types.Message
) -> dict:
    media_dict = {}
    msg_time = ''

    try:

        if message.chat.id < 0:
            msg_real_chat_id = 0 - message.chat.id - 1000000000000
        else:
            msg_real_chat_id = message.chat.id

        msg_real_chat_username = message.chat.username
        msg_real_message_id = message.id
        msg_real_chat_title = validate_title(message.chat.title)

        msg_from_chat_id = 0
        msg_from_chat_username = ''
        msg_from_message_id = 0
        msg_from_chat_title = ''
        msg_from = False  # 是否转发的信息

        if hasattr(message, 'forward_origin') and hasattr(message.forward_origin, 'message_id'):
            msg_from_chat_id = 0 - message.forward_origin.chat.id - 1000000000000
            msg_from_chat_username = message.forward_origin.chat.username
            msg_from_message_id = message.forward_origin.message_id
            msg_from_chat_title = validate_title(message.forward_origin.chat.title)
            msg_from = True

            if f"@{msg_real_chat_username}" in app.allowed_user_ids:
                msg_real_chat_id = msg_from_chat_id
                msg_real_chat_username = msg_from_chat_username
                msg_real_message_id = msg_from_message_id
                msg_real_chat_title = msg_from_chat_title

        if message.date:
            msg_time = message.date.strftime("%Y-%m-%d %H:%M")

        msg_caption = process_string(getattr(message, "caption", '')) or ""
        msg_media_group_id = getattr(message, "media_group_id", None)

        if msg_caption:
            app.set_caption_name(msg_real_message_id, msg_media_group_id, msg_caption)
        else:
            msg_caption = app.get_caption_name(msg_real_message_id, msg_media_group_id)
        default_ext = 'unknown'
        if message.audio and message.audio != '':
            msg_type = 'audio'
            msg_filename = message.audio.file_name
            msg_duration = message.audio.duration
            msg_size = message.audio.file_size
            default_ext = 'mp3'
        elif message.video and message.video != '':
            msg_type = 'video'
            msg_filename = message.video.file_name
            msg_duration = message.video.duration
            msg_size = message.video.file_size
            default_ext = 'mp4'
        elif message.photo and message.photo != '':
            msg_type = 'photo'
            msg_filename = f"[{msg_real_message_id}]"
            msg_duration = 0
            msg_size = message.photo.file_size
            default_ext = 'jpg'
        elif message.document and message.document != '':
            msg_type = 'document'
            msg_filename = message.document.file_name
            msg_duration = 0
            msg_size = message.document.file_size
            default_ext = 'txt'
        else:
            logger.info(
                f"无需处理的媒体类型: ",
                exc_info=True,
            )
            return None

        if not msg_filename or msg_filename == '':
            msg_filename = "NoName"
        msg_old_filename = msg_filename
        if '.' in msg_filename:
            msg_file_onlyname = process_string(os.path.splitext(msg_filename)[0])
            msg_file_ext = os.path.splitext(msg_filename)[1].replace('.', '')
        else:
            msg_file_onlyname = process_string(msg_filename)
            msg_file_ext = default_ext
        msg_title = f"{msg_file_onlyname}"
        if msg_caption and msg_caption != '':  #caption 存在
            name_from_caption = ""
            if re.search(r"作品.+?\s(.+?)\s", msg_caption):
                name_from_caption = re.search(r"作品.+?\s(.+?)\s", msg_caption).groups()[0]
                msg_title = f"{msg_title}({name_from_caption})"
            elif msg_filename == "NoName":
                name_from_caption = msg_caption
                msg_title = f"{name_from_caption}"

            if 'telegram' in msg_filename.lower() or '电报搜索' in msg_filename or '更多视频' in msg_filename or 'pandatv' in msg_filename.lower() or re.sub(
                    r'[._\-\s]', '',
                    msg_file_onlyname).isdigit() or name_from_caption:  # 文件名有问题
                if name_from_caption:
                    msg_filename = app.get_file_name(msg_real_message_id, f"{msg_title}.{msg_file_ext}", msg_caption)
                else:
                    msg_title = f"{process_string(msg_caption)}"
                    msg_filename = app.get_file_name(msg_real_message_id, f"{msg_title}.{msg_file_ext}", msg_caption)
            else:
                msg_filename = validate_title(
                    app.get_file_name(msg_real_message_id, f"{msg_title}.{msg_file_ext}", msg_caption))
        else:
            msg_filename = validate_title(
                app.get_file_name(msg_real_message_id, f"{msg_title}.{msg_file_ext}", msg_caption))

        if not msg_real_chat_username or msg_real_chat_username == '':
            subdir = validate_title(f"[{msg_real_chat_id}]{msg_real_chat_id}")
        else:
            subdir = validate_title(f"[{msg_real_chat_id}]{msg_real_chat_username}")

        file_save_path = os.path.join(app.get_file_save_path(msg_type, msg_real_chat_title, message.date),subdir)
        temp_save_path = os.path.join(app.temp_save_path,subdir)

        if "media_datetime" in app.config.get("file_path_prefix"):
            year_str = message.date.strftime("%Y")
            month_str = message.date.strftime("%m")
            file_save_path = os.path.join(file_save_path, year_str, month_str )
            temp_save_path = os.path.join(temp_save_path, year_str, month_str )


        if "message_id" in app.config.get("file_path_prefix"):
            file_save_path = os.path.join(file_save_path, str(msg_real_message_id // 100 * 100).zfill(6))
            temp_save_path = os.path.join(temp_save_path, str(msg_real_message_id // 100 * 100).zfill(6))


        file_save_url = os.path.join(file_save_path, msg_filename)
        temp_save_url = os.path.join(temp_save_path, msg_filename)


        if not msg_filename or 'None' in file_save_url:
            if not msg_real_chat_username:
                logger.error(f"[{msg_real_chat_id}]{msg_real_message_id}: ", exc_info=True, )
            else:
                logger.error(f"[{msg_real_chat_username}]{msg_real_message_id}: ", exc_info=True, )

        if msg_from:  #
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_ext,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'msg_from': msg_from,
                'msg_from_chat_id': msg_from_chat_id,
                'msg_from_chat_username': msg_from_chat_username,
                'msg_from_message_id': msg_from_message_id,
                'msg_from_chat_title': msg_from_chat_title,
                'msg_type': msg_type,
                'msg_link': message.link,
                'old_filename': msg_old_filename
            }
        else:
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_ext,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'msg_type': msg_type,
                'msg_link': message.link,
                'old_filename': msg_old_filename
            }
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('some info is missed')}:\n[{e}].",
            exc_info=True,
        )

    return media_dict


async def add_download_task(
        message: pyrogram.types.Message,
        node: TaskNode,
):
    if message.empty:
        return False

    To_Down = False

    msg_dict = _get_media_meta(message)
    msg_db_status = _get_msg_db_status(msg_dict, db, similar_set, sizerange_min)

    if msg_db_status == Msg_db_Status.DB_Exist:  # 数据库有完成
        node.download_status[message.id] = DownloadStatus.SuccessDownload
        return
    elif msg_db_status == Msg_db_Status.DB_Aka_Exist:  # 数据库有 标记为与其他等价
        # 文件有没有暂时不管
        node.download_status[message.id] = DownloadStatus.SkipDownload
        return
    elif msg_db_status == Msg_db_Status.DB_Downloading:  # 数据库标识为正在下载

        msg_file_status = await _get_msg_file_status(msg_dict)
        if msg_file_status == Msg_file_Status.File_Exist or msg_file_status == Msg_file_Status.File_Aka_Exist:
            # 文件存在
            node.download_status[message.id] = DownloadStatus.SkipDownload
            msg_dict['status'] = 1
            db.insert_into_db(msg_dict)  # 补写入数据库
            return
        else:
            # 文件没了
            To_Down = True  # 重新下载
    elif msg_db_status == Msg_db_Status.DB_Aka_Downloading:  # 数据库有其他等价文件在下载
        node.download_status[message.id] = DownloadStatus.SkipDownload
        return
    elif msg_db_status == Msg_db_Status.DB_No_Exist:  # 数据库没有
        To_Down = True
    elif msg_db_status == Msg_db_Status.DB_Passed:  #标记为人为跳过
        node.download_status[message.id] = DownloadStatus.SkipDownload
        return

    if not To_Down:
        node.download_status[message.id] = DownloadStatus.SkipDownload
        return

    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    msg_dict['status'] = 2  # 写入数据库 记录进入下载队列
    db.insert_into_db(msg_dict)

    if not msg_dict.get('chat_username') or msg_dict.get('chat_username') == '':
        show_chat_username = str(msg_dict.get('chat_id'))
    else:
        show_chat_username = msg_dict.get('chat_username')
    logger.info(f"加入队列[{show_chat_username}]{msg_dict.get('filename')}   当前队列长：{queue.qsize()}")
    node.total_task += 1

    return True


async def save_msg_to_file(
    app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Write message text into file"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"

    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )

    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")

    return DownloadStatus.SuccessDownload, file_name


async def download_task(client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode):
    """Download and Forward media"""

    download_status, file_name = await download_media(client=client, message=message, media_types=app.media_types,
                                                      file_formats=app.file_formats, node=node)


    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)

    if not node.bot:
        app.set_download_id(node, message.id, download_status)

    node.download_status[message.id] = download_status

    file_size = os.path.getsize(file_name) if file_name else 0

    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )

    # rclone upload
    if (
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
    ):
        if await app.upload_file(file_name):
            node.upload_success_count += 1

    await report_bot_download_status(
        node.bot,
        node,
        download_status,
        file_size,
    )


# pylint: disable = R0915,R0914


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    media_dict = _get_media_meta(message)
    msg_file_status = await _get_msg_file_status(media_dict)
    if msg_file_status == Msg_file_Status.File_Exist:
        await update_download_status(media_dict.get('media_size'), media_dict.get('media_size'), message.id,
                                     media_dict.get('filename'),
                                     time.time(),
                                     node, client)
        media_dict['status'] = 1
        db.insert_into_db(media_dict)

        return DownloadStatus.SuccessDownload, media_dict.get('filename')

    task_start_time: float = time.time()
    _media = None

    message_id = media_dict.get('message_id')
    _media = media_dict
    file_name = media_dict.get('file_fullname')
    temp_file_name = media_dict.get('temp_file_fullname')
    media_size = media_dict.get('media_size')
    _type = media_dict.get('msg_type')

    if media_dict.get('chat_username'):
        show_chat_username = media_dict.get('chat_username')
    else:
        show_chat_username = str(media_dict.get('chat_id'))

    ui_file_name = file_name.split('/')[-1]

    if app.hide_file_name:
        ui_file_name = f"****{os.path.splitext(file_name.split('/')[0])}"

    for retry in range(3):
        try:
            temp_file_path = os.path.dirname(temp_file_name)
            chunk_dir = f"{temp_file_path}/{message_id}_chunk"

            if media_size < 1024 * 1024 * CHUNK_MIN:  # 小于CHUNK_MIN M的就用单一文件下载
                chunk_count = 1
                chunk_filename = os.path.join(chunk_dir, "00000000")
                if os.path.exists(chunk_dir):
                    shutil.rmtree(chunk_dir)

                os.makedirs(chunk_dir, exist_ok=True)
                try:
                    await client.download_media(
                        message,
                        file_name=chunk_filename,
                        progress=update_download_status,
                        progress_args=(
                            message_id,
                            ui_file_name,
                            task_start_time,
                            node,
                            client,
                        ),
                    )
                except pyrogram.errors.exceptions.bad_request_400.BadRequest:
                    logger.warning(
                        f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
                    )
                    await asyncio.sleep(RETRY_TIME_OUT)
                    message = await fetch_message(client, message)
                    if _check_timeout(retry, message_id):
                        # pylint: disable = C0301
                        logger.error(
                            f"[{show_chat_username}]{message_id}]: "
                            f"{_t('file reference expired for 3 retries, download skipped.')}"
                        )
                except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                    await asyncio.sleep(wait_err.value)
                    logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
                    _check_timeout(retry, message_id)
                except Exception as e:
                    logger.exception(f"{e}")
                    pass
            else:  #大文件 采用分快下载模式
                if not os.path.exists(chunk_dir):
                    os.makedirs(chunk_dir, exist_ok=True)
                else:
                    temp_file = os.path.join(chunk_dir, '00000000.temp')
                    temp_file_end = os.path.join(chunk_dir, '00000000')
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    if os.path.exists(temp_file_end) and os.path.getsize(temp_file_end) > 1024 * 1024:
                        os.remove(temp_file_end)
                chunk_count = int(media_size / 1024 / 1024) + 1
                chunks_to_down = find_missing_files(chunk_dir, chunk_count)
                if chunks_to_down and len(chunks_to_down) >= 1:  # 至少有一批
                    for start_id, end_id in chunks_to_down:  # 遍历缺失的文件批次
                        down_byte = int(start_id) * 1024 * 1024
                        chunk_it = start_id
                        try:
                            async for chunk in client.stream_media(message, offset=start_id,
                                                                   limit=end_id - start_id + 1):
                                chunk_filename = f"{str(int(chunk_it)).zfill(8)}"
                                chunk_it += 1
                                down_byte += len(chunk)
                                save_chunk_to_file(chunk, chunk_dir, chunk_filename)
                                await update_download_status(down_byte, media_size, message_id, ui_file_name,
                                                             task_start_time,
                                                             node, client)
                                await asyncio.sleep(RETRY_TIME_OUT)
                        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
                            logger.warning(
                                f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
                            )
                            await asyncio.sleep(RETRY_TIME_OUT)
                            message = await fetch_message(client, message)
                            if _check_timeout(retry, message_id):
                                # pylint: disable = C0301
                                logger.error(
                                    f"[{show_chat_username}]{message_id}]: "
                                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                                )
                        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                            await asyncio.sleep(wait_err.value)
                            logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
                            _check_timeout(retry, message_id)
                        except Exception as e:
                            logger.exception(f"{e}")
                            pass

            #判断一下是否下载完成
            if chunk_dir and os.path.exists(chunk_dir):  #chunk_dir存在
                if check_download_finish(media_size, chunk_dir, ui_file_name, chunk_count):  # 大小数量一致
                    try:
                        if merge_chunkfile(folder_path=chunk_dir, output_file=file_name, chunk_count=chunk_count,
                                           file_size=media_size, method='shutil'):
                            # await asyncio.sleep(RETRY_TIME_OUT)
                            if _is_exist(file_name) and os.path.getsize(file_name) == media_size:
                                shutil.rmtree(chunk_dir)

                            media_dict['status'] = 1
                            db.insert_into_db(media_dict)

                            logger.success(f"完成下载{file_name}...剩余：{queue.qsize()}")

                            return DownloadStatus.SuccessDownload, file_name
                    except Exception as e:
                        logger.exception(f"Failed to merge files: {e}")
                        pass
            else:
                pass
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message_id):
                # pylint: disable = C0301
                logger.error(
                    f"[{show_chat_username}]{message_id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
            _check_timeout(retry, message_id)

        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{show_chat_username}]{message_id}, "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message_id):
                logger.error(
                    f"[{show_chat_username}]{message_id}: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"[{show_chat_username}]{message_id}: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


async def worker(client: pyrogram.client.Client):
    """Work for download task"""
    while app.is_running:
        try:
            item = await queue.get()
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission:
                continue

            if node.client:
                await download_task(node.client, message, node)
            else:
                await download_task(client, message, node)
        except Exception as e:
            logger.exception(f"{e}")




async def download_chat_task(client: pyrogram.Client,chat_download_config: ChatDownloadConfig,node: TaskNode,):

    if str(node.chat_id).isdigit():
        real_chat_id = 0 - node.chat_id - 1000000000000
    else:
        real_chat_id = node.chat_id

    chat_download_config.node = node

    if chat_download_config.ids_to_retry:
        retry_ids = list(chat_download_config.ids_to_retry)
        logger.info(f"[{node.chat_id}]{_t('Downloading files failed during last run')}...")
        downloading_messages = Optional[AsyncGenerator["types.Message", None]]
        batch_size = 200
        for i in range(0, len(retry_ids), batch_size):
            batch_files = retry_ids[i:i + batch_size]
            try:
                downloading_messages = await client.get_messages(  # type: ignore
                    chat_id=real_chat_id, message_ids=batch_files
                )
            except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                await asyncio.sleep(wait_err.value)
            except asyncio.TimeoutError:
                logger.error(_t("Operation timed out"))
            except ConnectionError:
                logger.error(_t("Network connection error"))
            except Exception as e:
                logger.exception("{}", e)

            if downloading_messages and len(downloading_messages) > 0:
                try:
                    for message in downloading_messages:
                        if need_skip_message(message, chat_download_config, app):  # 不在下载范围内
                            node.download_status[message.id] = DownloadStatus.SkipDownload
                            msg = db.getMsg(node.chat_id, message.id, 2)
                            msg.status = 5
                            msg.save()
                            logger.info(f"[{node.chat_id}]{msg.filename}文件已被频道删除，跳过")
                            continue
                        else:
                            await add_download_task(message, node)

                    await asyncio.sleep(RETRY_TIME_OUT)
                except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                    await asyncio.sleep(wait_err.value)
                except Exception as e:
                    logger.exception(f"{e}")

            await asyncio.sleep(RETRY_TIME_OUT)


    """Download all task"""
    proxies_num  = len(app.proxies)
    for retries in range(proxies_num):
        try:
            client.proxy = app.proxies[retries]
            messages_iter = get_chat_history_v2(
                client,
                real_chat_id,
                limit=node.limit,
                max_id=node.end_offset_id,
                offset_id=chat_download_config.last_read_message_id,
                reverse=True,
            )

            async for message in messages_iter:  # type: ignore

                if need_skip_message(message, chat_download_config, app):  # 不在下载范围内
                    node.download_status[message.id] = DownloadStatus.SkipDownload
                    continue
                else:
                    await add_download_task(message, node)

            chat_download_config.need_check = True
            chat_download_config.total_task = node.total_task
            node.is_running = True
            break

        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
        except asyncio.TimeoutError:
            logger.error(_t("Operation timed out"))
        except ConnectionError:
            logger.error(_t("Network connection error"))
        except Exception as e:
            logger.exception("{e}")
            if 'Connection lost' in str(e):
                continue

    app.update_config()


async def download_all_chat(client: pyrogram.Client, chat_download_items ):
    """Download All chat"""

    for key, value in chat_download_items:
        value.node = TaskNode(chat_id=key)
        try:
            logger.info(f"开始读取Chat:{key}...")
            await download_chat_task(client, value, value.node)
            logger.info(f"读取Chat:{key}完毕...")
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True


async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or app.restart_program:
            break

        await asyncio.sleep(1)


def _exec_loop():
    """Exec loop"""

    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    await client.stop()


def main():
    """Main function of the downloader."""
    tasks = []
    if app.proxies:
        proxy = app.proxies[0]
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy= proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
    )
    try:
        app.pre_run()
        init_web(app)

        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        # 假设 self.app.chat_download_config.items() 返回一个列表
        items = list(app.chat_download_config.items())

        # 计算每份的大小
        n = len(items)
        chunk_size = len(items) // app.max_download_task
        chunks = [items[i:i + chunk_size] for i in range(0, n, chunk_size)]

        # 分段循环处理
        for chunk in chunks:
            task = app.loop.create_task(download_all_chat(client, chunk))
            tasks.append(task)

        for _ in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except asyncio.TimeoutError:
        logger.error(_t("Operation timed out"))
    except ConnectionError:
        logger.error(_t("Network connection error"))
    except Exception as e:
        logger.exception("{e}")
    finally:
        app.update_config()
        app.is_running = False
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        app.loop.run_until_complete(stop_server(client))
        for task in tasks:
            task.cancel()
        logger.info(_t("Stopped!"))
        logger.info(f"{_t('update config')}......")
        logger.success(
            f"{_t('Updated last read message_id to config file')},"
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if not _check_config():
        logger.error("Configuration check failed. Exiting.")
    else:
        main()
