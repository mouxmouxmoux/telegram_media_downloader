import time
import os
from enum import Enum
from loguru import logger
import regex as re
import shutil
import string
import zhon.hanzi  # 导入中文标点符号集合
import difflib
import mimetypes
from opencc import OpenCC
import json
from ruamel.yaml.comments import CommentedSeq, CommentedMap
from utils.meta_data import MetaData

def load_waste_word_json(json_file):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            waste_words = json.load(f)
        return waste_words

def remove_special_characters(text):
    # 定义一个正则表达式模式，匹配所有非字母、数字、汉字、日文字等字符
    pattern = re.compile(r'[^\w\u4e00-\u9fff\u3040-\u30ff\u31f0-\u31ff]')
    # 使用sub方法替换匹配的字符为空字符串
    cleaned_text = re.sub(pattern, '', text)
    return cleaned_text

def save_list_to_json(list, json_file):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list, f, indent=4, ensure_ascii=False)


def clean_filename(filename :str):
    # 去除以[]括起来的内容
    a = re.sub(r'\[.*?\]', '', filename)

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', a)

    # 去除中文标点符号
    a = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', a)

    pattern = re.compile(r'[\u4e00-\u9fff]')  # 匹配中文字符的正则表达式范围
    if bool(pattern.search(a)):
        # 去掉开头的非汉字字符
        a = re.sub(r'^[^\u4e00-\u9fa5]+', '', a)
    else:
        # 去掉开头的非英文字符
        a = re.sub(r'^[^a-zA-Z]+', '', a)
    # 去掉所有标点符号和特殊字符
    r_str = r"[\/\\\:\*\?\"\<\>#\.\|\n\s/\:*?\"<>\|_ ~，、。？！@#￥%……&*（）+：；《》]+~【】"
    b = re.sub(r_str, " ", a)
    return b

def validate_title_clean(title: str) -> str:
    """Fix if title validation fails

    Parameters
    ----------
    title: str
        Chat title

    """
    if not title or title == '':
        return ''

    r_str = r"[\/\\\:\*\?\"\<\>#\.\|\n\s/\:*?\"<>\|_ ~，、。？！@#￥%……&*（）——+：；《》]+~【】丶̫•"
    b = re.sub(r_str, " ", title)

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', b)

    # 去除中文标点符号
    b = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', b)
    return b

def process_string(string_a: str):
    if not string_a or string_a == '':
        return ''
    a = string_a
    # 发现文件名中有时包含两次后缀，处理掉
    if string_a.lower().endswith('mp3'):
        a = re.sub("mp3", "", a, flags=re.IGNORECASE)
    elif string_a.lower().endswith('mp4'):
        a = re.sub("mp4", "", a, flags=re.IGNORECASE)
    elif string_a.lower().endswith('txt'):
        a = re.sub("txt", "", a, flags=re.IGNORECASE)


    # 清理已知的废文字
    waste_words_patterns = ["Asm糖七baby", "ASM艺彤酱", "ASM艺彤酱", "dea诱耳", "阿木木", "菊花花", "不详", "顾骁梦",
                            "酒Whiskey", "朗读向", "李莎", "林三岁-", "另类", "萝莉一凡", "夢冬", "南征", "清软~喵",
                            "清软喵.*?", "绅士音声", "说人话的吊", "小芸豆儿新地点", "音声", "有声清读", "御姐音",
                            "芝恩㱏", "烛灵儿", "（剧情）", "奶兮酱", "唐樱樱", "迷鹿", "沐醒醒子", "初霸霸", "小芸豆",
                            "直播", "小小奶瓶儿", "渔晚", "（立体声）", "【18+中文音声】", "南飞作品", "步二", "剧情:",
                            "阿稀稀大魔王", "大伊伊", "丸子君", "流景", "是喵宝啊", "桥桥超温柔", "椰子", "黧落大总攻",
                            "绮夏", "小羊喵", "婉儿别闹", "林三岁", "Floa圆圆", "步二", "步一", "步三", "枸杞子",
                            "JOK~清~", "五月织姬", "浅小茉", "夏乔恩", "迷路的卡卡酱", "陈玺颜", "织月黛黛", "剧场:",
                            r'\[人妻熟妇\]', r'\[青春校园\]', r'\[都市生活\]', r'\[古典修真\]', r'\[武侠玄幻\]',
                            r'\[家庭乱伦\]', r'\[现代情感\]',
                            "是喵宝呀", "是幼情呀", "离二烟烟", "林晓蜜", "不要吃咖喱", "奶斯学姐", "人妻熟妇_",
                            "雾心宝贝",
                            "ainnight雨", "暴躁啊御", "羊绵绵", "月一姐姐", "井上鸢御", "大宝 ", "奶斯姐姐", "楠兮",
                            "焱绯", "莉香", "花情女王", "耳屿剧社", "小米ASM", "香取绮罗", "雪音", "小曦老师",
                            "辣不辣", "不二丸叽", "小萌", "小太阳贼大", "圈圈 ", "奶兮酱", "唐樱樱", "曦曦", "沐醒醒",
                            "喵小咪", "音无来未", "温舒蕾", "林暮色", "小一熟了", "子初霸霸火箭", "（全本）", "派派小说",
                            "【完本】", "（未删节）", "步二", "步一", "步三",
                            "全作者", "全图文", "粉樱桃", "萝莉一凡_"
                            "（催眠）",
                            "（系统）",
                            "（原创_催眠类！）",
                            "（校对板）",
                            "（未删节全本）","搜索视频","搜索群组","寻片交流","频道导航",
                            "(完结 番外)", "【完】", "【人妻】", "Discuz", r"【中文.*?】", r"全本$", r"完结$", r"作者.+?$", r"（作者.+?）$",
                            r"【18禁.*?】", r"【3D.*?】", r"【A_SMR.*?】", r"【ASMR.*?】", r"【NJ..*?】", r"【NTR.*?】",
                            r"【Q弹一只菊.*?】",
                            r"【R18.*?】", r"【sophia喵酱.*?】r", r"【YiyiZi.*?】", r"【安里.*?】", r"【安眠.*?】",
                            r"【白杭芷.*?】",
                            r"【病娇.*?】", r"【厂长.*?】", r"【晨曦.*?】", r"【纯爱.*?】", r"【刺猬猫.*?】", r"【催眠】",
                            r"【大饼.*?】r", r"【蒂法.*?】", r"【都市】", r"【短篇.*?】", r"【耳边.*?】", r"【耳机.*?】",
                            r"【耳语.*?】",
                            r"【福利】", r"【付费】", r"【高考应援.*?】", r"【哈尼.*?】", r"【喊麦.*?】r", r"【杭白芷.*?】",
                            r"【合集】", r"【哄睡.*?】", r"【回放.*?】", r"【即兴.*?】", r"【剧场】",
                            r"【剧情】",
                            r"【咖喱.*?】", r"【林晓蜜.*?】", r"【另类】r", r"【乱伦】", r"【绿奴】", r"【曼曼.*?】",
                            r"【猫萝.*?】", r"【迷鹿.*?】", r"【蜜婕.*?】", r"【喵会长.*?】", r"【喵老师.*?】", r"【睦之人.*?】",
                            r"【男性向.*?】", r"【南锦.*?】r", r"【南星社.*?】", r"【南征.*?】", r"【楠兮.*?】", r"【陪睡.*?】",
                            r"【桥桥.*?】", r"【清软喵.*?】", r"【情感.*?】", r"【全集.*?】", r"【群[0-9].*?】",
                            r"【人头麦.*?】r", r"【桑九.*?】", r"【闪亮银.*?】", r"【闪亮银.*?】", r"【绅士.*?】", r"【实录.*?】",
                            r"【是幼情吖.*?】", r"【兽人.*?】", r"【双声道.*?】", r"【睡前故事.*?】", r"【岁岁.*?】r",
                            r"【桃夭.*?】", r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})", r"(\d{4})_(\d{2})_(\d{2})",
                            r"【同人.*?】", r"【完结.*?】", r"【完整.*?】", r"【无人声.*?】", r"【武侠.*?】", r"【希尔薇.*?】",
                            r"【闲话家常.*?】", r"【小剧场.*?】", r"【小咖喱.*?】r", r"【小咪.*?】", r"【小墨.*?】",
                            r"【小苮儿.*?】",
                            r"【小遥.*?】", r"【小窈.*?】", r"【小夜.*?】", r"【小芸豆.*?】", r"【校园.*?】",
                            r"【芯嫒.*?】",
                            r"【羞耻.*?】r", r"【妍希.*?】", r"【厌世.*?】", r"【叶月.*?】", r"【夜听.*?】", r"【夜袭.*?】",
                            r"【葉月.*?】",
                            r"【音频.*?】", r"【音声.*?】", r"【幼情.*?】", r"【诱耳.*?】", r"【渔子溪.*?】r", r"【芸汐.*?】",
                            r"【枕边.*?】", r"【直播.*?】", r"【中文.*?】", r"【助眠.*?】", r"【紫眸.*?】", r"【作者\..*?】",
                            r"作者:.*?", r"全$"]

    for waste_pattern in waste_words_patterns:
        if re.search(waste_pattern, a, flags=re.IGNORECASE):
            a = re.sub(waste_pattern, '', a, flags=re.IGNORECASE)

    # pattern = re.compile(r'[^\w\u4e00-\u9fff\u3040-\u30ff\u31f0-\u31ff‘’-]')  # 匹配中文、引文、日文、数字字符的正则表达式范围
    pattern = r'[^\u4e00-\u9fff\u3040-\u30ff\u31f0-\u31ff\u3400-\u4dbf\uac00-\ud7af\u3000-\u303f\ufe10-\ufe1f\ufe30-\ufe4f\uff00-\uffef\w\s\p{P}]'
    a = re.sub(pattern, ' ', a)

    # if bool(pattern.search(a)):
    #     # 去掉开头的非汉字字符
    #     a = re.sub(r'^[^\u4e00-\u9fa5]+', '', a)
    # else:
    #     # 去掉开头的非英文字符
    #     a = re.sub(r'^[^a-zA-Z]+', '', a)

    pattern = re.compile(r'(^\d+[\s_]*)([^\d].+)')
    match = pattern.match(a)
    if match:
        a = match.groups()[-1]

    pattern = re.compile(r'(.+\d+?)_(\d.+)')
    match = pattern.match(a)
    if match:
        a = f"{match.groups()[0]}-{match.groups()[1]}"

    pattern = re.compile(r'(.*?)([(（【]\d+[)）】])$')
    match = pattern.match(a)
    if match:
        a = match.groups()[0]

    pattern_copy = re.compile(r"([a-zA-Z0-9\u4e00-\u9fa5].+)(\([0-9]+\)+)+")
    match = pattern_copy.match(a)
    last_part = ''
    if match:
        a, last_part = match.groups()
        last_part = re.sub(r'[()（）【】]','',last_part)
    a = a + ' ' +  last_part

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', a)

    # 去除中文标点符号
    a = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', a)

    # 去除其他
    r_str = r"[\/\\\:\*\?\"\<\>#\.\|\n/\:*?\"<>\|_ ，、。？！@#￥%……&*（）+：；《》+【】\]\[]"
    a = re.sub(r_str, " ", a).replace('  ', ' ').replace('--', '-').replace('——', '-').replace('～', '-').strip()

    while '  ' in a:
        a = a.replace('  ', ' ')
    result = t2s(a)
    # print(f"trans from :  {string_a}=TO==>{result}\n")
    return result


def t2s(string_a):
    converter = OpenCC('t2s')
    return converter.convert(string_a)


def string_similar(s1, s2):
    if s1 == '' or s2 == '':
        return 0
    s1a = process_string(s1).lower().replace(' ','')
    s2a = process_string(s2).lower().replace(' ','')
    if s1a == '' or s2a == '':
        return 0
    if s1a == s2a:
        return 1
    similara = difflib.SequenceMatcher(None, s1a, s2a).quick_ratio()

    return similara


def split_string(input_string):
    # 去除空格
    cleaned_string = input_string.replace(" ", "").replace("　", "")

    # 使用正则表达式匹配字母或中文和数字的格式
    # punctuation_all = string.punctuation + zhon.hanzi.punctuation
    pattern = re.compile(r"([a-zA-Z\u4e00-\u9fa5]+)([\d（([【《<].+)")
    match = pattern.match(cleaned_string)

    if match:
        aaa_part, num_part = match.groups()
        return aaa_part, num_part
    else:
        return None, None

def string_sequence (s1, s2):
    s1 = process_string(s1).lower()
    s2 = process_string(s2).lower()
    if s1 == '' or s2 == '' or s1 == s2:
        return False
    s1a,s1b = split_string(s1)
    s2a,s2b = split_string(s2)
    if s1a and s1b and s2a and s2b and s1a == s2a and s1b != s2b:
        return True
    diff = difflib.Differ().compare(s1, s2)
    if not diff or diff == None:
        return False
    for line in diff:
        if line.startswith('+') or line.startswith('-'):
            if not line[2:].isnumeric() and line[2:] not in ['上', '中', '下']:
                return False
            else:
                pass
    return True

def move_file(source_directory, destination_directory, source_filename, destination_filename):
    try:
        # 源文件的完整路径
        source_file_path = os.path.join(source_directory, source_filename)
        # 目标文件的完整路径
        destination_file_path = os.path.join(destination_directory, destination_filename)
        # 创建目录c（如果不存在）
        os.makedirs(destination_directory, exist_ok=True)
        # 移动文件
        shutil.move(source_file_path, destination_file_path)
        # print(f"Moved {source_file_path} to {destination_file_path}")

        # print("File moved successfully!")
    except Exception as e:
        print(f"Error moving file: {e}")

def find_files_in_dir(directory, prefix, filename, filesize):

    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.startswith(prefix) and os.path.getsize(os.path.join(root, file)) > 0:
                matching_files.append(os.path.join(root, file))
            elif not re.match(r"^\[\d+\]", file) :
                if filename.split(' ')[0] in file and os.path.getsize(os.path.join(root, file)) == filesize:
                    matching_files.append(os.path.join(root, file))

    return matching_files


def guess_media_type(file_name_ext: str):

    # video_names = ['avi', 'mpg', 'mpeg', 'mov', 'mp4', 'mkv', 'ts', 'wmv', 'rmvb', 'vob', 'm1v', 'm2v', 'mpv']
    # audio_names = ['avi', 'mpg', 'mpeg', 'mov', 'mp4', 'mkv', 'ts', 'wmv', 'rmvb', 'vob', 'm1v', 'm2v', 'mpv']
    #
    #
    # media_type = 'default'
    docu_names = ["mobi", "azw3", "epub", "dox", "txt", "docx", "pdf", "chm", "rar", "7z", "zip"]
    if file_name_ext.startswith('.'):
        file_name_ext = file_name_ext[1:]

    if file_name_ext in docu_names:
        return "document"

    if '.' in file_name_ext:
        mime_type, _ = mimetypes.guess_type(f"aaaa{file_name_ext}")
    else:
        mime_type, _ = mimetypes.guess_type(f"aaaa.{file_name_ext}")
    if mime_type:
        if mime_type.startswith("video"):
            media_type =  "video"
        elif mime_type.startswith("audio"):
            media_type = "audio"
        elif mime_type.startswith("text"):
            media_type = "document"
        elif mime_type.startswith("image"):
            media_type = "photo"
        else:
            return "default"
    else:
        return "default"
    return media_type

def find_missing_files(folder_path, max_number):
    missing_ranges = []
    max_file_number = max_number -1
    if not os.path.exists(folder_path):
        missing_ranges = [(0,max_file_number)]
        return missing_ranges
    # Create a set to store existing file names
    all_files = set()

    # Iterate through the range of file names
    for i in range(max_number):
        all_files.add(f"{i:08d}")

    # Read the existing file names from the folder
    existing_files = set(os.listdir(folder_path))

    missing_files = all_files - existing_files
    for existing_file in existing_files:
        if os.path.getsize(os.path.join(folder_path, existing_file)) < 1024 * 1024:
            missing_files.add(existing_file)

    # Group consecutive missing file names
    if missing_files is None or len(missing_files) == 0:  # 没有缺失文件
        return [(max_file_number,max_file_number)]
    else:
        start_range = None
        prev_one = 0
        max_range = int(max(missing_files))
        for file_name in sorted(missing_files):
            cursor_idx = int(file_name)
            if start_range is None:  # 第一个元素
                start_range = cursor_idx
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, max_range))
                prev_one = cursor_idx
            elif cursor_idx != prev_one + 1:  # 不连续了
                missing_ranges.append((start_range, prev_one))
                start_range = cursor_idx
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, cursor_idx))
                prev_one = cursor_idx
            else:  # 连续
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, cursor_idx))
                prev_one = cursor_idx

        return missing_ranges

def merge_files_cat(folder_path, output_file ):
    os.system(f"cat '{folder_path}'/* > {output_file}")

def merge_files_shutil(folder_path, output_file):
    # 验证路径是否存在
    if not os.path.isdir(folder_path):
        raise ValueError(f"指定的文件夹路径不存在: {folder_path}")
    file_list = []
    # 获取文件夹中的所有文件
    try:
        file_list = os.listdir(folder_path)
    except Exception as e:
        print(f"指定的文件夹中不存在文件: {e}")

    # 当文件列表中只有一个文件时，执行文件复制操作
    if len(file_list) == 1:
        # 构建源文件的完整路径
        source_file = os.path.join(folder_path, file_list[0])
        # 执行文件复制，将唯一文件内容复制到输出文件
        shutil.copy(source_file, output_file)
        # 结束函数执行，确保不会继续处理
        return

    file_list.sort()  # 确保文件按照一致的顺序合并

    # 确认输出文件存在再删除
    if os.path.exists(output_file) and os.path.isfile(output_file):
        try:
            # 删除文件
            os.remove(output_file)
            print(f"File {output_file} has been removed.")
        except PermissionError:
            print(f"Permission denied: Cannot remove file {output_file}.")
        except Exception as e:
            print(f"An error occurred while removing the file: {e}")

        time.sleep(1)

    if not os.path.exists(output_file):
        try:
            with open(output_file, 'ab') as output_file:
                for file_name in file_list:
                    file_path = os.path.join(folder_path, file_name)
                    # 验证文件路径是否在指定文件夹内
                    if not os.path.realpath(file_path).startswith(os.path.realpath(folder_path)):
                        raise ValueError(f"无效的文件路径: {file_path}")

                    with open(file_path, 'rb') as source:
                        shutil.copyfileobj(source, output_file)
        except Exception as e:
            # 清理部分写入的数据
            if os.path.exists(output_file):
                os.remove(output_file)
            raise RuntimeError(f"合并文件时发生错误: {e}")

def merge_files_write(folder_path, output_file, batch_size=1000):
    # 获取文件夹中的所有文件
    files = os.listdir(folder_path)
    files.sort()  # 确保文件按照一致的顺序合并

    if os.path.exists(output_file):
        os.remove(output_file)

    with open(output_file, 'ab') as output_file:
        for file_name in files:
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'rb') as source:
                output_file.write(source.read())


def get_folder_files_size(folder_path):
    files_size = []
    total_size = 0
    files_count = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            file_size = os.path.getsize(file_path)
            if file_size < 1024 * 1024:
                files_size.append((filename,file_size))
            total_size += file_size
            files_count += 1
    return files_count, total_size, files_size

def add_commented_map_to_seq(seq, map_data):
    """
    向一个 CommentedSeq 对象中添加一个 CommentedMap 对象。

    :param seq: CommentedSeq 对象
    :param map_data: 字典，包含要添加的 CommentedMap 数据
    """
    if not isinstance(seq, CommentedSeq):
        raise TypeError("seq 必须是 CommentedSeq 类型")

    if not isinstance(map_data, dict):
        raise TypeError("map_data 必须是字典类型")

    commented_map = CommentedMap(map_data)
    seq.append(commented_map)


class Msg_db_Status(Enum):
    DB_No_Exist = 0  # 数据库中不存在
    DB_Exist = 1  # 数据库中存在
    DB_Downloading = 2  # 数据库中下载中
    DB_Aka_Exist = 3  # 数据库中等价内容存在
    DB_Aka_Downloading = 4  # 数据库中等价内容下载中
    DB_Passed = 5  # 数据库中等价内容存在


class Msg_file_Status(Enum):
    File_No_Exist = 0  # 文件系统中不存在
    File_Exist = 1  # 文件系统中存在
    File_Aka_Exist = 2  # 文件系统中等价文件存在

def save_chunk_to_file(chunk, file_path, file_name):
    try:
        # 创建目录
        os.makedirs(file_path, exist_ok=True)

        # 文件路径
        file_url = os.path.join(file_path, file_name)

        # 确保文件为空或不存在
        with open(file_url, "wb") as f:
            f.truncate(0)  # 清空文件
            f.write(chunk)

        # 检查文件大小是否正确
        if os.path.getsize(file_url) == len(chunk):
            return True
        else:
            return False

    except Exception as e:
        print(f"Error occurred: {e}")
        return False



def _get_msg_db_status(msg_dict: dict ,db, similar_set, sizerange_min):
    msg_chat_id = msg_dict.get('chat_id')
    try:
        msg_db_status = db.getStatus(msg_chat_id, msg_dict.get('message_id'))
    except Exception as e:
        logger.error(
            f"[{e}].",
            exc_info=True,
        )

    if msg_db_status == 0:  #数据库里没这条数据
        db_files = db.get_similar_files(msg_dict, similar_set, sizerange_min, [1, 2])  #看看是否有等价内容数据 4因为依附于1 暂时不管
        if db_files and len(db_files) >= 1:
            for db_file in db_files:
                if db_file.status == 1 or db_file.status == 4:  #等价存在
                    msg_db_status = Msg_db_Status.DB_Aka_Exist
                    break
                elif db_file.status == 2:  #等价下载中
                    msg_db_status = Msg_db_Status.DB_Aka_Downloading
                    break
        else:
            msg_db_status = Msg_db_Status.DB_No_Exist
    elif msg_db_status == 1:
        msg_db_status = Msg_db_Status.DB_Exist
    elif msg_db_status == 2:
        msg_db_status = Msg_db_Status.DB_Downloading
    elif msg_db_status == 4:
        msg_db_status = Msg_db_Status.DB_Aka_Exist
    elif msg_db_status == 3:
        msg_db_status = Msg_db_Status.DB_Passed

    return msg_db_status


async def _get_msg_file_status(msg_dict: dict):
    msg_file_status = Msg_file_Status.File_No_Exist  # 文件不存在
    if not msg_dict.get('file_fullname'):
        raise TypeError("Invalid argument: file_fullname")
    # if _is_exist(msg_dict.get('file_fullname')) and os.path.getsize(msg_dict.get('file_fullname')) > 0:
    if os.path.exists(msg_dict.get('file_fullname')):
        return Msg_file_Status.File_Exist  #文件存在
    else:
        # 根据前缀找文件
        file_dir = os.path.dirname(msg_dict.get('file_fullname'))
        filename_pre = f"[{msg_dict.get('message_id')}]"
        files = find_files_in_dir(file_dir, filename_pre, msg_dict.get('title'), msg_dict.get('media_size'))
        if len(files) > 0:
            return Msg_file_Status.File_Exist
        else:
            return Msg_file_Status.File_No_Exist

