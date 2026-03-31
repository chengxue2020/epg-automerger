#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG Merger Script
合并多个EPG源并生成统一的XML文件
支持HTTP/HTTPS，自动处理gzip压缩，时区转换
"""

import requests
import gzip
import xml.etree.ElementTree as ET
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set

# ==================== 配置常量 ====================
SOURCE_FILE = 'source_epg.txt'          # EPG源配置文件
OUTPUT_XML = 'epg.xml'                   # 输出文件名
TEMP_DIR_NAME = 'temp_epg_files'         # 临时文件目录
DEFAULT_TIME_FRAME = 48                  # 默认时间范围（小时）
MAX_RETRIES = 2                          # 最大重试次数
DOWNLOAD_TIMEOUT = 30                    # 下载超时（秒）
CHUNK_SIZE = 131072                      # 下载块大小（128KB）

# ==================== 时区配置 ====================
BEIJING_TZ = timezone(timedelta(hours=8))  # 北京时区 UTC+8
UTC = timezone.utc                         # UTC时区


# ==================== 工具函数 ====================
def print_separator(char: str = '=', length: int = 60) -> None:
    """打印分隔线"""
    print(char * length)


def format_size(bytes_size: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


# ==================== 配置解析 ====================
def parse_source(source_file: str) -> Tuple[Dict[str, List[str]], int]:
    """
    解析EPG源配置文件
    
    文件格式：
    第一行：timeframe=48
    后续：URL后跟该源的频道列表
    
    Args:
        source_file: 配置文件路径
        
    Returns:
        (数据源字典, 时间范围)
    """
    try:
        with open(source_file, 'r', encoding='utf-8') as source:
            # 读取时间范围
            first_line = source.readline().strip()
            time_frame_string = first_line.rpartition('=')[2].strip()
            
            try:
                time_frame = int(time_frame_string)
                print(f'✓ 时间范围: {time_frame} 小时')
            except ValueError:
                time_frame = DEFAULT_TIME_FRAME
                print(f'⚠ 未指定时间范围，使用默认值: {DEFAULT_TIME_FRAME} 小时')
            
            print()
            
            # 重置文件指针
            source.seek(0)
            current_source = ''
            data_source: Dict[str, List[str]] = {}
            
            for line_num, line in enumerate(source, 1):
                # 跳过第一行
                if line_num == 1:
                    continue
                
                # 移除注释和空白
                line = line.partition('#')[0].strip()
                if not line:
                    continue
                
                # 判断是URL还是频道ID
                if line.startswith(('http://', 'https://')):
                    current_source = line
                    if current_source not in data_source:
                        data_source[current_source] = []
                elif current_source:
                    channel_id = line
                    if channel_id not in data_source[current_source]:
                        data_source[current_source].append(channel_id)
            
            return data_source, time_frame
            
    except FileNotFoundError:
        print(f'✗ 错误: 配置文件 {source_file} 不存在！')
        sys.exit(1)
    except Exception as e:
        print(f'✗ 错误: 解析配置文件失败 - {e}')
        sys.exit(1)


# ==================== 文件下载 ====================
def download_file(url: str, path: str) -> Optional[str]:
    """
    下载EPG文件，支持HTTP/HTTPS，带重试机制
    
    Args:
        url: 下载URL
        path: 保存路径
        
    Returns:
        成功返回文件路径，失败返回None
    """
    # 提取文件名
    filename = os.path.basename(url)
    if not filename:
        # 根据URL生成文件名
        if 'epg' in url:
            filename = 'epg.xml'
        elif url.endswith('.gz'):
            filename = 'epg.xml.gz'
        else:
            filename = 'epg_data.xml'
    
    # 处理文件名冲突
    download_path = os.path.join(path, filename)
    name, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(download_path):
        download_path = os.path.join(path, f"{name}({counter}){ext}")
        counter += 1
    
    # 设置请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # 为特定域名添加Referer
    if '112114' in url:
        headers['Referer'] = 'https://epg.112114.xyz/'
    elif '51zjy' in url:
        headers['Referer'] = 'https://epg.51zjy.top/'
    elif 'fuyukai' in url:
        headers['Referer'] = 'https://epg.fuyukai.workers.dev/'
    
    # 重试下载
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                wait_time = attempt * 2
                print(f'    ⏳ 第 {attempt} 次重试，等待 {wait_time} 秒...')
                time.sleep(wait_time)
            
            # 创建session
            session = requests.Session()
            
            # 发送GET请求
            response = session.get(
                url,
                headers=headers,
                stream=True,
                timeout=DOWNLOAD_TIMEOUT,
                allow_redirects=True
            )
            
            # 检查响应状态
            if response.status_code == 200:
                # 写入文件
                with open(download_path, 'wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                
                print(f'    ✓ 下载成功: {format_size(downloaded)}')
                session.close()
                return download_path
                
            elif response.status_code == 403:
                print(f'    ✗ 访问被拒绝 (403) - 服务器禁止访问')
                if attempt == MAX_RETRIES:
                    return None
                    
            elif response.status_code == 404:
                print(f'    ✗ 文件不存在 (404)')
                return None
                
            else:
                print(f'    ✗ HTTP错误: {response.status_code}')
                if attempt == MAX_RETRIES:
                    return None
                    
        except requests.exceptions.SSLError as e:
            print(f'    ✗ SSL证书错误: {e}')
            return None
            
        except requests.exceptions.Timeout:
            print(f'    ✗ 连接超时')
            if attempt == MAX_RETRIES:
                return None
                
        except requests.exceptions.ConnectionError:
            print(f'    ✗ 连接错误 - 无法连接到服务器')
            if attempt == MAX_RETRIES:
                return None
                
        except requests.exceptions.RequestException as e:
            print(f'    ✗ 请求错误: {e}')
            if attempt == MAX_RETRIES:
                return None
                
        except Exception as e:
            print(f'    ✗ 未知错误: {e}')
            return None
    
    return None


# ==================== 日期转换 ====================
def convert_date(epg_format_date: str) -> Optional[datetime]:
    """
    转换EPG日期字符串为datetime对象（统一返回UTC时间）
    
    支持格式：
    - 20240101120000 +0800 (带时区)
    - 20240101120000 (无时区，假设为UTC)
    
    Args:
        epg_format_date: EPG格式的日期字符串
        
    Returns:
        UTC datetime对象，失败返回None
    """
    if not epg_format_date:
        return None
    
    try:
        # 尝试解析带时区的时间
        date_obj = datetime.strptime(epg_format_date, '%Y%m%d%H%M%S %z')
        return date_obj.astimezone(UTC)
    except ValueError:
        try:
            # 尝试解析无时区的时间（假设为UTC）
            date_obj = datetime.strptime(epg_format_date, '%Y%m%d%H%M%S')
            return date_obj.replace(tzinfo=UTC)
        except Exception:
            return None


# ==================== EPG处理 ====================
def process_epg_source(
    file_path: str,
    channels_to_process: List[str],
    channel_section: List,
    program_section: List,
    start_utc: datetime,
    time_frame: int
) -> None:
    """
    处理EPG源文件，提取频道和节目信息
    
    Args:
        file_path: EPG文件路径
        channels_to_process: 需要处理的频道列表
        channel_section: 频道列表（用于追加）
        program_section: 节目列表（用于追加）
        start_utc: 起始时间（UTC）
        time_frame: 时间范围（小时）
    """
    # 处理gzip压缩文件
    if file_path.endswith('.gz'):
        dir_path = os.path.dirname(file_path)
        xml_file = os.path.join(dir_path, os.path.basename(file_path).replace('.gz', '.xml'))
        
        try:
            with gzip.open(file_path, 'rb') as gz_file:
                with open(xml_file, 'wb') as xml_file_obj:
                    xml_file_obj.write(gz_file.read())
            os.remove(file_path)
        except Exception as e:
            print(f'    ⚠ 解压失败: {e}')
            return
    else:
        xml_file = file_path
    
    # 解析XML
    try:
        tree = ET.parse(xml_file)
    except ET.ParseError:
        print(f'    ✗ XML格式错误')
        return
    except Exception as e:
        print(f'    ✗ 解析失败: {e}')
        return
    
    # 提取频道
    remaining_channels = set(channels_to_process)
    channels_found = 0
    
    for channel in tree.findall('channel'):
        channel_id = channel.attrib.get('id', '')
        if channel_id in remaining_channels:
            channel_section.append(channel)
            remaining_channels.discard(channel_id)
            channels_found += 1
    
    # 提取节目
    programs_found = 0
    programs_total = 0
    
    for programme in tree.findall('programme'):
        channel_id = programme.attrib.get('channel', '')
        if channel_id in channels_to_process:
            programs_total += 1
            
            program_start = convert_date(programme.attrib.get('start', ''))
            program_stop = convert_date(programme.attrib.get('stop', ''))
            
            if program_start and program_stop:
                # 计算时间差
                start_delta = (program_start - start_utc).total_seconds() / 3600
                stop_delta = (program_stop - start_utc).total_seconds() / 3600
                
                # 过滤时间范围内的节目
                if start_delta < time_frame and stop_delta > 0:
                    program_section.append(programme)
                    programs_found += 1
            else:
                # 时间格式异常，仍然添加
                program_section.append(programme)
                programs_found += 1
    
    # 输出未找到的频道
    if remaining_channels:
        for channel in remaining_channels:
            print(f'    ⚠ 未找到频道: {channel}')
    
    print(f'    📺 频道: {channels_found}/{len(channels_to_process)}')
    print(f'    📅 节目: {programs_found}/{programs_total}')


# ==================== 主函数 ====================
def main() -> None:
    """主函数"""
    # 开始时间
    start_utc = datetime.now(UTC)
    start_beijing = start_utc.astimezone(BEIJING_TZ)
    
    print_separator('=')
    print('EPG Merger v2.0')
    print_separator('=')
    print(f'开始时间: {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'开始时间: {start_utc.strftime("%Y-%m-%d %H:%M:%S")} (UTC)')
    print()
    
    # 解析配置
    print('📖 读取配置文件...')
    sources, time_frame = parse_source(SOURCE_FILE)
    
    if not sources:
        print('✗ 错误: 配置文件中没有找到有效的EPG源')
        sys.exit(1)
    
    print(f'✓ 找到 {len(sources)} 个EPG源')
    print(f'✓ 时间范围: {time_frame} 小时')
    print()
    
    # 准备临时目录
    temp_dir = os.path.relpath(TEMP_DIR_NAME)
    os.makedirs(temp_dir, exist_ok=True)
    
    # 清理临时目录
    print('🧹 清理临时目录...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
    print()
    
    # 处理EPG源
    channel_section: List[ET.Element] = []
    program_section: List[ET.Element] = []
    processed_channels: Set[str] = set()
    success_count = 0
    
    for idx, (source_url, channel_list) in enumerate(sources.items(), 1):
        print_separator('-')
        print(f'📡 源 {idx}/{len(sources)}: {source_url}')
        print(f'   频道数量: {len(channel_list)}')
        
        # 过滤已处理的频道
        new_channels = [ch for ch in channel_list if ch not in processed_channels]
        
        if not new_channels:
            print(f'   ⏭ 跳过: 所有频道已处理')
            print()
            continue
        
        print(f'   新频道: {len(new_channels)}')
        
        # 下载文件
        file_path = download_file(source_url, temp_dir)
        
        # 处理文件
        if file_path:
            process_epg_source(
                file_path, new_channels,
                channel_section, program_section,
                start_utc, time_frame
            )
            processed_channels.update(new_channels)
            success_count += 1
            print(f'   ✓ 处理成功')
        else:
            print(f'   ✗ 下载失败，跳过此源')
        
        print()
    
    # 检查是否有成功处理的源
    if success_count == 0:
        print('✗ 错误: 所有EPG源都下载失败！')
        print('请检查网络连接和源地址是否正确')
        sys.exit(1)
    
    # 生成最终XML
    print_separator('=')
    print('📝 生成最终XML文件...')
    
    root = ET.Element('tv')
    
    # 添加生成信息
    comment = ET.Comment(f' Generated by EPG Merger on {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} Beijing Time ')
    root.append(comment)
    
    # 排序
    channels_sorted = sorted(channel_section, key=lambda c: c.attrib.get('id', '').lower())
    programs_sorted = sorted(
        program_section,
        key=lambda p: (p.attrib.get('channel', '').lower(), p.attrib.get('start', ''))
    )
    
    # 添加到根元素
    for channel in channels_sorted:
        root.append(channel)
    for program in programs_sorted:
        root.append(program)
    
    # 写入文件
    tree = ET.ElementTree(root)
    ET.indent(tree, space='    ', level=0)
    tree.write(OUTPUT_XML, encoding='UTF-8', xml_declaration=True)
    
    # 获取文件大小
    xml_size = os.path.getsize(OUTPUT_XML)
    
    print(f'✓ 输出文件: {OUTPUT_XML}')
    print(f'✓ 文件大小: {format_size(xml_size)}')
    print(f'✓ 总频道数: {len(channels_sorted)}')
    print(f'✓ 总节目数: {len(programs_sorted)}')
    print()
    
    # 清理临时文件
    print('🧹 清理临时文件...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
    print()
    
    # 结束时间
    end_utc = datetime.now(UTC)
    end_beijing = end_utc.astimezone(BEIJING_TZ)
    duration = (end_utc - start_utc).total_seconds()
    
    print_separator('=')
    print('✅ EPG合并完成')
    print_separator('=')
    print(f'结束时间: {end_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'结束时间: {end_utc.strftime("%Y-%m-%d %H:%M:%S")} (UTC)')
    print(f'总耗时: {duration:.2f} 秒')
    print(f'成功处理: {success_count}/{len(sources)} 个源')
    print_separator('=')


# ==================== 程序入口 ====================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\n\n⚠ 用户中断')
        sys.exit(1)
    except Exception as e:
        print(f'\n\n✗ 程序异常: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)