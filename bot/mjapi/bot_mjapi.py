""" Bot for mjapi"""

import time
import json
import os
import datetime
import random
import requests
import threading
from common.settings import Settings
from common.log_helper import LOGGER
from common.utils import random_str
from common.mj_helper import MjaiType
from bot.mjapi.mjapi import MjapiClient

from bot.bot import Bot, GameMode


class ProxyManager:
    """管理代理池和token"""
    def __init__(self, proxy_file="port.json", token_file="proxy_tokens.json"):
        # 尝试多个可能的路径
        self.proxy_file = self._find_file(proxy_file)
        self.token_file = self._find_file(token_file)
        self.proxies = self._load_proxies()
        self.tokens = self._load_tokens()
        self.current_proxy = None
        self.current_proxy_url = None
        self.failed_proxies = set()  # 记录失败的代理

        # 总是添加直连模式作为备选，但不会轻易使用
        LOGGER.info(f"添加直连模式作为最后备选")
        self.proxies["direct"] = {"name": "直连模式", "ip": "direct", "region": "本地"}

        # 默认不使用直连模式，除非没有其他代理
        if not self.current_proxy:
            if len(self.proxies) > 1:  # 如果有代理，不默认使用直连
                self.reset_failed_proxies()
            else:  # 如果只有直连模式，才默认使用
                self.current_proxy = "direct"
                self.current_proxy_url = None

    def _find_file(self, filename):
        """尝试在多个路径查找文件"""
        # 可能的路径列表
        possible_paths = [
            filename,  # 当前目录
            os.path.join(os.path.dirname(__file__), filename),  # 模块所在目录
            os.path.join(os.path.dirname(os.path.dirname(__file__)), filename),  # 上级目录
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename),  # 再上级目录
        ]

        for path in possible_paths:
            if os.path.exists(path):
                LOGGER.info(f"找到配置文件: {path}")
                return path

        # 如果找不到文件，返回原始文件名，后续处理时会创建新文件
        LOGGER.warning(f"未找到配置文件 {filename}，将在当前目录创建")
        return filename

    def _load_proxies(self):
        """加载代理列表"""
        try:
            if os.path.exists(self.proxy_file):
                with open(self.proxy_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                LOGGER.warning(f"代理文件 {self.proxy_file} 不存在")
            return {}
        except Exception as e:
            LOGGER.error(f"加载代理文件失败: {e}")
            return {}

    def _load_tokens(self):
        """加载令牌信息"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            LOGGER.error(f"加载token文件失败: {e}")
            return {}

    def _save_tokens(self):
        """保存令牌信息"""
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(os.path.abspath(self.token_file)), exist_ok=True)
            with open(self.token_file, 'w', encoding='utf-8') as f:
                json.dump(self.tokens, f, indent=2)
        except Exception as e:
            LOGGER.error(f"保存token文件失败: {e}")

    def reset_failed_proxies(self):
        """重置失败代理记录"""
        self.failed_proxies = set()
        LOGGER.info("重置失败代理记录")

    def mark_proxy_failed(self, port):
        """标记代理为失败状态"""
        if port != "direct":
            self.failed_proxies.add(port)
            LOGGER.info(f"标记代理 {port} 为失败状态，当前失败代理数: {len(self.failed_proxies)}")

    def get_proxy(self, force_new=False):
        """获取一个可用的代理，优先使用非直连代理"""
        # 如果当前代理可用且不强制切换，则继续使用
        if not force_new and self.current_proxy and self.is_token_valid(self.current_proxy) and self.current_proxy not in self.failed_proxies:
            LOGGER.info(f"继续使用当前代理: {self.current_proxy}")
            if self.current_proxy == "direct":
                return self.current_proxy, None
            return self.current_proxy, self.current_proxy_url

        # 获取所有可用代理（排除direct和已失败的）
        available_proxies = [key for key in self.proxies.keys()
                            if key != "direct" and key not in self.failed_proxies]

        LOGGER.info(f"当前可用代理数: {len(available_proxies)}, 已失败代理数: {len(self.failed_proxies)}")

        # 如果没有可用代理，则重置失败记录并重新尝试所有代理
        if not available_proxies and self.failed_proxies:
            LOGGER.warning("所有代理都已失败，重置失败记录重新尝试")
            self.reset_failed_proxies()
            available_proxies = [key for key in self.proxies.keys() if key != "direct"]

        # 如果代理列表为空，或者所有代理都已尝试失败，则使用直连模式
        if not available_proxies:
            LOGGER.warning("没有可用代理，将使用直连模式")
            self.current_proxy = "direct"
            self.current_proxy_url = None
            return "direct", None

        # 提取两组代理：有效token的代理和无token的代理
        valid_token_proxies = []
        no_token_proxies = []

        for port in available_proxies:
            if port in self.tokens and self.is_token_valid(port):
                valid_token_proxies.append(port)
            else:
                no_token_proxies.append(port)

        # 随机选择一个代理，优先使用有有效token的代理
        if valid_token_proxies and not force_new:
            # 使用有效token的代理
            LOGGER.info(f"找到{len(valid_token_proxies)}个有效token的代理")
            port = random.choice(valid_token_proxies)
            LOGGER.info(f"随机选择了有效token的代理: {port}")
        elif available_proxies:
            # 使用无token的代理或被强制刷新的代理
            LOGGER.info(f"没有找到有效token的代理，将从{len(available_proxies)}个可用代理中选择")
            port = random.choice(available_proxies)
            LOGGER.info(f"随机选择了代理: {port}")
        else:
            # 这种情况应该不会发生，但保险起见
            LOGGER.warning("没有可用代理，将使用直连模式")
            self.current_proxy = "direct"
            self.current_proxy_url = None
            return "direct", None

        self.current_proxy = port
        self.current_proxy_url = f"http://127.0.0.1:{port}"
        LOGGER.info(f"选择HTTP代理: {port} ({self.proxies[port]['name']})")

        return port, self.current_proxy_url

    def save_token(self, port, token):
        """保存代理对应的token"""
        self.tokens[port] = {
            "token": token,
            "created_at": datetime.datetime.now().isoformat(),
            "expires_at": (datetime.datetime.now() + datetime.timedelta(hours=1)).isoformat()
        }
        self._save_tokens()

    def get_token(self, port):
        """获取代理对应的token"""
        if port in self.tokens and self.is_token_valid(port):
            return self.tokens[port]["token"]
        return None

    def is_token_valid(self, port):
        """检查token是否有效"""
        # 直连模式没有token，但视为有效
        if port == "direct" and port not in self.tokens:
            return False

        if port not in self.tokens:
            return False

        try:
            expires_at = datetime.datetime.fromisoformat(self.tokens[port]["expires_at"])
            # 提前5分钟认为过期，避免临界点问题
            return datetime.datetime.now() < (expires_at - datetime.timedelta(minutes=5))
        except Exception:
            return False


class BotMjapi(Bot):
    """ Bot using mjapi online API"""
    batch_size = 24
    retries = 3
    retry_interval = 1
    bound = 256
    usage_update_interval = 300  # 每5分钟更新一次使用量统计（300秒）

    """ MJAPI based mjai bot"""
    def __init__(self, setting:Settings) -> None:
        super().__init__("MJAPI Bot")
        self.st = setting
        self.api_usage = None
        self.last_usage_update = 0  # 上次更新使用量的时间戳

        LOGGER.info("初始化MJAPI Bot，开始创建代理管理器")
        self.proxy_manager = ProxyManager()
        LOGGER.info(f"代理管理器已创建，已加载代理数量: {len(self.proxy_manager.proxies)}")

        self.mjapi = None
        LOGGER.info("开始设置MJAPI客户端")
        self._setup_client()
        LOGGER.info("MJAPI客户端设置完成")

        self.id = -1
        self.ignore_next_turn_self_reach:bool = False

        # 启动定时更新使用量的线程
        self.usage_update_thread = threading.Thread(target=self._update_usage_periodically, daemon=True)
        self.usage_update_thread.start()
        LOGGER.info(f"已启动API使用量定时更新线程，更新间隔: {self.usage_update_interval}秒")

    @property
    def info_str(self):
        proxy_info = ""
        if self.proxy_manager.current_proxy and self.proxy_manager.current_proxy in self.proxy_manager.proxies:
            proxy_name = self.proxy_manager.proxies[self.proxy_manager.current_proxy]['name']
            proxy_info = f" 代理: {proxy_name}"
        return f"{self.name} [{self.st.mjapi_model_select}]{proxy_info} (Usage: {self.api_usage})"

    def _setup_client(self, force_new=False):
        """设置MJAPI客户端，包括代理配置"""
        LOGGER.info("开始设置MJAPI客户端，获取代理...")
        port, proxy_url = self.proxy_manager.get_proxy(force_new)
        LOGGER.info(f"获取到代理: port={port}, proxy_url={proxy_url}")

        # 创建新的客户端
        LOGGER.info(f"创建MjapiClient, URL={self.st.mjapi_url}")
        self.mjapi = MjapiClient(self.st.mjapi_url)

        # 如果不是直连模式，则设置代理
        if proxy_url:
            # 设置requests使用代理
            session = requests.Session()
            session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            # 替换mjapi中的session
            self.mjapi.session = session
            LOGGER.info(f"设置HTTP代理: {proxy_url}")
        else:
            LOGGER.info("使用直连模式")

        # 先尝试使用已有token
        token = self.proxy_manager.get_token(port)
        if token:
            LOGGER.info(f"使用已有token进行认证")
            self.mjapi.set_bearer_token(token)
            try:
                # 验证token是否有效
                LOGGER.info("验证token有效性...")
                user_info = self.mjapi.get_user_info()
                LOGGER.info("Token验证成功，不再切换代理")

                # 确保设置模型和获取用量信息
                try:
                    # 获取模型列表
                    model_list = self.mjapi.list_models()
                    if model_list:
                        self.st.mjapi_models = model_list
                        if self.st.mjapi_model_select in model_list:
                            self.model_name = self.st.mjapi_model_select
                            LOGGER.info(f"使用已选模型: {self.model_name}")
                        else:
                            self.model_name = model_list[-1]
                            self.st.mjapi_model_select = self.model_name
                            LOGGER.info(f"已选模型不可用，使用: {self.model_name}")

                    # 获取用量信息
                    self.api_usage = self.mjapi.get_usage()
                    self.st.mjapi_usage = self.api_usage
                    self.last_usage_update = time.time()  # 记录更新时间
                    self.st.save_json()

                    # 如果已经在牌桌上，确保初始化bot
                    if self.initialized and self.seat >= 0:
                        LOGGER.info(f"检测到已在牌桌上(座位:{self.seat})，重新初始化bot...")
                        self._init_bot_impl()
                except Exception as e:
                    # 获取模型或用量失败不影响主要功能，只记录警告
                    LOGGER.warning(f"获取模型或用量信息失败: {e}")

                # 成功验证token后直接返回，不再尝试登录
                return
            except Exception as e:
                LOGGER.warning(f"已存token无效，将重新登录: {e}")
                if port != "direct":
                    self.proxy_manager.mark_proxy_failed(port)

        # 重新登录并保存token
        LOGGER.info("准备重新登录获取token...")
        login_success = self._login_or_reg()

        # 如果登录失败，且不是直连模式，尝试切换代理
        if not login_success:
            if port != "direct":
                LOGGER.info("尝试切换到新代理...")
                return self._setup_client(force_new=True)
            else:
                # 直连模式也失败，抛出异常
                raise RuntimeError("登录失败，无法连接到MJAPI服务，所有代理和直连模式均已尝试")

    def _login_or_reg(self):
        """登录或注册，获取token"""
        # 使用trial登录
        try:
            LOGGER.info("尝试使用trail_login登录...")
            self.mjapi.trail_login()
            LOGGER.info("trail_login成功获取token")

            # 保存新获取的token
            if self.mjapi.token and self.proxy_manager.current_proxy:
                LOGGER.info(f"保存token到代理 {self.proxy_manager.current_proxy}")
                self.proxy_manager.save_token(self.proxy_manager.current_proxy, self.mjapi.token)

            LOGGER.info("获取可用模型列表...")
            model_list = self.mjapi.list_models()
            if not model_list:
                raise RuntimeError("No models available in MJAPI")

            self.st.mjapi_models = model_list
            if self.st.mjapi_model_select in model_list:
                # OK
                LOGGER.info(f"使用已选模型: {self.st.mjapi_model_select}")
            else:
                LOGGER.debug(
                    "mjapi selected model %s N/A, using last one from available list %s",
                    self.st.mjapi_model_select, model_list[-1])
                self.st.mjapi_model_select = model_list[-1]

            self.model_name = self.st.mjapi_model_select
            LOGGER.info("获取API使用情况...")
            self.api_usage = self.mjapi.get_usage()
            self.st.mjapi_usage = self.api_usage
            self.last_usage_update = time.time()  # 记录更新时间
            self.st.save_json()
            LOGGER.info("Login to MJAPI successful with model_name=%s, 已找到可用代理和token", self.model_name)
            # 登录成功后，不再进行代理切换
            return True
        except Exception as e:
            LOGGER.error(f"登录失败: {type(e).__name__}: {e}")

            # 标记当前代理为失败状态
            if self.proxy_manager.current_proxy != "direct":
                LOGGER.warning(f"标记当前代理 {self.proxy_manager.current_proxy} 为失败状态")
                self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)

            return False

    def __del__(self):
        LOGGER.debug("Deleting bot %s", self.name)
        if self.initialized:
            try:
                self.mjapi.stop_bot()
            except Exception as e:
                LOGGER.warning(f"停止bot时发生错误: {e}")
        if self.mjapi and self.mjapi.token:    # update usage and logout on deleting
            try:
                self.api_usage = self.mjapi.get_usage()
                self.st.mjapi_usage = self.api_usage
                self.st.save_json()
                self.mjapi.logout()
            except Exception as e:
                LOGGER.warning(f"登出时发生错误: {e}")

    def _init_bot_impl(self, _mode:GameMode=GameMode.MJ4P):
        """初始化麻将AI，告知座位信息"""
        try:
            LOGGER.info(f"启动AI Bot，座位: {self.seat}")
            response = self.mjapi.start_bot(self.seat, BotMjapi.bound, self.model_name)
            if isinstance(response, dict) and 'error' in response:
                LOGGER.error(f"启动Bot失败: {response}")
                # 标记当前代理为失败状态
                if self.proxy_manager.current_proxy != "direct":
                    LOGGER.warning(f"标记当前代理 {self.proxy_manager.current_proxy} 为失败状态")
                    self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
                # 重新设置客户端
                self._setup_client(force_new=True)
                # 递归重试一次
                return self._init_bot_impl(_mode)

            LOGGER.info(f"Bot启动成功，座位: {self.seat}, 模型: {self.model_name}")
            self.id = -1
        except Exception as e:
            LOGGER.error(f"启动Bot时发生错误: {type(e).__name__}: {e}")
            # 标记当前代理为失败状态
            if self.proxy_manager.current_proxy != "direct":
                LOGGER.warning(f"标记当前代理 {self.proxy_manager.current_proxy} 为失败状态")
                self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
            # 重新设置客户端
            self._setup_client(force_new=True)
            # 递归重试一次
            return self._init_bot_impl(_mode)

    def _handle_api_error(self, err, retry_count=0):
        """处理API错误，必要时切换代理"""
        if retry_count >= 2:  # 最多尝试2次代理切换
            LOGGER.error(f"多次尝试失败，返回错误: {err}")
            raise err

        # 如果是网络连接错误，才尝试切换代理
        is_connection_error = (
            isinstance(err, requests.exceptions.ConnectionError) or
            isinstance(err, requests.exceptions.Timeout) or
            isinstance(err, requests.exceptions.ReadTimeout) or
            "Cannot connect" in str(err) or
            "Connection refused" in str(err) or
            "Connection reset" in str(err)
        )

        if is_connection_error:
            LOGGER.warning(f"网络连接错误，尝试切换代理: {type(err).__name__}: {str(err)}")

            # 标记当前代理为失败状态
            if self.proxy_manager.current_proxy != "direct":
                self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)

            # 强制切换代理
            self._setup_client(force_new=True)
        else:
            # 对于非连接错误，可能是API本身的问题，尝试重新登录但不切换代理
            LOGGER.warning(f"API错误(非连接问题)，尝试重新登录: {type(err).__name__}: {str(err)}")
            try:
                # 重新获取token
                self.mjapi.trail_login()
                if self.mjapi.token and self.proxy_manager.current_proxy:
                    self.proxy_manager.save_token(self.proxy_manager.current_proxy, self.mjapi.token)
            except Exception as login_err:
                LOGGER.error(f"重新登录失败: {login_err}")
                # 登录失败才切换代理
                if self.proxy_manager.current_proxy != "direct":
                    self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
                self._setup_client(force_new=True)

        return retry_count + 1

    def _process_reaction(self, reaction, recurse):
        if reaction:
            # 检查是否是错误响应
            if isinstance(reaction, dict) and 'error' in reaction:
                LOGGER.error(f"Bot react error: {reaction}")
                # 标记当前代理为失败状态
                if self.proxy_manager.current_proxy != "direct":
                    LOGGER.warning(f"标记当前代理 {self.proxy_manager.current_proxy} 为失败状态 (API错误)")
                    self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
                # 重新设置客户端（切换到新代理）
                self._setup_client(force_new=True)
                return None
            pass
        else:
            return None

        # 检查api是否正常，如果异常则重新登录
        if 'type' not in reaction:
            LOGGER.error(f"Bot react error: {reaction}")
            # 标记当前代理为失败状态
            if self.proxy_manager.current_proxy != "direct":
                LOGGER.warning(f"标记当前代理 {self.proxy_manager.current_proxy} 为失败状态 (缺少type字段)")
                self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
            # 重新设置客户端（切换到新代理）
            self._setup_client(force_new=True)
            return None

        # process self reach
        if recurse and reaction['type'] == MjaiType.REACH and reaction['actor'] == self.seat:
            LOGGER.debug("Send reach msg to get reach_dahai.")
            reach_msg = {'type': MjaiType.REACH, 'actor': self.seat}
            reach_dahai = self.react(reach_msg, recurse=False)
            reaction['reach_dahai'] = self._process_reaction(reach_dahai, False)
            self.ignore_next_turn_self_reach = True

        return reaction

    def react(self, input_msg:dict, recurse=True) -> dict | None:
        # input_msg['can_act'] = True
        msg_type = input_msg['type']
        if self.ignore_next_turn_self_reach:
            if  msg_type == MjaiType.REACH and input_msg['actor'] == self.seat:
                LOGGER.debug("Ignoring repetitive self reach msg, reach msg already sent to AI last turn")
                return None
            self.ignore_next_turn_self_reach = False

        old_id = self.id
        err = None
        self.id = (self.id + 1) % BotMjapi.bound
        reaction = None

        retry_count = 0
        for _ in range(BotMjapi.retries):
            try:
                reaction = self.mjapi.act(self.id, input_msg)
                err = None
                break
            except Exception as e:
                err = e
                retry_count = self._handle_api_error(e, retry_count)
                time.sleep(BotMjapi.retry_interval)

        if err:
            self.id = old_id
            raise err
        return self._process_reaction(reaction, recurse)

    def react_batch(self, input_list: list[dict]) -> dict | None:
        if self.ignore_next_turn_self_reach and len(input_list) > 0:
            if input_list[0]['type'] == MjaiType.REACH and input_list[0]['actor'] == self.seat:
                LOGGER.debug("Ignoring repetitive self reach msg, reach msg already sent to AI last turn")
                input_list = input_list[1:]
            self.ignore_next_turn_self_reach = False
        if len(input_list) == 0:
            return None
        num_batches = (len(input_list) - 1) // BotMjapi.batch_size + 1
        reaction = None
        for (i, start) in enumerate(range(0, len(input_list), BotMjapi.batch_size)):
            reaction = self._react_batch_impl(
                input_list[start:start + BotMjapi.batch_size],
                can_act= i + 1 == num_batches)
        return reaction

    def _react_batch_impl(self, input_list, can_act):
        if len(input_list) == 0:
            return None
        batch_data = []

        old_id = self.id
        err = None
        for (i, msg) in enumerate(input_list):
            self.id = (self.id + 1) % BotMjapi.bound
            if i + 1 == len(input_list) and not can_act:
                msg = msg.copy()
                msg['can_act'] = False
            action = {'seq': self.id, 'data': msg}
            batch_data.append(action)
        reaction = None

        retry_count = 0
        for _ in range(BotMjapi.retries):
            try:
                reaction = self.mjapi.batch(batch_data)
                err = None
                break
            except Exception as e:
                err = e
                retry_count = self._handle_api_error(e, retry_count)
                time.sleep(BotMjapi.retry_interval)

        if err:
            self.id = old_id
            raise err
        return self._process_reaction(reaction, True)

    def _update_usage_periodically(self):
        """定期更新API使用量统计"""
        while True:
            try:
                # 如果已经初始化并且距离上次更新已经过了指定的时间
                current_time = time.time()
                if (self.mjapi and self.mjapi.token and
                    (current_time - self.last_usage_update) >= self.usage_update_interval):
                    LOGGER.info("定时更新API使用量统计...")
                    self.api_usage = self.mjapi.get_usage()
                    self.st.mjapi_usage = self.api_usage
                    self.st.save_json()
                    self.last_usage_update = current_time
                    LOGGER.info(f"API使用量更新完成：{self.api_usage}")
            except Exception as e:
                LOGGER.warning(f"更新API使用量时发生错误: {e}")

            # 休眠一段时间后再检查
            time.sleep(60)  # 每分钟检查一次是否需要更新
