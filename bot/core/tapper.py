import asyncio
import os
import json
import traceback
import aiohttp
import aiofiles
import random
import string

from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from typing import Tuple
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw import types
from urllib.parse import unquote, parse_qs

from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.utils.logger import logger
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import connection_manager
from .headers import headers


class Tapper:
    def __init__(self, tg_client: Client, proxy: str):
        self.session_name = tg_client.name
        self.tg_client = tg_client
        self.user_id = 0
        self.username = None
        self.first_name = None
        self.last_name = None
        self.fullname = None
        self.start_param = None
        self.peer = None
        self.first_run = None
        self.proxy = proxy
        self.gateway_url = "https://gateway.blum.codes"
        self.game_url = "https://game-domain.blum.codes"
        self.wallet_url = "https://wallet-domain.blum.codes"
        self.subscription_url = "https://subscription.blum.codes"
        self.tribe_url = "https://tribe-domain.blum.codes"
        self.user_url = "https://user-domain.blum.codes"
        self.earn_domain = "https://earn-domain.blum.codes"

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self) -> str:
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            with_tg = True

            if not self.tg_client.is_connected:
                with_tg = False
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            self.start_param = random.choices([settings.REF_ID, "ref_QmiirCtfhH"], weights=[75, 25], k=1)[0]
            peer = await self.tg_client.resolve_peer('BlumCryptoBot')
            InputBotApp = types.InputBotAppShortName(bot_id=peer, short_name="app")

            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=InputBotApp,
                platform='android',
                write_allowed=True,
                start_param=self.start_param
            ))

            auth_url = web_view.url
            #print(auth_url)
            tg_web_data = unquote(
                string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0])

            try:
                if self.user_id == 0:
                    information = await self.tg_client.get_me()
                    self.user_id = information.id
                    self.first_name = information.first_name or ''
                    self.last_name = information.last_name or ''
                    self.username = information.username or ''
            except Exception as e:
                print(e)

            if with_tg is False:
                await self.tg_client.disconnect()

            return tg_web_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(
                f"<light-yellow>{self.session_name}</light-yellow> | Unknown error during Authorization: {error}")
            await asyncio.sleep(delay=3)

    async def login(self, http_client: aiohttp.ClientSession, initdata):
        try:
            await http_client.options(url=f'{self.user_url}/api/v1/auth/provider/PROVIDER_TELEGRAM_MINI_APP')
            while True:
                if settings.USE_REF is False:

                    json_data = {"query": initdata}
                    resp = await http_client.post(f"{self.user_url}/api/v1/auth/provider"
                                                  "/PROVIDER_TELEGRAM_MINI_APP",
                                                  json=json_data, ssl=False)
                    if resp.status == 520:
                        logger.warning(f"{self.session_name} | Relogin")
                        await asyncio.sleep(delay=3)
                        continue

                    resp_json = await resp.json()

                    return resp_json.get("token").get("access"), resp_json.get("token").get("refresh")

                else:

                    json_data = {"query": initdata, "username": self.username,
                                 "referralToken": self.start_param.split('_')[1]}

                    resp = await http_client.post(f"{self.user_url}/api/v1/auth/provider"
                                                  "/PROVIDER_TELEGRAM_MINI_APP",
                                                  json=json_data, ssl=False)
                    if resp.status == 520:
                        logger.warning(f"{self.session_name} | Relogin")
                        await asyncio.sleep(delay=3)
                        continue
                    resp_json = await resp.json()

                    if resp_json.get("message") == "rpc error: code = AlreadyExists desc = Username is not available":
                        while True:
                            name = self.username
                            rand_letters = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 8)))
                            new_name = name + rand_letters

                            json_data = {"query": initdata, "username": new_name,
                                         "referralToken": self.start_param.split('_')[1]}

                            resp = await http_client.post(
                                f"{self.user_url}/api/v1/auth/provider/PROVIDER_TELEGRAM_MINI_APP",
                                json=json_data, ssl=False)
                            if resp.status == 520:
                                logger.warning(f"{self.session_name} | Relogin")
                                await asyncio.sleep(delay=3)
                                continue

                            resp_json = await resp.json()

                            if resp_json.get("token"):
                                logger.info(f"{self.session_name} | Registered using ref - {self.start_param} and nickname - {new_name}")
                                return resp_json.get("token").get("access"), resp_json.get("token").get("refresh")

                            elif resp_json.get("message") == 'account is already connected to another user':

                                json_data = {"query": initdata}
                                resp = await http_client.post(f"{self.user_url}/api/v1/auth/provider"
                                                              "/PROVIDER_TELEGRAM_MINI_APP",
                                                              json=json_data, ssl=False)
                                if resp.status == 520:
                                    logger.warning(f"{self.session_name} | Relogin")
                                    await asyncio.sleep(delay=3)
                                    continue
                                resp_json = await resp.json()
                                return resp_json.get("token").get("access"), resp_json.get("token").get("refresh")

                            else:
                                logger.info(f"{self.session_name} | Username taken, retrying register with new name")
                                await asyncio.sleep(1)

                    elif resp_json.get("message") == 'account is already connected to another user':

                        json_data = {"query": initdata}
                        resp = await http_client.post(f"{self.user_url}/api/v1/auth/provider"
                                                      "/PROVIDER_TELEGRAM_MINI_APP",
                                                      json=json_data, ssl=False)
                        if resp.status == 520:
                            logger.warning(f"{self.session_name} | Relogin")
                            await asyncio.sleep(delay=3)
                            continue
                        resp_json = await resp.json()

                        return resp_json.get("token").get("access"), resp_json.get("token").get("refresh")

                    elif resp_json.get("token"):

                        logger.success(f"{self.session_name} | Registered using ref - {self.start_param} and nickname - {self.username}")
                        return resp_json.get("token").get("access"), resp_json.get("token").get("refresh")

        except Exception as error:
            logger.error(f"{self.session_name}| Login error {error}")
            return None, None

    async def claim_task(self, http_client: aiohttp.ClientSession, task_id):
        try:
            resp = await http_client.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/claim',
                                          ssl=False)
            resp_json = await resp.json()

            return resp_json.get('status') == "FINISHED"
        except Exception as error:
            logger.error(f"{self.session_name} | Claim task error {error}")

    async def start_task(self, http_client: aiohttp.ClientSession, task_id):
        try:
            resp = await http_client.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/start',
                                          ssl=False)

        except Exception as error:
            logger.error(f"<light-yellow>{self.session_name}</light-yellow> | Start complete error {error}")

    async def validate_task(self, http_client: aiohttp.ClientSession, task_id, title):
        try:
            keywords = {
                'How to Analyze Crypto?': 'VALUE',
                'Forks Explained': 'GO GET',
                'Secure your Crypto!': 'BEST PROJECT EVER',
                'Navigating Crypto': 'HEYBLUM',
                'What are Telegram Mini Apps?': 'CRYPTOBLUM',
                'Say No to Rug Pull!': 'SUPERBLUM'
            }

            payload = {'keyword': keywords.get(title)}

            resp = await http_client.post(f'{self.earn_domain}/api/v1/tasks/{task_id}/validate',
                                          json=payload, ssl=False)
            resp_json = await resp.json()
            if resp_json.get('status') == "READY_FOR_CLAIM":
                status = await self.claim_task(http_client, task_id)
                if status:
                    return status
            else:
                return False

        except Exception as error:
            logger.error(f"{self.session_name}| Claim task error {error}")

    async def join_tribe(self, http_client: aiohttp.ClientSession):
        tribe_chatnames = [
            'tonstationgames',
            'freecryptoj',
            'cryptoladesov',
            'blumvnd',
            'invest_zonaa',
            'cryptancichat'
        ]

        random_chatname = random.choice(tribe_chatnames)
        title = None

        try:
            resp = await http_client.get(f'{self.tribe_url}/api/v1/tribe/by-chatname/{random_chatname}', ssl=False)
            json_response = await resp.json()
            title = json_response.get('title')

            if title is None:
                logger.warning(f"{self.session_name}| Title not found in response.")
        except Exception as error:
            logger.error(f"{self.session_name} | Get tribe error: {error}")

        try:
            tribe_id = json_response.get('id')

            logger.info(f"{self.session_name} | Attempting to join tribe {title}")
            resp = await http_client.post(f'{self.tribe_url}/api/v1/tribe/{tribe_id}/join', ssl=False)
            text = await resp.text()

            if text == 'OK':
                logger.success(f"{self.session_name} | Joined new tribe <ly>{title}</ly>")
            else:
                logger.info(f"{self.session_name} | Failed to join the tribe. Response: {text}")

        except Exception as error:
            logger.error(f"{self.session_name} | Join tribe error: {error}")

    async def leave_tribe(self, http_client: aiohttp.ClientSession):
        payload = {}

        try:
            resp = await http_client.post(f'{self.tribe_url}/api/v1/tribe/leave',
                                          json=payload,
                                          ssl=False)

            if resp.status == 200:
                logger.info(f"{self.session_name} | Successfully left the tribe.")
            else:
                logger.info(f"{self.session_name} | Failed to leave the tribe. Status code: {resp.status}")

        except Exception as e:
            logger.error(f"{self.session_name} | An error occurred: {e}")

    async def my_tribe(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.get(f'{self.tribe_url}/api/v1/tribe/my', ssl=False)
            json_response = await resp.json()

            tribe_id = json_response.get('id')
            title = json_response.get('title')

            if tribe_id is None and title is None:
                logger.info(f"{self.session_name} | Not found tribe id")
                return None, None
            else:
                logger.info(f"{self.session_name} | Your currently tribe: <ly>{title}</ly>")
                return tribe_id, title

        except aiohttp.ClientResponseError as e:
            logger.error(f"{self.session_name} | HTTP error occurred: {e.status} - {e.message}")
            return None, None
        except Exception as e:
            logger.error(f"{self.session_name} | An unexpected error occurred: {e}")
            return None, None

    async def get_tasks(self, http_client: aiohttp.ClientSession):
        try:
            while True:
                resp = await http_client.get(f'{self.earn_domain}/api/v1/tasks', ssl=False)
                if resp.status not in [200, 201]:
                    continue
                else:
                    break
            resp_json = await resp.json()

            def collect_tasks(resp_json):
                collected_tasks = []
                for task in resp_json:
                    if task.get('sectionType') == 'HIGHLIGHTS':
                        tasks_list = task.get('tasks', [])
                        for t in tasks_list:
                            sub_tasks = t.get('subTasks')
                            if sub_tasks:
                                for sub_task in sub_tasks:
                                    collected_tasks.append(sub_task)
                            if t.get('type') != 'PARTNER_INTEGRATION':
                                collected_tasks.append(t)

                    if task.get('sectionType') == 'WEEKLY_ROUTINE':
                        tasks_list = task.get('tasks', [])
                        for t in tasks_list:
                            sub_tasks = t.get('subTasks', [])
                            for sub_task in sub_tasks:
                                # print(sub_task)
                                collected_tasks.append(sub_task)

                    if task.get('sectionType') == "DEFAULT":
                        sub_tasks = task.get('subSections', [])
                        for sub_task in sub_tasks:
                            tasks = sub_task.get('tasks', [])
                            for task_basic in tasks:
                                collected_tasks.append(task_basic)

                return collected_tasks

            all_tasks = collect_tasks(resp_json)

            return all_tasks
        except Exception as error:
            logger.error(f"{self.session_name} | Get tasks error {error}")
            return []

    async def play_game(self, http_client: aiohttp.ClientSession, play_passes, refresh_token):
        try:
            total_games = 0
            tries = 3
            while play_passes:
                game_id = await self.start_game(http_client=http_client)

                if not game_id or game_id == "cannot start game":
                    logger.info(f"{self.session_name}| Couldn't start play in game!"
                                f" play_passes: {play_passes}, trying again")
                    tries -= 1
                    if tries == 0:
                        logger.warning(f"{self.session_name} | No more trying, gonna skip games")
                        break
                    continue
                else:
                    if total_games != 25:
                        total_games += 1
                        logger.success(f"{self.session_name} | Started playing game")
                    else:
                        logger.info(f"{self.session_name} | Getting new token to play games")
                        while True:
                            (access_token,
                             refresh_token) = await self.refresh_token(http_client=http_client, token=refresh_token)
                            if access_token:
                                http_client.headers["Authorization"] = f"Bearer {access_token}"
                                logger.success(f"{self.session_name} | Got new token")
                                total_games = 0
                                break
                            else:
                                logger.error(f"{self.session_name} | Can`t get new token, trying again")
                                continue

                await asyncio.sleep(random.uniform(30, 40))

                msg, points = await self.claim_game(game_id=game_id, http_client=http_client)
                if isinstance(msg, bool) and msg:
                    logger.info(f"{self.session_name} | Finish play in game!"
                                f" reward: {points}")
                else:
                    logger.info(f"{self.session_name} | Couldn't play game,"
                                f" msg: {msg} play_passes: {play_passes}")
                    break

                await asyncio.sleep(random.uniform(1, 5))

                play_passes -= 1
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during play game: {e}")

    async def start_game(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.post(f"{self.game_url}/api/v2/game/play", ssl=False)
            response_data = await resp.json()
            if "gameId" in response_data:
                return response_data.get("gameId")
            elif "message" in response_data:
                return response_data.get("message")
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during start game: {e}")

    async def claim_game(self, game_id: str, http_client: aiohttp.ClientSession):
        try:
            points = random.randint(settings.POINTS[0], settings.POINTS[1])
            json_data = {"gameId": game_id, "points": points}

            resp = await http_client.post(f"{self.game_url}/api/v2/game/claim", json=json_data,
                                          ssl=False)
            if resp.status != 200:
                resp = await http_client.post(f"{self.game_url}/api/v2/game/claim", json=json_data,
                                              ssl=False)

            txt = await resp.text()

            return True if txt == 'OK' else txt, points
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during claim game: {e}")

    async def claim(self, http_client: aiohttp.ClientSession):
        try:
            while True:
                resp = await http_client.post(f"{self.game_url}/api/v1/farming/claim", ssl=False)
                if resp.status not in [200, 201]:
                    continue
                else:
                    break

            resp_json = await resp.json()

            return int(resp_json.get("timestamp") / 1000), resp_json.get("availableBalance")
        except Exception as e:
            logger.info(f"{self.session_name} | Error occurred during claim: {e}")

    async def start(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.post(f"{self.game_url}/api/v1/farming/start", ssl=False)

            if resp.status != 200:
                resp = await http_client.post(f"{self.game_url}/api/v1/farming/start", ssl=False)
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during start: {e}")

    async def friend_balance(self, http_client: aiohttp.ClientSession):
        try:
            while True:
                resp = await http_client.get(f"{self.user_url}/api/v1/friends/balance", ssl=False)
                if resp.status not in [200, 201]:
                    continue
                else:
                    break
            resp_json = await resp.json()
            claim_amount = resp_json.get("amountForClaim")
            is_available = resp_json.get("canClaim")

            return (claim_amount,
                    is_available)
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during friend balance: {e}")

    async def friend_claim(self, http_client: aiohttp.ClientSession):
        try:

            resp = await http_client.post(f"{self.user_url}/api/v1/friends/claim", ssl=False)
            resp_json = await resp.json()
            amount = resp_json.get("claimBalance")
            if resp.status != 200:
                resp = await http_client.post(f"{self.user_url}/api/v1/friends/claim", ssl=False)
                resp_json = await resp.json()
                amount = resp_json.get("claimBalance")

            return amount
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during friends claim: {e}")

    async def balance(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.get(f"{self.game_url}/api/v1/user/balance", ssl=False)
            resp_json = await resp.json()

            timestamp = resp_json.get("timestamp")
            play_passes = resp_json.get("playPasses")

            start_time = None
            end_time = None
            if resp_json.get("farming"):
                start_time = resp_json["farming"].get("startTime")
                end_time = resp_json["farming"].get("endTime")

            return (int(timestamp / 1000) if timestamp is not None else None,
                    int(start_time / 1000) if start_time is not None else None,
                    int(end_time / 1000) if end_time is not None else None,
                    play_passes)
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during balance: {e}")

    async def wallet(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.get(f"{self.wallet_url}/api/v1/wallet/my/points/balance", ssl=False)

            if resp.status != 200:
                logger.error(f"{self.session_name} | Failed to retrieve balance. Status code: {resp.status}")
                return None

            resp_json = await resp.json()
            # self.info(f"Response JSON for balance: {resp_json}")

            points = resp_json.get("points", [])
            if points:
                balance = points[0].get("balance")
                return float(balance) if balance else None
            else:
                logger.error(f"{self.session_name} | No points found in response.")
                return None

        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during balance retrieval: {e}")
            return None

    async def claim_daily_reward(self, http_client: aiohttp.ClientSession):
        try:
            resp = await http_client.post(f"{self.game_url}/api/v1/daily-reward?offset=-180",
                                          ssl=False)
            txt = await resp.text()
            return True if txt == 'OK' else txt
        except Exception as e:
            logger.error(f"{self.session_name} | Error occurred during claim daily reward: {e}")

    async def refresh_token(self, http_client: aiohttp.ClientSession, token):
        if "Authorization" in http_client.headers:
            del http_client.headers["Authorization"]
        json_data = {'refresh': token}
        resp = await http_client.post(f"{self.user_url}/api/v1/auth/refresh", json=json_data, ssl=False)
        resp_json = await resp.json()

        return resp_json.get('access'), resp_json.get('refresh')

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        if not self.proxy:
            logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
            return

        access_token = None
        refresh_token = None
        login_need = True

        proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
        http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
        connection_manager.add(http_client)

        if not await self.check_proxy(http_client):
            logger.error(f"{self.session_name} | Proxy check failed. Aborting operation.")
            return

        while True:
            try:
                if http_client.closed:
                    if proxy_conn:
                        if not proxy_conn.closed:
                            await proxy_conn.close()

                    proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
                    http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
                    connection_manager.add(http_client)

                if login_need:
                    if "Authorization" in http_client.headers:
                        del http_client.headers["Authorization"]

                    init_data = await self.get_tg_web_data()

                    access_token, refresh_token = await self.login(http_client=http_client, initdata=init_data)

                    http_client.headers["Authorization"] = f"Bearer {access_token}"
                    self.headers["Authorization"] = f"Bearer {access_token}"

                    if self.first_run is not True:
                        logger.success(f"{self.session_name} | Logged in successfully")
                        self.first_run = True

                    login_need = False

                timestamp, start_time, end_time, play_passes = await self.balance(http_client=http_client)
                balance = await self.wallet(http_client)

                if balance is not None:
                    logger.info(f"{self.session_name} | Balance: <green>{balance:,.0f}</green> BP | You have <ly>{play_passes}</ly> play passes")

                msg = await self.claim_daily_reward(http_client=http_client)
                if isinstance(msg, bool) and msg:
                    logger.success(f"{self.session_name} | Claimed daily reward!")

                claim_amount, is_available = await self.friend_balance(http_client=http_client)

                if claim_amount != 0 and is_available:
                    amount = await self.friend_claim(http_client=http_client)
                    logger.success(f"{self.session_name} | Claimed friend ref reward <cyan>{amount}</cyan>")

                # if play_passes and play_passes > 0 and settings.PLAY_GAMES is True:
                #     await self.play_game(http_client=http_client, play_passes=play_passes, refresh_token=refresh_token)

                tribe_id, title = await self.my_tribe(http_client=http_client)
                await asyncio.sleep(random.randint(5, 15))

                # if tribe_id == '':
                #     await self.leave_tribe(http_client=http_client)
                #     await asyncio.sleep(random.randint(10, 45))
                #     await self.join_tribe(http_client=http_client)

                await asyncio.sleep(random.randint(10, 45))

                if settings.TASKS is True:
                    tasks = await self.get_tasks(http_client=http_client)

                    for task in tasks:
                        if task.get('status') == "NOT_STARTED" and task.get('type') != "PROGRESS_TARGET":
                            logger.info(f"{self.session_name} | Started doing task <ly>{task['title']}</ly>")
                            await self.start_task(http_client=http_client, task_id=task["id"])
                            await asyncio.sleep(0.5)

                    await asyncio.sleep(5)

                    tasks = await self.get_tasks(http_client=http_client)

                    for task in tasks:
                        if task.get('status'):
                            if task['status'] == "READY_FOR_CLAIM" and task['type'] != 'PROGRESS_TASK':
                                status = await self.claim_task(http_client=http_client, task_id=task["id"])
                                if status:
                                    logger.success(f"{self.session_name} | Claimed task <ly>{task['title']}</ly>")
                                await asyncio.sleep(0.5)

                            elif task['status'] == "READY_FOR_VERIFY" and task['validationType'] == 'KEYWORD':
                                status = await self.validate_task(http_client=http_client, task_id=task["id"],
                                                                  title=task['title'])
                                if status:
                                    logger.success(f"{self.session_name} | Confirmed task <ly>{task['title']}</ly>")
                else:
                    logger.info(f"{self.session_name} | TASKS setting is disabled, skipping task execution.")

                await asyncio.sleep(random.uniform(1, 3))

                try:
                    timestamp, start_time, end_time, play_passes = await self.balance(http_client=http_client)

                    if start_time is None and end_time is None:
                        await self.start(http_client=http_client)
                        logger.info(f"{self.session_name} | Start farming!")

                    elif (start_time is not None and end_time is not None and timestamp is not None and
                          timestamp >= end_time):
                        timestamp, balance = await self.claim(http_client=http_client)
                        logger.info(f"{self.session_name} | Claimed reward!")

                except Exception as e:
                    logger.info(f"{self.session_name} | Error in farming management: {e}")

            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                   f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if proxy_conn:
                    if not proxy_conn.closed:
                        await proxy_conn.close()
                connection_manager.remove(http_client)

                next_claim = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(next_claim // 3600)
                minutes = (int(next_claim % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep before wake up <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(next_claim)


async def run_tapper(tg_client: Client, proxy: str):
    session_name = tg_client.name
    if not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
