import time
from threading import Lock, Condition
from . import ProxyExceptions
from collections import deque

class _ProxyDict(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            raise ProxyExceptions.UnknownProxy(f"Unknown proxy: {item}")

class _ProxyQueue(deque):
    def __init__(self, ProxyPoolLink, clean_proxy_list):
        super().__init__(clean_proxy_list)
        self.ProxyPoolLink = ProxyPoolLink
        self.alter_lock = Lock()

    def peek(self):
        return self[0]

    def pop(self):
        with self.alter_lock:
            return super().pop()

    def remove(self, item):
        with self.alter_lock:
            return super().remove(item)

    def popleft(self):
        with self.alter_lock:
            return super().popleft()

    def put(self, proxy):

        with self.alter_lock:
            try:
                super().remove(proxy.proxy_url)
            except ValueError:
                pass

            for i, a in enumerate(self):
                if proxy.timeout <= self.ProxyPoolLink[a].timeout:
                    if i == 0:
                        self.appendleft(proxy.proxy_url)
                    else:
                        self.insert(i, proxy.proxy_url)
                    break
            else:
                self.append(proxy.proxy_url)

class ProxyPool:

    def __init__(self, proxy_list: [str], *, max_time_outs = 0, max_uses = 0, time_out_on_use = 0, replenish_proxies_func = None):

        self.max_time_outs = max_time_outs
        self.max_uses = max_uses
        self.time_out_on_use = time_out_on_use

        self._replenish_condition = Condition()
        self._replenish_lock = Lock()

        self.replenish_proxies_func = replenish_proxies_func

        self._proxy_dict = _ProxyDict({
            a: ProxyData(a) for a in proxy_list
        })

        self._proxy_queue = _ProxyQueue(self, proxy_list)

    def __contains__(self, item):
        return item in self._proxy_dict

    def __getitem__(self, item):
        return self._proxy_dict[item]

    def __len__(self):
        return self.available_proxy_count()

    def add_proxy(self, proxy):
        self._proxy_dict[proxy] = ProxyData(proxy)
        self._proxy_queue.put(self._proxy_dict[proxy])

    def add_proxies(self, proxy_list):
        for a in proxy_list:
            self.add_proxy(a)

    def remove_proxy(self, proxy):
        self._proxy_dict.pop(proxy)
        self._proxy_queue.remove(proxy)

    def remove_proxies(self, proxy_list):
        for a in proxy_list:
            self.remove_proxy(a)

    def available_proxy_count(self):
        prox_counter = 0
        for proxy_str, prox_data in self._proxy_dict.items():

            if self.proxy_valid_to_give(prox_data):
                prox_counter += 1

        return prox_counter

    def Proxy(self, proxy = None):

        if proxy is not None and proxy not in self:
            self.add_proxy(proxy)

        return Proxy(self, proxy)

    def return_proxy(self, proxy):

        if not isinstance(proxy, ProxyData):
            proxy = self._proxy_dict[proxy]

        proxy.given_out_counter -= 1

        if self.proxy_valid_to_give(proxy, ignore_timeout=True):
            self._proxy_queue.put(proxy)
            

    def get_proxy(self, prev_proxy: str = None, *, _replenish = True) -> str:

        with self._replenish_condition:
            while self._replenish_lock.locked():
                self._replenish_condition.wait()

        try:
            proxy_str = self._proxy_queue.popleft()

            prox_data = self._proxy_dict[proxy_str]

            if prox_data.is_timeout():
                self._proxy_queue.put(prox_data)
                raise ProxyExceptions.ProxiesTimeout(f"One proxy will be available at {prox_data.timeout}", prox_data.timeout)

            if prev_proxy is not None:
                self.return_proxy(prev_proxy)

            prox_data.given_out_counter += 1

            return proxy_str

        except IndexError:
            if self.replenish_proxies_func is not None and _replenish:

                self.replenish_proxies()

                return self.get_proxy(prev_proxy = prev_proxy, _replenish = False)

            raise ProxyExceptions.NoValidProxies("No valid proxies available")

    def replenish_proxies(self):
        with self._replenish_condition:

            self._replenish_lock.acquire()

            try:
                self.replenish_proxies_func(self)
                self._replenish_lock.release()
                self._replenish_condition.notify_all()
            except Exception as e:
                self._replenish_lock.release()
                self._replenish_condition.notify_all()
                raise e

    def proxy_valid_to_give(self, proxy, ignore_timeout = False):

        if not isinstance(proxy, ProxyData):
            proxy = self._proxy_dict[proxy]

        return (
                (self.max_time_outs <= 0 or proxy.timed_out_counter < self.max_time_outs)
                and
                (self.max_uses <= 0 or proxy.used_counter < self.max_uses)
                and
                proxy.is_valid(ignore_timeout)
        )

    def proxy_valid_to_use(self, proxy):

        if not isinstance(proxy, ProxyData):
            proxy = self._proxy_dict[proxy]

        return (
                (self.max_uses <= 0 or proxy.used_counter < self.max_uses)
                and
                (self.max_time_outs <= 0 or proxy.timed_out_counter < self.max_time_outs)
                and
                proxy.is_valid()
        )

    def use_proxy(self, proxy: str):
        self._proxy_dict[proxy].use(self.time_out_on_use)

    def timeout_proxy(self, proxy: str, time_sec: int):
        self._proxy_dict[proxy].give_timeout(time_sec)

    def ban_proxy(self, proxy: str):
        self._proxy_dict[proxy].ban()

    def unban_proxy(self, proxy: str):
        self._proxy_dict[proxy].unban()

class ProxyData:

    def __init__(self, proxy_url):
        self.proxy_url = proxy_url
        self.timeout = 0
        self.banned = False
        self.given_out_counter = 0
        self.timed_out_counter = 0
        self.used_counter = 0

    def use(self, use_timeout = 0):
        self.used_counter += 1

        if use_timeout > 0:
            self.give_timeout(use_timeout)

    def give_timeout(self, time_sec):
        self.timeout = time.time() + time_sec
        self.timed_out_counter += 1

    def ban(self):
        self.banned = True

    def unban(self):
        self.banned = False

    def is_valid(self, ignore_timeout = False):
        return (not self.banned) and (ignore_timeout or self.timeout < time.time())

    def is_timeout(self):
        return self.timeout > time.time()


class Proxy:

    def __init__(self, proxy_pool: ProxyPool, proxy = None):
        self.proxy_pool = proxy_pool

        if proxy is None:
            self.assigned_proxy = proxy_pool.get_proxy()
        else:
            self.assigned_proxy = proxy

    def _is_valid(self):
        return self.proxy_pool.proxy_valid_to_use(self.assigned_proxy)

    def use(self):
        if not self._is_valid():
            self.assigned_proxy = self.proxy_pool.get_proxy(self.assigned_proxy)

        self.proxy_pool.use_proxy(self.assigned_proxy)

        return self.assigned_proxy

    def timeout(self, time_sec):
        self.proxy_pool.timeout_proxy(self.assigned_proxy, time_sec)

    def ban(self):
        self.proxy_pool.ban_proxy(self.assigned_proxy)

    def __del__(self):
        if self.assigned_proxy is not None:
            self.proxy_pool.return_proxy(self.assigned_proxy)
