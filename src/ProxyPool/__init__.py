import time
from threading import Lock, Condition
from . import ProxyExceptions

class _ProxyDict(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            raise ProxyExceptions.UnknownProxy(f"Unknown proxy: {item}")

class ProxyPool:

    def __init__(self, proxy_list: [str], *, max_time_outs = 0, max_uses = 0, time_out_on_use = 0, replenish_proxies_func = None):

        self.max_time_outs = max_time_outs
        self.max_uses = max_uses
        self.time_out_on_use = time_out_on_use

        self._replenish_condition = Condition()
        self._replenish_lock = Lock()

        self.replenish_proxies_func = replenish_proxies_func

        self._proxy_dict = _ProxyDict({
            a: ProxyData() for a in proxy_list
        })

    def __getitem__(self, item):
        return self._proxy_dict[item]

    def __len__(self):
        return self.available_proxy_count()

    def add_proxies(self, proxy_list):
        temp_list = _ProxyDict({
            a: ProxyData() for a in proxy_list
        })

        self._proxy_dict = temp_list | self._proxy_dict

    def remove_proxies(self, proxy_list):
        for prox in proxy_list:
            self._proxy_dict.pop(prox)

    def clear_unusable(self):
        for proxy_str, prox_data in self._proxy_dict.items():
            if self.proxy_valid_to_use(prox_data):
                self._proxy_dict.pop(proxy_str)

    def available_proxy_count(self):
        prox_counter = 0
        for proxy_str, prox_data in self._proxy_dict.items():

            if self.proxy_valid_to_give(prox_data):
                prox_counter += 1

        return prox_counter

    def Proxy(self, proxy = None):
        return Proxy(self, proxy)

    def return_proxy(self, proxy):
        self._proxy_dict[proxy].given_out_counter -= 1

    def get_proxy(self, prev_proxy: str = None, *, _replenish = True) -> str:

        with self._replenish_condition:
            while self._replenish_lock.locked():
                self._replenish_condition.wait()

        min_timeout = None

        for proxy_str, prox_data in self._proxy_dict.items():

            if self.proxy_valid_to_give(prox_data):

                if prev_proxy is not None:
                    self.return_proxy(prev_proxy)

                prox_data.given_out_counter += 1

                return proxy_str

            if self.proxy_valid_to_give(prox_data, ignore_timeout=True):
                assert isinstance(prox_data, ProxyData)
                min_timeout = prox_data.timeout if min_timeout is None else min(min_timeout, prox_data.timeout)

        if min_timeout is None:

            if self.replenish_proxies_func is not None and _replenish:
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

                return self.get_proxy(prev_proxy = prev_proxy, _replenish = False)

            raise ProxyExceptions.NoValidProxies("No valid proxies available")
        else:
            raise ProxyExceptions.ProxiesTimeout(f"One proxy will be available at {min_timeout}", min_timeout)


    def proxy_valid_to_give(self, proxy, ignore_timeout = False):

        if isinstance(proxy, str):
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

    def __init__(self):

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
