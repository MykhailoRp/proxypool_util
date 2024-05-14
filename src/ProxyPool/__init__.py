import time
import ProxyExceptions

class _ProxyDict(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            raise ProxyExceptions.UnknownProxy(f"Unknown proxy: {item}")

class ProxyPool:

    def __init__(self, proxy_list, *, max_give_outs = 0, max_time_outs = 0, max_uses = 0, time_out_on_use = 0):

        self.max_give_outs = max_give_outs
        self.max_time_outs = max_time_outs
        self.max_uses = max_uses
        self.time_out_on_use = time_out_on_use

        self._proxy_dict = _ProxyDict({
            a: ProxyData() for a in proxy_list
        })

    def __getitem__(self, item):
        return self._proxy_dict[item]

    def available_proxy_count(self):
        prox_counter = 0
        for proxy_str, prox_data in self._proxy_dict.items():

            if self.proxy_valid_to_give(prox_data):
                prox_counter += 1

        return prox_counter

    def Proxy(self, proxy = None):
        return Proxy(self, proxy)

    def get_proxy(self, prev_proxy: str = None) -> str:

        min_timeout = None

        for proxy_str, prox_data in self._proxy_dict.items():

            if self.proxy_valid_to_give(prox_data):

                if prev_proxy is not None:
                    self._proxy_dict[prev_proxy].given_out_counter -= 1

                prox_data.given_out_counter += 1

                return proxy_str

            if self.proxy_valid_to_give(prox_data, ignore_timeout=True):
                assert isinstance(prox_data, ProxyData)
                min_timeout = prox_data.timeout if min_timeout is None else min(min_timeout, prox_data.timeout)

        if min_timeout is None:
            raise ProxyExceptions.NoValidProxies("No valid proxies available")
        else:
            raise ProxyExceptions.ProxiesTimeout(f"One proxy will be available at {min_timeout}", min_timeout)


    def proxy_valid_to_give(self, proxy, ignore_timeout = False):

        if isinstance(proxy, str):
            proxy = self._proxy_dict[proxy]

        return (
                (self.max_give_outs <= 0 or proxy.given_out_counter < self.max_give_outs)
                and
                (self.max_time_outs <= 0 or proxy.timed_out_counter < self.max_time_outs)
                and
                (self.max_uses <= 0 or proxy.used_counter < self.max_uses)
                and
                proxy.is_valid(ignore_timeout)
        )

    def proxy_valid_to_use(self, proxy):

        if isinstance(proxy, str):
            proxy = self._proxy_dict[proxy]

        return (
                (self.max_uses <= 0 or proxy.used_counter < self.max_uses)
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
