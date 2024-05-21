# ProxyPool v0.1.0

by Myhailo Razbeiko

## Instalation

Install using pip

    pip install proxypool_util

## Usage

### Create a ProxyPool object 

    MyPool = ProxyPool(["https://proxy1:port", "https://proxy2:port", "https://proxy3:port"])

#### Attributes:

- **proxy_list** -- a list of proxies
- **max_give_outs** -- maximum number of simultaneous uses of one proxy
- **max_time_outs** -- maximum number of time-outs one proxy can receive, being banned after exceeding it 
- **max_uses** -- maximum number of uses per proxy
- **time_out_on_use** -- time-out given to proxy after being used

#### Methods:

- **available_proxy_count()** -- returns number of available proxies
- **Proxy()** -- returns ProxyPool.Proxy() linked to this ProxyPool

### Generate proxy object

    MyProxy = MyPool.Proxy()

#### Get an available proxy to use

    temp_proxy = MyProxy.use()

- If no proxies are available **ProxyExceptions.NoValidProxies** will be raised. This can happen due to all proxies being banned.
- If no proxies are available, but at least one is on a time-out **ProxyExceptions.ProxiesTimeout** will be raised. You can access a time-out end timestamp with ProxyExceptions.ProxiesTimeout().timeout.

#### Timeout last proxy

    MyProxy.timeout(25)  # proxy will be returned to proxy pool and not available for 25 seconds 

#### Ban last proxy

    MyProxy.ban()  # proxy will be banned and returned to proxy pool

## Example

