import yaml
import hmac
import hashlib
import base64
import urllib.parse
from collections.abc import Iterable

with open("oss_exporter.yml", "r") as f:
    cfg = yaml.load(f, Loader=yaml.FullLoader)


def percent_encode(encode_str):
    encode_str = str(encode_str)
    res = urllib.parse.quote(encode_str.encode('UTF-8'), '')
    res = res.replace('+', '%20')
    res = res.replace('*', '%2A')
    res = res.replace('%7E', '~')
    return res


# 阿里云签名计算
def aliyun_sign(access_secret, http_method, params):
    sorted_parameters = sorted(params.items(), key=lambda params: params[0])
    canonicalized_query_string = ''
    for (k, v) in sorted_parameters:
        canonicalized_query_string += '&' + percent_encode(k) + '=' + percent_encode(v)
    string_to_sign = http_method + '&%2F&' + percent_encode(canonicalized_query_string[1:])
    h = hmac.new((access_secret + '&').encode('UTF-8'), string_to_sign.encode('UTF-8'), hashlib.sha1)
    signature = base64.encodebytes(h.digest()).decode('UTF-8').strip()
    return signature


# 移动云签名计算
def eos_sign(access_secret, bucket, date, http_method='HEAD'):
    canonicalized_resource = "/" + bucket
    string_to_sign = http_method + "\n" + "\n" + "\n" + date + "\n" + canonicalized_resource
    h = hmac.new(access_secret.encode('UTF-8'), string_to_sign.encode('UTF-8'), hashlib.sha1)
    signature = base64.encodebytes(h.digest()).decode('UTF-8').strip()
    return signature


# 用于将多层嵌套的数据结构（如列表、元组等）扁平化为一个单层的迭代器
def flatten(items, ignore_types=dict):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types):
            yield from flatten(x)
        else:
            yield x


obs_buckets_info = [
    {
        "bucket_name": bucket,
        "dc": dc["dc_name"],
        "region": region["region_name"],
    }
    for region in cfg['heyingyun']["region"]
    for dc in region["dc"]
    for bucket in dc["buckets"]
]

obs_buckets = {
    region['region_name']: [bucket for dc in region['dc'] for bucket in dc['buckets']]
    for region in cfg['heyingyun']['region']
}

obs_metrics = ['upload_bytes_extranet', 'upload_server_request_latency', 'capacity_standard']

obs_metric_map = {
    'upload_bytes_extranet': 'InternetRecvBandwidth',
    'upload_server_request_latency': 'PutObjectServerLatency',
    'capacity_standard': 'MeteringStorageUtilization',
}
