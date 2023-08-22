import datetime
import time
import uuid
import json
from collections import ChainMap
from urllib.parse import urlencode
from utils import aliyun_sign, eos_sign, obs_metric_map
from apig_sdk import signer


# 阿里云云监控数据接口
async def get_aliyun_monitor_oss_data(access_key, access_secret, region, endpoint, session, metric_name, ali_oss_cfg,
                                      dimensions,
                                      params=None):
    if params is None:
        params = {}

    url = 'https://' + endpoint
    params['Format'] = 'json'
    params['Version'] = '2019-01-01'
    params['AccessKeyId'] = access_key
    params['SignatureVersion'] = '1.0'
    params['SignatureMethod'] = 'HMAC-SHA1'
    params['SignatureNonce'] = str(uuid.uuid4())
    params['Timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    params['Action'] = 'DescribeMetricLast'
    params['Namespace'] = 'acs_oss_dashboard'
    params['MetricName'] = metric_name
    params['Dimensions'] = str(dimensions)
    params['Signature'] = aliyun_sign(access_secret, 'GET', params)
    async with session.get(url, params=params, ) as resp:
        assert resp.status == 200, f'url: {url} \n params: {params} \n status {resp.status}'
        res = await resp.json()
        assert res['Code'] == '200', params
        res_points = json.loads(res['Datapoints'])
        assert len(res_points) >= 1, '未查询到监控数据'
#        metric_info = [
#            {"metric_name": metric_name, "bucket": point['BucketName'], "value": int(point['Value']), "region": region,
#             "cloud": "aliyun"} for point in res_points]
        metric_info = []
        for point in res_points:
            if 'region' not in point or point.get('region') == 'cn-' + region:
                metric_info.append({
                    "metric_name": metric_name,
                    "bucket": point['BucketName'],
                    "value": int(point['Value']),
                    "region": region,
                    "cloud": "aliyun"
                })
        points = [dict(ChainMap(i, j)) for i in metric_info for j in ali_oss_cfg if
                  i['bucket'] == j['bucket']]
        return points


# 获取移动云存储容量
async def eos_storage(access_key, access_secret, region, endpoint, dc, bucket, session, resource_pool):
    date = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    headers = {
        "Authorization": "AWS " + access_key + ":" + eos_sign(access_secret=access_secret, bucket=bucket, date=date),
        "Date": date,
        "Host": endpoint
    }
    url = 'http://' + endpoint + '/' + bucket
    async with session.head(url, headers=headers) as resp:
        assert resp.status == 200, f'url: {url} \n headers: {headers} \n status {resp.status}'
        return {"metric_name": "MeteringStorageUtilization", "region": region, "dc": dc, "bucket": bucket,
                "value": resp.headers.get('X-RGW-Bytes-Used'), "cloud": "yidongyun"}


# 合营云云监控数据接口

async def get_ces_obs_data(access_key, access_secret, session, project_id, endpoint, metric_name, bucket, region_name,
                           dc_name):
    interval = 1800 if metric_name == 'capacity_standard' else 150
    params = {
        "namespace": "SYS.OBS",
        "metric_name": metric_name,
        "dim.0": f'bucket_name,{bucket}',
        "from": int((time.time() - interval) * 1000),
        "to": int(time.time() * 1000),
        "period": 1,
        "filter": "average"
    }
    sig = signer.Signer()
    sig.Key = access_key
    sig.Secret = access_secret
    headers = {"content-type": "application/json"}
    url = f'https://ces.{endpoint}/V1.0/{project_id}/metric-data'
    r = signer.HttpRequest('GET', url + '?' + urlencode(params))
    r.headers = headers
    sig.Sign(r)
    async with session.get(url, headers=r.headers, params=params) as response:
        data = await response.json()
        value = data['datapoints'][-1].get('average', 0) if 'datapoints' in data and len(data['datapoints']) > 0 else 0
        return {"metric_name": obs_metric_map[metric_name], "bucket": bucket,
                "value": value * 8 if metric_name == 'upload_bytes_extranet' else value, "cloud": "heyingyun", "region": region_name,
                "dc": dc_name}
