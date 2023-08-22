import aiohttp
import asyncio
from prometheus_client.core import GaugeMetricFamily
from aiohttp import TCPConnector
from oss_api import *
from utils import obs_buckets_info, obs_metrics, flatten, cfg

oss_metrics = ['MeteringStorageUtilization', 'InternetRecvBandwidth', 'PutObjectServerLatency']


async def async_get_aliyun_monitor_oss_data(session):
    tasks = [get_aliyun_monitor_oss_data(
        access_key=cfg['aliyun']['access_key'],
        access_secret=cfg['aliyun']['access_secret'],
        region=region['region_name'],
        endpoint=region['endpoint'],
        session=session,
        ali_oss_cfg=[{'bucket': bucket, 'dc': dc['dc_name']} for region in cfg['aliyun']['region'] for dc in
                     region['dc']
                     for bucket in dc['buckets']],
        dimensions=[{"BucketName": bucket, "storageType": "standard"} for dc in region['dc'] for bucket in
                    dc['buckets']],
        metric_name=metric,
    ) for region in cfg['aliyun']['region']
        for metric in oss_metrics]
    aliyun_oss_monitor_data = await asyncio.gather(*tasks)
    return aliyun_oss_monitor_data


async def async_get_eos_storage(session):
    tasks = [eos_storage(access_key=region['access_key'],
                         access_secret=region['access_secret'],
                         region=region['region_name'],
                         dc=dc['dc_name'],
                         bucket=bucket,
                         endpoint=region['endpoint'],
                         session=session,
                         resource_pool="hangyan")
             for region in cfg['yidongyun_hangyan'] for dc in
             region['dc'] for bucket in dc['buckets']]
    eos_storage_data = await asyncio.gather(*tasks)
    return eos_storage_data


async def async_get_obs_metric_data(session):
    tasks = [get_ces_obs_data(access_key=cfg['heyingyun']['access_key'],
                              access_secret=cfg['heyingyun']['access_secret'],
                              endpoint=region['endpoint'],
                              project_id=region['project_id'],
                              session=session,
                              metric_name=metric_name,
                              bucket=bucket_info['bucket_name'],
                              region_name=bucket_info['region'],
                              dc_name=bucket_info['dc'])
             for region in cfg['heyingyun']['region']
             for metric_name in obs_metrics
             for bucket_info in obs_buckets_info
             if bucket_info['region'] == region['region_name']]
    results = await asyncio.gather(*tasks)
    return results


class OssCollector:
    async def merge_metric_data(self):
        async with aiohttp.ClientSession(connector=TCPConnector(ssl=False)) as session:
            finished = await asyncio.gather(async_get_aliyun_monitor_oss_data(session),
                                            async_get_eos_storage(session), 
                                            async_get_obs_metric_data(session))
            return finished

    def collect(self):
        points = [x for x in flatten(asyncio.run(self.merge_metric_data()))]
        for metric in oss_metrics:
            gauge = GaugeMetricFamily(metric, '', labels=['bucket', 'oss_region', 'oss_dc', 'oss_cloud'])
            for point in points:
                if point.get('metric_name') == metric:
                    gauge.add_metric([point.get('bucket'), point.get('region'), point.get('dc'), point.get('cloud')],
                                     point.get('value'))
            yield gauge
