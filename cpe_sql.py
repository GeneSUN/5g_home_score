with snr_sn as (
select
distinct substr(rowkey,6,11) as sn,
ts,
try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.SNR') AS decimal) as __4gsnr,
try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.5GSNR') AS decimal) as __5gsnr

from "bhrdatabase"."bhrx_owlhistory_version_001" 
where date = '20250105'
and owl_data_fwa_cpe_data is not null
and CAST(json_extract(owl_data_fwa_cpe_data , '$.ModelName') AS varchar) in (
'XCI55AX', 'ASK-NCQ1338', 'ASK-NCQ1338FA', 'ASK-NCM1100', 'WNC-CR200A')
and (try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.SNR') AS decimal) <=40
and try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.SNR') AS decimal) >=-10
and try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.5GSNR') AS decimal) <=40
and try_cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.5GSNR') AS decimal) >=-10)),


intialbandwidth as (
select distinct ts, substr(rowkey,6,11) as sn,
case when CAST(json_extract(owl_data_fwa_cpe_data , '$.ModelName') AS varchar) in ('ASK-NCQ1338', 'ASK-NCQ1338FA') then 'ASK-NCQ1338' else CAST(json_extract(owl_data_fwa_cpe_data , '$.ModelName') AS varchar) end as model,
case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.4GPccBand') as bigint) > 0 then 20 else 0 end +
case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.4GScc1Band') as bigint) > 0 then 20 else 0 end +
case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.4GScc2Band') as bigint) > 0 then 20 else 0 end +
case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.4GScc3Band') as bigint) > 0 then 20 else 0 end as __lte_band,


case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GPccBand') as bigint) > 0
and cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.5GPccBand') as bigint) != 77 then 20 else 0 end +
case when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GScc1Band') as bigint) > 0
and cast(json_extract_scalar(owl_data_fwa_cpe_data, '$.5GScc1Band') as bigint) != 77 then 20 else 0 end as __nwbandwidth,

case 
when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GPccBand') as bigint) = 77
and cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GScc1Band') as bigint) = 77  then 160
when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GPccBand') as bigint) = 77
and try_cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GEARFCN_DL') as bigint) between 646667 and 653329 then 100 
when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GPccBand') as bigint) = 77
and try_cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GEARFCN_DL') as bigint) not between 646667 and 653329 then 60
when cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GPccBand') as bigint) != 77
and cast(json_extract_scalar(owl_data_fwa_cpe_data,'$.5GScc1Band') as bigint) = 77  then 80 else 0 end  as __cbandbandwidths


from "bhrdatabase"."bhrx_owlhistory_version_001" 
where date = '20250105'
and owl_data_fwa_cpe_data is not null
and CAST(json_extract(owl_data_fwa_cpe_data , '$.ModelName') AS varchar) in (
'XCI55AX', 'ASK-NCQ1338', 'ASK-NCQ1338FA', 'ASK-NCM1100', 'WNC-CR200A')),


bandwidth as (
select
sn,
ts,
model,
sum(__lte_band) as _lte_band, 
sum(__nwbandwidth) as _nwbandwidth,
sum(__cbandbandwidths) as _cbandbandwidths
from intialbandwidth
group by sn,ts, model),




fulltable as (
select
 bandwidth.sn,
 bandwidth.ts,
 bandwidth.model,
 bandwidth._lte_band as lte_band,
 bandwidth._nwbandwidth as nwbandwidth,
 bandwidth._cbandbandwidths as cbandbandwidths,
 snr_sn.__4gsnr as _4gsnr,
 snr_sn.__5gsnr as _5gsnr
 from bandwidth join snr_sn on bandwidth.sn = snr_sn.sn and bandwidth.ts = snr_sn.ts
),



capacities as (
select
sn,
model,
case when _4gsnr = 0 then 0 else lte_band * least(1, (try_cast(_4gsnr as decimal) + 11) / 41.0) end as lte_capacity,
case when _5gsnr = 0 then 0 else nwbandwidth * least(1, (try_cast(_5gsnr as decimal) + 11) / 41.0) end as nw_capacity,
case when _5gsnr = 0 then 0 else cbandbandwidths * 0.8 * least(1, (try_cast(_5gsnr as decimal) + 10) / 41.0) end as c_band_capacity 
from fulltable
where ((_4gsnr!=0 and lte_band>0) or (_5gsnr!=0 and (nwbandwidth+cbandbandwidths>0)))),


_capacity_percentage as (
select
sn,
model,
(lte_capacity + nw_capacity + c_band_capacity) / 218 as _capacity_percentage
from capacities
),

avg_capacity_percentage as (
select sn, model, avg(_capacity_percentage) as capacity_percentage
from _capacity_percentage
group by sn, model
)


select
model,
avg(capacity_percentage) as avg_capacity_percentage,
min(capacity_percentage) as min_capacity,
max(capacity_percentage) as max_capacity,
approx_percentile(capacity_percentage, 0.1) as percentile_1,
approx_percentile(capacity_percentage, 0.25) as percentile_25,
approx_percentile(capacity_percentage, 0.50) as percentile_50,
approx_percentile(capacity_percentage, 0.75) as percentile_75,
approx_percentile(capacity_percentage, 0.99) as percentile_99,
count(distinct sn) as sn_count
from avg_capacity_percentage
group by model