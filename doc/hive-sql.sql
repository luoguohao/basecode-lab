
 --查询最近五天新增的用户
 select udid ,channel,device_name ,create_time,create_date from device_info_day where datediff(to_date(from_unixtime(unix_timestamp())),create_date)<=5 and to_date(create_time) = create_date limit 1;

--获取昨天各个专辑收听人数、收听次数、收听时长
 select  a.radioid, b.name,count(distinct a.udid) as lisn_pers,count(a.udid) as lisn_times,sum(a.play_time) as play_times 
 from basis_event as a  join media_resources.tb_album as b on a.radioid = b.id
 where a.create_date='2015-03-30'
 and a.event_code='101010' group by a.radioid,b.name limit 10;

 --获取app版本为3.3.1中的各个页面中的播放人数、播放次数

  select a.page, count(distinct a.udid),count(a.udid)
 from basis_event  a left outer join basis_device_info_day b 
 on a.udid = b.udid and a.create_date=b.create_date and b.version = '3.3.1' 
 where a.create_date>='2015-02-24' and a.create_date<='2015-03-30' 
 and a.event_code in ('300009','300010','300024') group by a.page limit 5;

 --优化过后
 select b.page,count(distinct b.udid),count(b.udid)
 from ( 
select udid from basis_device_info_day where create_date>='2015-02-24' and create_date<='2015-03-30' and version = '3.3.1' group by udid) a 
 join ( 
select page,udid from basis_event where create_date>='2015-02-24' and create_date<='2015-03-30' and (event_code ='300009' or event_code='300010' or event_code='300024')) b
on a.udid = b.udid group by b.page ;

--查询app版本为3.3.1，活跃用户近一周每天点击关闭和启动自动收听的次数

select b.create_date,b.result,count(distinct b.udid)
 from ( 
select udid ,create_date from basis_device_info_day where create_date>='2015-03-24' and create_date<='2015-03-30' and version = '3.3.1') a 
 join ( 
select udid,result,create_date  from basis_event where create_date>='2015-03-24' and create_date<='2015-03-30' and event_code ='100004' and type='3') b
on a.udid = b.udid and a.create_date=b.create_date  group by b.create_date,b.result ;


--用户忠诚度分析： 近一周内，app版本为3.3.1,查询满足用户对该专辑收听次数达到两天以上的用户个数的专辑。

select a.name,count( distinct d.udid) from  media_resources.tb_album a join select c.radioid,c.udid from ( select udid ,create_date from basis_data.basis_device_info_day where create_date>='2015-03-24' and create_date<='2015-03-30' and version = '3.3.1') b join ( select radioid,udid,create_date from basis_data.basis_event where create_date>='2015-03-24' and create_date<='2015-03-30' and event_code ='101013') c on b.udid = c.udid and b.create_date = c.create_date group by c.radioid,c.udid having count(c.create_date)>=2 ) d on a.id = d.radioid group by a.name ,d.radioid ;


select a.name,d.ud_count from  media_resources.tb_album a join ( select c.radioid,count(distinct c.udid) as ud_count from (select udid from basis_device_info_day where create_date>='2015-03-24' and create_date<='2015-03-30' and version = '3.3.1' group by udid) b join ( select udid,radioid,count(distinct create_date) days from basis_event where create_date>='2015-03-24' and create_date<='2015-03-30' and event_code ='101013' group by udid,radioid) c on (b.udid = c.udid) where c.days>=2 group by c.radioid ) d on a.id = d.radioid;




--用户忠诚度分析： 近一周内，app版本为3.3.1,查询满足用户对该某个专辑收听次数达到两天以上的总用户数。
select count(distinct c.udid) as ud_count 
from (select udid from basis_device_info_day where create_date>='2015-03-24' 
and create_date<='2015-03-30' and version = '3.3.1' group by udid) b 
join (
select udid,count(distinct create_date) days 
from basis_event 
where create_date>='2015-03-24'
and create_date<='2015-03-30' 
and event_code ='101013' 
group by udid,radioid ) c 
on b.udid = c.udid where c.days>=2





--点播来源日报,每个专辑,每个点播来源占当天总点播次数的比率
 ---修改后，有效版本,
 ---专辑名称	点播总次数	推荐	全部	我的电台	离线	全部二级页	详情页

select e.name,e.sum_count,sum(e.page_01) as sum_page01,sum(e.page_02) as sum_page02,sum(e.page_03) as sum_page03,sum(e.page_04) as sum_page04,sum(e.page_05) as sum_page05,sum(e.page_06) as sum_page06
from (
select m.name,b.radioid,b.sum_count,
case when a.page = '200005' then  a.sum_count_page/b.sum_count else 0.0 end as page_01,
case when a.page = '200004' then a.sum_count_page/b.sum_count  else 0.0 end as page_02,
case when a.page = '200001' then a.sum_count_page/b.sum_count  else 0.0 end as page_03,
case when a.page = '200007' then a.sum_count_page/b.sum_count  else 0.0 end as page_04,
case when a.page = '200009' then a.sum_count_page/b.sum_count  else 0.0 end as page_05,
case when a.page = '200003' then a.sum_count_page/b.sum_count  else 0.0 end as page_06
from media_resources.tb_album as m join
(
select  radioid,page,count(udid) sum_count_page from basis_data.basis_event where create_date='2015-03-31' and (event_code ='300009' or event_code='300010' or event_code='300024') and radioid is not null group by radioid,page
) a join (
select  radioid,count(udid) sum_count from basis_data.basis_event where create_date='2015-03-31' and (event_code ='300009' or event_code='300010' or event_code='300024') and radioid is not null group by radioid
) b on (a.radioid = b.radioid and a.radioid = m.id and m.id=b.radioid)
union all 
select m.name,b.radioid,b.sum_count,
case when a.page = '200005' then  a.sum_count_page/b.sum_count else 0.0 end as page_01,
case when a.page = '200004' then a.sum_count_page/b.sum_count  else 0.0 end as page_02,
case when a.page = '200001' then a.sum_count_page/b.sum_count  else 0.0 end as page_03,
case when a.page = '200007' then a.sum_count_page/b.sum_count  else 0.0 end as page_04,
case when a.page = '200009' then a.sum_count_page/b.sum_count  else 0.0 end as page_05,
case when a.page = '200003' then a.sum_count_page/b.sum_count  else 0.0 end as page_06
from media_resources.tb_radio as m join
(
select  radioid,page,count(udid) sum_count_page from basis_data.basis_event where create_date='2015-03-31' and (event_code ='300009' or event_code='300010' or event_code='300024') and radioid is not null group by radioid,page
) a join (
select  radioid,count(udid) sum_count from basis_data.basis_event where create_date='2015-03-31' and (event_code ='300009' or event_code='300010' or event_code='300024') and radioid is not null group by radioid
) b on (a.radioid = b.radioid and a.radioid = m.id and m.id=b.radioid)
) e group by e.name,e.sum_count,e.radioid;


-- 计算昨天八点之后和19点之后的自有push查询
(版本3.1+) 八点之后
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-01' and hour>=8 and (event_code='101011' or event_code='300015');
(版本3.1+) 19点之后
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-01' and hour>=19 and (event_code='101011' or event_code='300015');
(版本3.1+) 2015-03-31 19点之后
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-03-31' and hour>=19 and (event_code='101011' or event_code='300015');
(版本3.1+) 2015-03-31 19点之前
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-03-31' and hour <19 and (event_code='101011' or event_code='300015');
3.0  2015-03-31 19点之后
SELECT count((case when eventcode='300040' then udid end)),count(distinct (case when eventcode='300040' then udid end)),count((case when eventcode='300041' then udid end)),count(distinct (case when eventcode='300041' then udid end)) FROM default.basis_common where createdate='2015-03-31' and hour >=19 and (eventcode='300040' or eventcode='300041');
3.0  2015-03-31  19点之前
SELECT count((case when eventcode='300040' then udid end)),count(distinct (case when eventcode='300040' then udid end)),count((case when eventcode='300041' then udid end)),count(distinct (case when eventcode='300041' then udid end)) FROM default.basis_common where createdate='2015-03-31' and hour <19 and (eventcode='300040' or eventcode='300041');


(版本3.1+) 昨天和今天的数据
SELECT create_date, count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-08' and (event_code='101011' or event_code='300015') group by create_date;

(版本3.1+) 2015-04-03 20点之后
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-03' and hour>=20 and (event_code='101011' or event_code='300015');
(版本3.1+) 2015-04-06 20点之后
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-06' and hour>=20 and (event_code='101011' or event_code='300015');


(版本3.1+) 2015-04-09 分设备查询
SELECT a.device_type ,count((case when b.event_code='101011' then a.udid end)),count(distinct (case when b.event_code='101011' then a.udid end)),
count((case when b.event_code='300015' then a.udid end)),count(distinct (case when b.event_code='300015' then a.udid end))
 FROM (select udid,device_type from basis_data.basis_device_info_day where create_date='2015-04-09' group by udid,device_type) a 
 join (select event_code,udid from basis_data.basis_event where create_date='2015-04-09' and (event_code='101011' or event_code='300015')) b on a.udid = b.udid group by a.device_type ;

(版本3.1+) 今天的数据
SELECT count((case when event_code='101011' then udid end)),count(distinct (case when event_code='101011' then udid end)),count((case when event_code='300015' then udid end)),count(distinct (case when event_code='300015' then udid end)) FROM basis_data.basis_event where create_date='2015-04-15' and (event_code='101011' or event_code='300015');
(版本3.0) 今天的数据
SELECT count((case when eventcode='300040' then udid end)),count(distinct (case when eventcode='300040' then udid end)),count((case when eventcode='300041' then udid end)),count(distinct (case when eventcode='300041' then udid end)) FROM default.basis_common where createdate='2015-04-13' and (eventcode='300040' or eventcode='300041');


(版本3.1+) 2015-04-09 分设备查询
SELECT a.device_type ,
count((case when b.event_code='101011' then a.udid end)),
count(distinct (case when b.event_code='101011' then a.udid end)),
count((case when b.event_code='300015' then a.udid end)),
count(distinct (case when b.event_code='300015' then a.udid end))
 FROM (select udid,device_type from basis_data.basis_device_info_day where create_date='2015-04-08' group by udid,device_type) a 
 join (select event_code,udid from basis_data.basis_event where create_date='2015-04-08' and (event_code='101011' or event_code='300015')) b 
 on (a.udid = b.udid) group by a.device_type ;
			    101011推送—接收				   300015推送—点击 
 android       次数=115133     人数=111359  	5701    5089
 ios           次数=75         人数=73      	243     228
				115312 							5957


--3月23到3月29每天的卡顿人数、次数、所有错误数据量（排除误报(闪屏错误)的错误数据），并且以IOS,安卓系统区分


select f.device_type,f.create_date,dis_udid_count,udid_count,g.numbs
from (
select m.create_date,m.device_type,count(m.udid) as numbs
from (select b.create_date,case when d.producer='apple' then 'IOS' else 'android' end as device_type,b.udid from basis_data.basis_device_info_day d join basis_data.basis_error b on d.udid=b.udid where b.create_date>='2015-03-23' and b.create_date<='2015-03-29' and d.create_date>='2015-03-23' and d.create_date<='2015-03-29' and b.event_code <> '100011') m group by m.create_date,m.device_type 
) g right join (
select h.device_type,h.create_date,count(distinct h.udid) as dis_udid_count,count(h.udid) as udid_count
from (
select 
case when b.producer='apple' then 'IOS' else 'android' end as device_type,a.udid,a.create_date 
from  basis_data.basis_device_info_day b join basis_data.basis_event a
on a.udid = b.udid and a.create_date = b.create_date
where a.create_date>='2015-03-23' and a.create_date<='2015-03-29' and  b.create_date>='2015-03-23' and b.create_date<='2015-03-29'and a.event_code='300030'
) h group by h.device_type,h.create_date) f 
on (f.create_date = g.create_date and f.device_type = g.device_type) 



select b.create_date,d.device_type,
count( distinct case when b.event_code='300030' then b.udid end ) as  dis_udid_count,
count( case when b.event_code='300030' then b.udid end ) as  udid_count,
count( case when (b.event_code !='300030' and b.event_code != '100011') then b.udid end ) as  numbs
from (select distinct udid,case when producer='apple' then 'IOS' else 'android' end as device_type from basis_data.basis_device_info_day where create_date>='2015-03-28' and create_date<='2015-03-29') d 
join (
select udid,event_code,create_date from basis_data.basis_error where create_date>='2015-03-28' and create_date<='2015-03-29' and event_code != '100011'
union all 
select udid,event_code,create_date from basis_data.basis_event where create_date>='2015-03-28' and create_date<='2015-03-29' and event_code='300030'
) b on d.udid=b.udid group by b.create_date,d.device_type



时间	系统	卡顿人数	卡杜按次数	错误数据量
2015-03-23      IOS     2650    6758    65
2015-03-23      android 19274   180022  2195
2015-03-24      IOS     2929    7861    61
2015-03-24      android 20433   197451  5699
2015-03-25      IOS     2861    7774    55
2015-03-25      android 20558   197736  2814
2015-03-26      IOS     3078    7989    52
2015-03-26      android 21871   203869  4407
2015-03-27      IOS     2782    7532    51
2015-03-27      android 21745   214117  1937
2015-03-28      IOS     2633    6715    56
2015-03-28      android 21849   208566  2356
2015-03-29      IOS     2628    6549    60
2015-03-29      android 22411   209530  1652



--人均关注次数对比

select a.create_date,count(distinct a.udid),count(a.udid)
from 
( select udid from  kaola_stat.user_info_day  where create_date>='2015-03-27' and create_date<='2015-04-02' and version='3.3.1' group by udid) b
join  (select udid,create_date from basis_data.basis_event where  create_date>='2015-03-27' and create_date<='2015-04-02' and event_code='400005') a on a.udid = b.udid  group by a.create_date

版本为3.3.1，人均关注次数对比：
2015-03-27      8254    23929
2015-03-28      8783    27975
2015-03-29      9132    28705
2015-03-30      9833    32119
2015-03-31      9957    30885
2015-04-01      9457    28173
2015-04-02      8932    25451

版本为3.2.1，人均关注次数对比：
select a.create_date,count(distinct a.udid),count(a.udid)
from 
( select udid from  kaola_stat.user_info_day  where create_date>='2015-03-06' and create_date<='2015-03-12' and version='3.2.1' group by udid) b
join  (select udid,create_date from basis_data.basis_event where  create_date>='2015-03-06' and create_date<='2015-03-12' and event_code='400005') a on a.udid = b.udid  group by a.create_date




--人均离线次数对比

select a.create_date,count(distinct a.udid),count(a.udid)
from 
( select udid from  kaola_stat.user_info_day  where create_date>='2015-03-27' and create_date<='2015-04-02' and version='3.3.1' group by udid) b
join  (select udid,create_date from basis_data.basis_event where  create_date>='2015-03-27' and create_date<='2015-04-02' and event_code='300006') a on a.udid = b.udid  group by a.create_date

版本为3.3.1，人均离线次数对比：
2015-03-27      8473    191431
2015-03-28      8775    193928
2015-03-29      9142    213504
2015-03-30      9565    202893
2015-03-31      10092   217507
2015-04-01      9680    213087
2015-04-02      9336    201461

select a.create_date,count(distinct a.udid),count(a.udid)
from 
( select udid from  kaola_stat.user_info_day  where create_date>='2015-03-06' and create_date<='2015-03-12' and version='3.2.1' group by udid) b
join  (select udid,create_date from basis_data.basis_event where  create_date>='2015-03-06' and create_date<='2015-03-12'and event_code='300006') a on a.udid = b.udid  
group by a.create_date

版本为3.2.1，人均离线次数对比：

2015-03-06      8430    171642
2015-03-07      9476    193105
2015-03-08      9821    202682
2015-03-09      9077    192656
2015-03-10      9329    191619
2015-03-11      9307    191706
2015-03-12      9929    215278


--直播统计 版本3.3.1，时间2015-03-27~2015-04-02
--时间	直播收听人数	直播收听时长	直播列表页面浏览次数	全部页面（大全）浏览次数	直播列表浏览人数	预订次数  预订人数  预订收听次数  点赞次数	分享次数	

--修改后
 select b.create_date,
                    count(distinct case
                            when b.event_code = '101015' then
                             b.udid
                          end) as online_person,
                    sum(case
                          when b.event_code = '101015' then
                           b.play_time
                        end) as online_times,
                    count(case
                            when b.event_code = '200011' then
                             b.udid
                          end) as online_list_time,
                    count(case
                            when b.event_code = '200009' then
                             b.udid
                          end) as all_page_time,
                    count(distinct case
                            when b.event_code = '200011' then
                             b.udid
                          end) as online_list_per,
                    --预订次数  预订人数  预订收听次数 
                    count(case
                            when b.event_code = '400008' then
                             b.udid
                          end) as book_time,
                    count(distinct case
                            when b.event_code = '400008' then
                             b.udid
                          end) as book_user_per,
                    count(case
                            when b.event_code = '400007' and
                                 b.radioid >= '1400000000' and
                                 b.radioid <= '9999999999' then
                             b.udid
                          end) as praise_time,
                    count(case
                            when b.event_code = '300008' and b.eventid != 'null' then
                             b.udid
                          end) as share_time
             
               from (select udid,create_date
                       from basis_data.basis_device_info_day
                      where create_date >= '2015-04-11'
                        and create_date <= '2015-04-16'
                        and version = '3.3.1'
                      group by udid,create_date) a
               join (select create_date,
                           udid,
                           event_code,
                           play_time,
                           radioid,
                           eventid
                      from basis_data.basis_event
                     where create_date >= '2015-04-11'
                       and create_date <= '2015-04-16') b on (a.udid =b.udid and a.create_date=b.create_date)
              where (b.event_code = '200011' or b.event_code = '200009' or
                    b.event_code = '101015' or b.event_code = '400007' or
                    b.event_code = '300008' or b.event_code = '400008')
              group by b.create_date

---预订收听次数 单独计算 
select count(tmp_c.udid) as book_play_cnt, tmp_c.create_date
  from (select tmp_b.* from (select udid,create_date
          from basis_data.basis_device_info_day
         where create_date >= '2015-04-11'
           and create_date <= '2015-04-16'
           and version = '3.3.1') tmp_a
  join (select create_date, udid, audioid, event_code
          from basis_data.basis_event
         where create_date >= '2015-04-11'
           and create_date <= '2015-04-16'
           and (event_code = '400008')) tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date=tmp_b.create_date)) tmp_c
  left semi join (select create_date, udid, audioid, event_code
          from basis_data.basis_event
         where create_date >= '2015-04-11'
           and create_date <= '2015-04-16'
           and (event_code = '101015')) tmp_d on (
                                                 tmp_d.udid = tmp_c.udid and
                                                 tmp_d.audioid = tmp_c.audioid and tmp_d.create_date= tmp_c.create_date)
 group by tmp_c.create_date
 
 
2015-03-27      4781    2933660 7819    79687   4900    183     0
2015-03-28      1362    1579560 6815    83815   4695    84      0
2015-03-29      1177    1102445 6910    89076   4832    70      0
2015-03-30      4601    2183325 8648    97026   5836    130     0
2015-03-31      4442    2813305 8696    98925   6022    118     0
2015-04-01      4716    2953985 8342    93279   5633    193     0
2015-04-02      5926    3669100 8014    91581   5375    181     0


--各个版本中，2015-03-27,2015-04-02,2015-04-03的banner的点击数和全部页的浏览数
--版本3.1+
select create_date,count( case when event_code='300016' then udid end) as banner_count,count( case when event_code='200004' then udid end) as allpage_count
from basis_data.basis_event  
where (create_date='2015-03-27' or create_date='2015-04-02' or create_date='2015-04-03')
and (event_code='300016' or event_code='200004') group by create_date
2015-03-27      6163    275625
2015-04-02      6973    307941
2015-04-03      8321    295695

--每个专辑（3.1-3.31）每天的收听次数，收听人数，收听时长，关注人数，完整收听率

select stat_date,album_name,play_count,active_play_user_count,play_time,follow_user_count,max_play_time/max_program_length
from data_st.analysis_of_album_stat_client where stat_date>='2015-03-01' and stat_date <='2015-03-31' and device_type ='-1' and album_id !='-1'


--每天、每个系统（安卓,ios）,每个版本的事件统计人数，次数
create external table if not exists kaola_tj.event_stat_day (
launch_cnt int,
launch_user_cnt int,
splash_cnt int ,
splash_user_cnt int,
play_cnt int ,
play_user_cnt int,
push_cnt int ,
push_user_cnt int ,
push_click_cnt int ,
push_click_user_cnt int,
error_cnt int ,
error_user_cnt int ,
stop_cnt int ,
stop_user_cnt int,
device_type int , 
version string
)
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://ks01:8020/hive/kaola_tj/event_stat_day';



--数据导入(device_type 0:android；1：ios)
insert overwrite table kaola_tj.event_stat_day partition(create_date='2015-04-03')
select sum( case when b.event='100010' then b.event_cnt else 0 end),
count(distinct case when b.event='100010' then b.udid end),
sum(case when (a.version>='3.1' and b.event='100011') or (a.version<'3.1' and a.version>='3.0' and (b.event='201000' or b.event='201001')) then b.event_cnt else 0 end),
count(distinct case when (a.version>='3.1' and b.event='100011') or (a.version<'3.1' and a.version>='3.0' and (b.event='201000' or b.event='201001')) then b.udid  end),
sum( case when b.event='101010' then b.event_cnt else 0 end),
count(distinct case when b.event='101010' then b.udid end),
sum( case when (a.version>='3.1' and b.event='101011') or (a.version<'3.1' and a.version>='3.0' and b.event='300040') then b.event_cnt else 0 end),
count(distinct case when (a.version>='3.1' and b.event='101011') or (a.version<'3.1' and a.version>='3.0' and b.event='300040') then b.udid end),
sum( case when (a.version>='3.1' and b.event='300015') or (a.version<'3.1' and a.version>='3.0' and b.event='300041') then b.event_cnt else 0 end),
count(distinct case when (a.version>='3.1' and b.event='300015') or (a.version<'3.1' and a.version>='3.0' and b.event='300041') then b.udid end),
sum( case when (a.version<'3.1' and a.version>='3.0' and b.event='999999') then b.event_cnt else 0 end),
count(distinct case when (a.version<'3.1' and a.version>='3.0' and b.event='999999') then b.udid end),
sum( case when (a.version>='3.1' and b.event='300022') then b.event_cnt else 0 end),
count(distinct case when (a.version>='3.1' and b.event='300022') then b.udid end),
a.device_type,a.version 
from   (select udid,version,device_type from kaola_stat.user_info_day where create_date='2015-04-03' group by udid,version,device_type )a 
join (select udid ,event,event_cnt ,create_date from kaola_stat.user_event_day where  create_date='2015-04-03' and (event ='100010' or event ='100011' or event ='101010' or event ='101011' or event ='300015' or event ='300022' or  event ='300030' or event='201000' or event='201001' or event='300040' or event='300041' or event='990000' or event='990001' or event='999999')) b on (a.udid = b.udid)  
group by a.device_type,a.version


--对于版本3.1+的错误数据单独导入
insert overwrite table kaola_tj.event_stat_day partition(create_date='2015-04-03')
select a.launch_cnt,
a.launch_user_cnt,
a.splash_cnt,
       a.splash_user_cnt,
       a.play_cnt,
       a.play_user_cnt,
       a.push_cnt,
       a.push_user_cnt,
       a.push_click_cnt,
       a.push_click_user_cnt,
       nvl(b.b_error_cnt,a.error_cnt),
       nvl(b.b_error_user_cnt,a.error_user_cnt),
       a.stop_cnt,
       a.stop_user_cnt,
       a.device_type,
       a.version
  from (select *
          from kaola_tj.event_stat_day
         where create_date = '2015-04-03') a
  left join (select c.create_date,
               c.version,
               c.device_type,
               count(d.udid) as b_error_cnt,
               count(distinct d.udid) as b_error_user_cnt
          from (select create_date, udid, version, device_type
                  from basis_data.basis_device_info_day
                 where create_date = '2015-04-03'
                 group by udid, version, device_type, create_date) c
          join (select udid
                 from basis_data.basis_error
                where create_date = '2015-04-03') d on c.udid = d.udid
         group by c.create_date, c.version, c.device_type) b on a.create_date =
                                                                b.create_date
                                                            and a.version =
                                                                b.version
                                                            and a.device_type =
                                                                b.device_type


--每个月每天的所有专辑完整收听率

select stat_date,max_play_time/max_program_length  
from data_st.analysis_of_album_stat_client  
where stat_date like '2015-02%' and device_type=-1 and album_id='-1' and album_name='all'


--渠道  appstore，A-baidu
--版本  全部，3.1及以上
--指标：MAU（月活跃用户数）
--时间：三月

select a.channel,case when a.version>='3.1' then '3.1+' else '3.1以下' end,count(distinct a.udid)
from  (select udid ,channel,version from kaola_stat.user_info_day where create_date like '2015-03%'  and (channel='appstore' or channel='A-baidu') group by udid ,channel,version) a 
join (select distinct udid from kaola_stat.user_stat_day where create_date like '2015-03%' and appid='10001' and user_active=1) b 
on (a.udid = b. udid) group by a.channel,case when a.version>='3.1' then '3.1+' else '3.1以下' end;

--渠道小米 ，每个版本
select a.channel,a.version,count(distinct a.udid)
from  (select udid ,channel,version from kaola_stat.user_info_day where create_date like '2015-03%'  and (channel='B-xiaomi') group by udid ,channel,version) a 
join (select distinct udid from kaola_stat.user_stat_day where create_date like '2015-03%' and appid='10001' and user_active=1) b 
on (a.udid = b. udid) group by a.channel,a.version;


--提取 1、2月份 全渠道按机型统计数据
--时间	渠道号	机型	NU	Play NU	30日留存数（按收听）

select a.create_date,a.channel,a.device_name,count(distinct case when b.user_new=1 then a.udid end ),count(distinct case when b.user_new_play=1 then a.udid end),count(distinct case when b.remain_30_days_play=1 then a.udid end)
from  (select udid,channel,device_name,create_date from kaola_stat.user_info_day where create_date like '2015-02%' group by udid ,channel,device_name,create_date) a 
join (select udid,create_date ,user_new,user_new_play,remain_30_days_play  from kaola_stat.user_stat_day where create_date like '2015-02%' and appid='10001' and (user_new=1 or user_new_play=1 or remain_30_days_play=1)) b 
on (a.udid = b. udid and a.create_date=b.create_date) group by a.channel,a.device_name,a.create_date;

5、6两个月的机型NU数据，需求字段如下：日期，机型，渠道号，全部NU，全部次日留存数
select a.create_date,a.device_name,a.channel,count(distinct case when b.user_new=1 then a.udid end ),count(distinct case when b.remain_1_days=1 then a.udid end)
from  (select udid,channel,device_name,create_date from kaola_stat.user_info_day where create_date like '2015-06%' group by udid ,channel,device_name,create_date) a 
join (select udid,create_date ,user_new,remain_1_days  from kaola_stat.user_stat_day where create_date like '2015-06%' and appid='10001' and (user_new=1 or remain_1_days=1)) b 
on (a.udid = b. udid and a.create_date=b.create_date) group by a.channel,a.device_name,a.create_date;

--2015/3/1到2015/3/31
select a.create_date,a.channel,a.device_name,count(distinct case when b.user_new=1 then a.udid end ),count(distinct case when b.user_new_play=1 then a.udid end),count(distinct case when b.remain_30_days_play=1 then a.udid end) from  (select udid,channel,device_name,create_date from kaola_stat.user_info_day where create_date like '2015-03%' group by udid ,channel,device_name,create_date) a join (select udid,create_date ,user_new,user_new_play,remain_30_days_play  from kaola_stat.user_stat_day where create_date like '2015-03%' and appid='10001' and (user_new=1 or user_new_play=1 or remain_30_days_play=1)) b on (a.udid = b. udid and a.create_date=b.create_date) group by a.channel,a.device_name,a.create_date;


--修改tb_push表结构
CREATE  TABLE tb_push(
  id int, 
  content_id bigint, 
  effective_begin_time string, 
  effective_end_time string, 
  push_channel string, 
  push_type int, 
  send_form int, 
  status int, 
  title string, 
  url string, 
  content_type int, 
  msg_type string, 
  createby string, 
  create_time bigint, 
  create_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/media_resources/tb_push'
TBLPROPERTIES (
  'transient_lastDdlTime'='1423644129')
  
  
 CREATE  TABLE media_resources.tb_push(
  id int, 
  content_id bigint, 
  effective_begin_time string, 
  effective_end_time string, 
  push_channel string, 
  push_type int, 
  send_form int, 
  status int, 
  title string, 
  url string, 
  content_type int, 
  msg_type string, 
  createby string, 
  create_time bigint, 
  create_date string,
  channels  string,
  creater_name  string ,
   platform  string ,
   versions  string ,
   channels_android  string ,
   channels_ios string,
   description_android  string ,
   description_ios  string ,
   update_date  bigint,
   update_name  string ,
   version_android  string ,
   version_ios  string ,
   carrieroperator  string ,
   udid_url  string ,
   effective_date  int ,
   channels_arr_wp  string ,
   version_arr_wp  string ,
   description_wp  string ,
   product_line  int 
  )
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
LOCATION
  'hdfs://ks01:8020/hive/media_resources/tb_push'
  
  
  
  --查询每天的push信息：
--日期：当天的日期；
--ID：CMS中当条PUSH的ID；
--标题：CMS中当条PUSH的名称；
--链接内容：内容资源或页面webview；
--推送时间：取PUSH列表中的推送时间；
--接收人数：PUSH被接收的次数；
--xxxx接收转化率：接收人数/当日可推送用户数(心跳用户数)；xxxx 删除
--点击人数：PUSH被点击的人数；
--点击转化率：点击人数/接收人数；
--收听人数：通过PUSH产生收听行为的人数；		删除
--收听转化率：收听人数/点击人数； 				删除
--推送平台：CMS推送设置的平台，Android、iOS或WP；
--推送方式：CMS推送设置的推送方式，广播、指定渠道版本（移动、电信或联通）、指定UDID；
--推送形式：CMS中推送设置的推送形式，非强制推送或强制推送。

select '当前日期', a.id,a.title,(case when a.push_type='1' then '内容资源' when a.push_type='2' then 'webview（APP自有浏览器）'when a.push_type='8' then 
'webview(外部浏览器)' end),from_unixtime(cast(a.effective_begin_time/1000 as bigint)),
b.receive_user_cnt,b.click_user_cnt,b.click_user_cnt/b.receive_user_cnt,(case when instr(a.platform,'0')>0 and instr(a.platform,'1') >0 
then 'IOS、Android' when instr(a.platform,'0')>0 then 'Android' when instr(a.platform,'1')>0 then 'IOS' end),(case when a.msg_type='0' 
then '广播' when a.msg_type='1' then '指定UDID' when a.msg_type='2' then '指定渠道版本' end),( case when a.send_form='1' then '及时推送' 
when a.send_form='2' then '定时推送' when a.send_form='3' then '强制定时' end) from (  select * from media_resources.tb_push) a 
join( SELECT eventid,count((case when event_code='101011' then udid end))as receive_cnt,
count(distinct(case when event_code='101011'then udid end))as receive_user_cnt,
count((case when event_code='300015'then udid end))as click_cnt,
count(distinct(case when event_code='300015' then udid end))as click_user_cnt 
FROM basis_data.basis_event where create_date='2015-04-10' and(event_code='101011' or event_code='300015')group by eventid) b on(a.id=b.eventid)


--闪屏数据

--日期：当天的日期；
--标题：CMS中当条闪屏的名称；
--链接内容：CMS中部署的内容：无、内容资源、页面webview
--接收次数：闪屏被接收的次数；
--接收人数：闪屏被接收的人数；
--接收转化率：接收次数/启动次数；
--点击次数：闪屏被点击的次数；
--点击人数：闪屏被点击的人数；
--点击转化率：点击次数/接收次数；
--平台：CMS部署的平台，Android、iOS或WP；
--生效时间：CMS部署的生效时间；
--显示时长：CMS部署的显示时长。

创建表：
CREATE TABLE media_resources.tb_splash_screen (
  id int  ,
  name string  ,
  show_time_length int  ,
  remark string  ,
  platform int   ,
  version_type int  ,
  version_id int  ,
  channel_type int  ,
  channel_id int  ,
  splash_screen_img_id int  ,
  link_type int  ,
  link_address string  ,
  valid_start_date string  ,
  valid_end_date string  ,
  status int  ,
  createby int  ,
  creater_name string  ,
  create_date string  ,
  create_time bigint  ,
  link_ype int,
  prod_line string  )
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LOCATION 'hdfs://ks01:8020/hive/media_resources/tb_splash_screen'
  
  从mysql中导入数据：
/usr/local/hadoop/bin/hadoop fs -rm -r /hive/media_resources/tb_splash_screen

/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://10.10.4.34/media_resources --username dev --password dev@2014 --query "SELECT id,name,show_time_length,remark,platform,version_type,version_id,channel_type,channel_id,splash_screen_img_id,link_type,link_address,unix_timestamp(valid_start_date),unix_timestamp(valid_end_date),status,createby,creater_name,date_format(create_date,'%Y-%m-%d'), unix_timestamp(create_date),link_ype, prod_line FROM tb_splash_screen WHERE \$CONDITIONS" --split-by id --fields-terminated-by '\t' --append --target-dir /hive/media_resources/tb_splash_screen



--专题表

CREATE TABLE media_resources.tb_feature (
  id string,
  name string,
  content_num int,
  type_id string,
  content_type int,
  status int
 )
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LOCATION 'hdfs://ks01:8020/hive/media_resources/tb_feature'

 从mysql中导入数据：
/usr/local/hadoop/bin/hadoop fs -rm -r /hive/media_resources/tb_feature

/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://10.10.4.34/media_resources --username dev --password dev@2014 --query "SELECT id,name,content_num,type_id,content_type,status FROM tb_feature WHERE \$CONDITIONS" --split-by id --fields-terminated-by '\t' --append --target-dir /hive/media_resources/tb_feature


--查询闪屏数据
select '当天时间', a.name, case when a.link_type=0 then '无' when a.link_type=1 then '内容资源' when a.link_type=2 then '页面webview' 
end ,b.splash_cnt,         b.splash_user_cnt,         b.splash_cnt / b.launch_cnt,         b.splash_click_cnt,        
 b.splash_click_user_cnt,         b.splash_click_cnt / b.splash_cnt,     
 (case           when instr(a.platform, '0') > 0 and instr(a.platform, '1') > 0 then            'IOS、Android'          
 when instr(a.platform, '0') >=0 then            'Android'           when instr(a.platform, '1') >0 
 then            'IOS'         end),         a.valid_start_date,         show_time_length    
 from (select * from media_resources.tb_splash_screen) a   
 left join 
 (select temp_a.eventid,                      temp_b.launch_cnt,                      temp_a.splash_cnt,                 
 temp_a.splash_user_cnt,                      temp_a.splash_click_cnt,                   
 temp_a.splash_click_user_cnt                 from (select count(udid) as launch_cnt        
 from basis_data.basis_event                      
 where create_date = '2015-04-10'                   
 and event_code = '100010') temp_b                
 join (SELECT eventid,                           
 count((case                                 
 when event_code = '101011' then            
 udid                                   end)) as splash_cnt,       
 count(distinct(case                                         
 when event_code = '101011' then                                 
 udid                                            end)) as splash_user_cnt,    
 count((case                                     when event_code = '300015' then         
 udid                                   end)) as splash_click_cnt,                  
 count(distinct(case                                              when event_code = '300015' then                 
 udid                                            end)) as splash_click_user_cnt                     
 FROM basis_data.basis_event                       where create_date = '2015-04-10'              
 and (event_code = '101011' or event_code = '300015')                       group by eventid) temp_a) b on (a.id = b.eventid)  


--导入banner数据

--创建表：
CREATE TABLE media_resources.tb_banner (
   id  int,
   name  string,
   img  string,
   type  int,
   data_id  bigint,
   data_type  int,
   content_desc  string,
   web_url  string,
   status  int,
   orderset  int ,
   createby  int ,
   create_time  bigint,
   create_date string,
   creater_name  string,
   updateby  int ,
   update_date  bigint,
   updater_name  string,
   product_line  int ,
   image_pixel  string
) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LOCATION 'hdfs://ks01:8020/hive/media_resources/tb_banner';
  
 --导入表
/usr/local/hadoop/bin/hadoop fs -rm -r /hive/media_resources/tb_banner

/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://10.10.4.34/media_resources --username dev --password dev@2014 --query "SELECT id,name,img,type,data_id,data_type,content_desc,web_url,status,orderset,createby,unix_timestamp(create_date),date_format(create_date,'%Y-%m-%d') ,creater_name,updateby,unix_timestamp(update_date),updater_name, product_line,image_pixel FROM tb_banner WHERE \$CONDITIONS" --split-by id --fields-terminated-by '\t' --append --target-dir /hive/media_resources/tb_banner



--4月9号到4月15日，各个地区(按省)的人的收听大类分布 收听人数，收听次数，收听时长

select temp_c.create_date,a.areaname,temp_c.catalog_name,temp_c.play_user_cnt,temp_c.play_cnt,temp_c.play_time
from  (select areaname,areacode from default.code_area where parentareacode=156)  a left join (
select temp_a.create_date,min(temp_a.area_code) as area_code,max(temp_b.catalog_name) as catalog_name,count(distinct temp_b.udid) as play_user_cnt,count(temp_b.udid) as play_cnt ,sum(temp_b.play_time) as play_time 
from 
(select udid,create_date,area_code from kaola_stat.user_info_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_a 
left join (select udid,catalog_id,catalog_name,create_date,play_time from kaola_stat.user_play_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_b
on temp_a.udid = temp_b.udid and temp_a.create_date = temp_b.create_date group by substr(temp_a.area_code,0,2),temp_b.catalog_id,temp_a.create_date
) temp_c on a.areacode = temp_c.area_code


----4月9号到4月15日，各个地区收听人数最多的TOP30专辑的 收听人数

select temp_d.create_date,temp_d.areaname,temp_d.album_name,temp_d.play_user_cnt from (
select temp_c.create_date,a.areaname,temp_c.album_name,temp_c.play_user_cnt,dense_rank() over(partition by temp_c.create_date,a.areaname order by temp_c.play_user_cnt desc) as rank_id
from  (select areaname,areacode from default.code_area where parentareacode=156)  a left join (
select temp_a.create_date,min(temp_a.area_code) as area_code,max(temp_b.album_name) as album_name,count(distinct temp_b.udid) as play_user_cnt
from (select udid,create_date,area_code from kaola_stat.user_info_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_a 
left join (select udid,album_id,album_name,create_date from kaola_stat.user_play_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date = temp_b.create_date)
group by substr(temp_a.area_code,0,2),temp_b.album_id,temp_a.create_date
) temp_c on a.areacode = temp_c.area_code ) temp_d where temp_d.rank_id<=30

----4月9号到4月15日，各个地区收听人数最多的TOP30专辑的 收听时长
select temp_d.create_date,temp_d.areaname,temp_d.album_name,temp_d.play_times from (
select temp_c.create_date,a.areaname,temp_c.album_name,temp_c.play_times,dense_rank() over(partition by temp_c.create_date,a.areaname order by temp_c.play_times desc) as rank_id
from  (select areaname,areacode from default.code_area where parentareacode=156)  a left join (
select temp_a.create_date,min(temp_a.area_code) as area_code,max(temp_b.album_name) as album_name,sum(temp_b.play_time) as play_times
from (select udid,create_date,area_code from kaola_stat.user_info_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_a 
left join (select udid,album_id,album_name,create_date,play_time from kaola_stat.user_play_day where create_date>='2015-04-09' and create_date<='2015-04-15') temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date = temp_b.create_date)
group by substr(temp_a.area_code,0,2),temp_b.album_id,temp_a.create_date
) temp_c on a.areacode = temp_c.area_code ) temp_d where temp_d.rank_id<=30

--维度：天、渠道 指标：事件数小于5的人数，事件数小于10的人数，总人数

select *  from (select t_a.channel,count(distinct case when t_b.event_cnt<=5 then t_b.udid end ) as less_5_user ,
count(distinct case when t_b.event_cnt<=10 and t_b.event_cnt>5 then t_b.udid end ) as less_10_user ,count(distinct t_b.udid) as total_user 
from (select udid,channel from kaola_stat.user_info_day where create_date='2015-04-16') t_a join 
(select udid ,event_cnt from kaola_stat.warn_user_day where create_date='2015-04-16') t_b on (t_a.udid=t_b.udid) group by t_a.channel) t_c  
order by t_c.less_5_user ,t_c.less_10_user desc 

时间段
 select *  from (select t_a.channel,t_a.create_date,count(distinct case when t_b.event_cnt<=5 then t_b.udid end ) as less_5_user ,
count(distinct case when t_b.event_cnt<=10 and t_b.event_cnt>5 then t_b.udid end ) as less_10_user ,count(distinct t_b.udid) as total_user 
from (select udid,channel,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12') t_a join 
(select udid ,event_cnt,create_date from kaola_stat.warn_user_day where create_date>='2015-04-06' and create_date<='2015-04-12') t_b on (t_a.udid=t_b.udid and t_a.create_date=t_b.create_date) group by t_a.channel,t_a.create_date ) t_c  
order by t_c.less_5_user ,t_c.less_10_user desc 



--近7天收听人数分布数据（2015-04-12~2015-04-19） 使用hive_kaola_album_hour.sh执行查询
时间   	 0:00 1:00 2:00 ..... 23:00
专辑名称：

create external table if not exists tmp_stat.temp_album_hour (
name string,
version string,
h0_cnt int,
h1_cnt int ,
h2_cnt int ,
h3_cnt int ,
h4_cnt int ,
h5_cnt int ,
h6_cnt int ,
h7_cnt int ,
h8_cnt int ,
h9_cnt int ,
h10_cnt int ,
h11_cnt int ,
h12_cnt int ,
h13_cnt int ,
h14_cnt int ,
h15_cnt int ,
h16_cnt int ,
h17_cnt int ,
h18_cnt int ,
h19_cnt int ,
h20_cnt int ,
h21_cnt int ,
h22_cnt int ,
h23_cnt int 
)
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://ks01:8020/hive/tmp_stat/temp_album_hour';



--专辑来源是爱听(考拉主播)的专辑统计信息(搜听次数，收听时长，playAU ,Play NU ,人均收听专辑时长，完整收听率)
select temp_a.name,temp_b.stat_date,temp_b.play_count,temp_b.play_time,temp_b.active_play_user_count,temp_b.new_play_user_count,temp_b.time_per_user,temp_b.listen_ratio 
from (select id,name from media_resources.tb_album where source=2 ) temp_a left join
(select album_id,stat_date,play_count,play_time,active_play_user_count,new_play_user_count,(play_time/active_play_user_count) as time_per_user ,(max_play_time/max_program_length) as  listen_ratio 
from data_st.analysis_of_album_stat_client where stat_date>='2015-04-13' and stat_date<='2015-04-19' and device_type=-2)  temp_b on (temp_a.id=temp_b.album_id)



select * from data_st.analysis_of_album_stat_client where album_name='开车实用小单元' and stat_date='2015-04-13' limit 3;


--专辑统计信息3.18~4.18(分类，收听次数，收听人数，收听时长，关注人数，完整收听率)

select stat_date,album_name,catalog_name,play_count,active_play_user_count,play_time,follow_user_count,(max_play_time/max_program_length) as  listen_ratio 
from data_st.analysis_of_album_stat_client where stat_date>='2015-03-18' and stat_date<='2015-04-18' and device_type=-2


--3.4.1版本用户的启动次数和浏览全部页的次数

select temp_a.create_date,sum(case when temp_b.event='100010' then temp_b.event_cnt end ),sum(case when temp_b.event='200004' then temp_b.event_cnt end )
 from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-14' and create_date<='2015-04-20' and version='3.4.1' group by udid,create_date) temp_a 
 join (select udid,create_date,event,event_cnt from kaola_stat.user_event_day where create_date>='2015-04-14' and create_date<='2015-04-20' and (event='100010' or event='200004')) temp_b
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date
 --推荐页，全部页
 select temp_a.create_date,sum(case when temp_b.event='200005' then temp_b.event_cnt end ),sum(case when temp_b.event='200004' then temp_b.event_cnt end )
 from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-07' and create_date<='2015-04-11' and version='3.3.1' group by udid,create_date) temp_a 
 join (select udid,create_date,event,event_cnt from kaola_stat.user_event_day where create_date>='2015-04-07' and create_date<='2015-04-11' and (event='200005' or event='200004')) temp_b
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date
 
 
 
 
  
3.4.1	2015-04-13      17539   4388    12036   40
3.4.1	2015-04-14      66529   17998   41140   3
3.4.1	2015-04-15      86762   18164   53450   0
3.4.1	2015-04-16      99066   19622   55625   0
3.4.1	2015-04-17      100889  20025   56497   0
3.4.1	2015-04-18      106901  25712   62859   0
3.4.1	2015-04-19      174294  31423   108121  0
3.3.1	2015-04-06      36265   0       184870  0
3.3.1	2015-04-07      38368   107     201953  548
3.3.1	2015-04-08      42761   225     234894  1512
3.3.1	2015-04-09      44589   223     249638  1897
3.3.1	2015-04-10      43847   231     251667  1968
3.3.1	2015-04-11      46278   260     259395  1968
3.3.1	2015-04-12      48303   280     262221  1905

--3.3.1  时间 4.6-4.12
--推荐页的浏览次数    推荐页的收听次数 推荐页的试听次数（300010）推荐页的点播次数（300024）    全部页的浏览次数   全部页的收听次数  全部页的收听次数 全部页的试听次数（300010）全部页的点播次数（300024）   
select temp_a.create_date,count(case when temp_b.event_code='200005' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200005'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200005'  then temp_b.udid end )+count(case when temp_b.event_code='300024' and temp_b.page ='200005'  then temp_b.udid end ),
count(case when temp_b.event_code='200004' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200004'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200004'  then temp_b.udid end )+count(case when temp_b.event_code='300024' and temp_b.page ='200004'  then temp_b.udid end )
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12' and version='3.3.1' and device_type=0 group by udid,create_date) temp_a 
join (select udid,create_date,page,event_code from basis_data.basis_event where create_date>='2015-04-06' and create_date<='2015-04-12' 
and (event_code='200005' or event_code='200004'or(event_code='300009' and (page ='200005' or page='200004')) or(event_code='300010' and (page ='200005' or page='200004'))  
or(event_code='300024' and (page ='200005' or page='200004')))) temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date


(安卓)版本 日期 推荐页的浏览次数    推荐页的收听次数     全部页的浏览次数   全部页的收听次数
3.3.1	2015-04-06      36203   3551    184718  6468
3.3.1	2015-04-07      34188   3885    182742  6310
3.3.1	2015-04-08      32728   3699    176442  6872
3.3.1	2015-04-09      33900   3987    179605  7047
3.3.1	2015-04-10      33433   4153    180439  6554
3.3.1	2015-04-11      35817   4858    186445  7691
3.3.1	2015-04-12      36500   4712    187142  7702


3.4.1	2015-04-13      16661   4389    10959   514
3.4.1	2015-04-14      66050   18006   40521   1450
3.4.1	2015-04-15      86269   18165   52765   2038
3.4.1	2015-04-16      98590   19623   55232   2200
3.4.1	2015-04-17      100442  20025   56161   2125
3.4.1	2015-04-18      106832  25715   62809   2490
3.4.1	2015-04-19      174255  31430   108093  3770


select temp_a.create_date,count(case when temp_b.event_code='200005' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200005'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200005'  then temp_b.udid end )+count(case when temp_b.event_code='300024' and temp_b.page ='200005'  then temp_b.udid end ),
count(case when temp_b.event_code='200004' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200004'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200004'  then temp_b.udid end )+count(case when temp_b.event_code='300024' and temp_b.page ='200004'  then temp_b.udid end )
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-13' and create_date<='2015-04-19' and version='3.4.1' and device_type=0 group by udid,create_date) temp_a 
join (select udid,create_date,page,event_code from basis_data.basis_event where create_date>='2015-04-13' and create_date<='2015-04-19' 
and (event_code='200005' or event_code='200004'or(event_code='300009' and (page ='200005' or page='200004')) or(event_code='300010' and (page ='200005' or page='200004'))  
or(event_code='300024' and (page ='200005' or page='200004')))) temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date


 --电台资料页（200003）全部播放（300033）按钮次数
(安卓)版本	日期	电台资料页	全部播放按钮次数
3.4.1	2015-04-14      71416   527
3.4.1	2015-04-15      94113   725
3.4.1	2015-04-16      102978  706
3.4.1	2015-04-17      105728  1296
3.4.1	2015-04-18      124233  810
3.4.1	2015-04-19      155738  1174
3.4.1	2015-04-20      121241  1050
3.4.1	2015-04-21      121812  1192
  select temp_a.create_date,sum(case when temp_b.event='200003' then temp_b.event_cnt end ),sum(case when temp_b.event='300033' then temp_b.event_cnt end )
 from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-14' and create_date<='2015-04-21' and version='3.4.1' and device_type=0 group by udid,create_date) temp_a 
 join (select udid,create_date,event,event_cnt from kaola_stat.user_event_day where create_date>='2015-04-14' and create_date<='2015-04-21' and (event='200003' or event='300033')) temp_b
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date
 
 --3.4.1 4.13-4.18  用户浏览  推荐   全部   我的电台    离线    设置   搜索 页面的次数 以及用户浏览   全部页  各个分类的次数

 select temp_a.create_date,sum(case when temp_b.event='200005' then temp_b.event_cnt end ),sum(case when temp_b.event='200004' then temp_b.event_cnt end ),
 sum(case when temp_b.event='200001' then temp_b.event_cnt end ),sum(case when temp_b.event='200007' then temp_b.event_cnt end ),
 sum(case when temp_b.event='200010' then temp_b.event_cnt end ), sum(case when temp_b.event='200008' then temp_b.event_cnt end )
 from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-13' and create_date<='2015-04-18' and version='3.4.1' group by udid,create_date) temp_a 
 join (select udid,create_date,event,event_cnt from kaola_stat.user_event_day where create_date>='2015-04-13' and create_date<='2015-04-18' 
 and (event='200005' or event='200004' or event='200001' or event='200007' or event='200010' or event='200008')) temp_b
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date

 ---------单独查询 全部页  各个分类的次数(200009)
 select catalog_tb.name,temp_c.create_date,count(temp_c.udid)
 from media_resources.tb_cms_catalog catalog_tb join (
 select temp_b.create_date,temp_b.type,temp_a.udid
 from  (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-13' and create_date<='2015-04-18' and version='3.4.1' group by create_date,udid) temp_a 
 join (select udid,create_date,type from basis_data.basis_event where create_date>='2015-04-13' and create_date<='2015-04-18' and event_code='200009') temp_b 
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date))temp_c on (catalog_tb.id = temp_c.type) group by catalog_tb.name,temp_c.create_date;
 
--4-15到4-21日 关键字  搜索次数  搜索人数
select temp_a.create_date,temp_a.result,sum(temp_a.search_cnt),sum(temp_a.search_user_cnt)
from (
	select create_date,result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from basis_data.basis_event where 
	create_date>='2015-04-15' and create_date<='2015-04-21' and event_code='300029' and type=1  group by create_date,result
	union all
	select createdate as create_date,eventinfo as result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from default.basis_common where 
	createdate>='2015-04-15' and createdate<='2015-04-21' and eventcode='300026' and remarks=1 group by createdate,eventinfo
) temp_a group by temp_a.create_date,temp_a.result

--3.4.1  我的电台页   点击离线的用户数 时间是  4.13-4.18

 select temp_a.create_date,count( distinct case when temp_b.event='200001' then temp_b.udid end ),count(distinct case when temp_b.event='200007' then temp_b.udid end )
 from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-13' and create_date<='2015-04-18' and version='3.4.1' group by udid,create_date) temp_a 
 join (select udid,create_date,event from kaola_stat.user_event_day where create_date>='2015-04-13' and create_date<='2015-04-18' 
 and (event='200001' or event='200007')) temp_b
 on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date
 
 
 
--3.4.1  4.13-4.19   电台资料页的浏览次数  在电台资料页的点播次数  进入大全二级页的次数
--3.3.1  4.6-4.12     电台资料页的浏览次数  在电台资料页的点播次数  进入大全二级页的次数


(安卓)版本	日期	电台资料浏览次数	电台资料点播次数	大全二级页次数

3.4.1	2015-04-13      15056   2035    7322
3.4.1	2015-04-14      71416   8348    29526
3.4.1	2015-04-15      94113   10246   39916
3.4.1	2015-04-16      102978  10945   42652
3.4.1	2015-04-17      105728  10941   42307
3.4.1	2015-04-18      124233  12364   47755
3.4.1	2015-04-19      155738  14063   62764

3.3.1	2015-04-06      202583  18755   96508
3.3.1	2015-04-07      190727  18386   91774
3.3.1	2015-04-08      184195  17835   87804
3.3.1	2015-04-09      193109  18170   90135
3.3.1	2015-04-10      192797  18206   87914
3.3.1	2015-04-11      207455  18832   93873
3.3.1	2015-04-12      212398  21437   98003

300033

select temp_a.create_date,count(case when temp_b.event_code='200003' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200003'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200003'  then temp_b.udid end )+
count(case when temp_b.event_code='300024' and temp_b.page ='200003'  then temp_b.udid end ),
count(case when temp_b.event_code='200009' then temp_b.udid end )
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-13' and create_date<='2015-04-19' and version='3.4.1' and device_type=0 group by udid,create_date) temp_a 
join (select udid,create_date,page,event_code from basis_data.basis_event where create_date>='2015-04-13' and create_date<='2015-04-19' 
and (event_code='200003' or event_code='200009'or(event_code='300009' and page ='200003') or(event_code='300010' and page ='200003') or(event_code='300024' and page ='200003' ))) temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date

select temp_a.create_date,count(case when temp_b.event_code='200003' then temp_b.udid end ),
count(case when temp_b.event_code='300009' and temp_b.page ='200003'  then temp_b.udid end )+count(case when temp_b.event_code='300010' and temp_b.page ='200003'  then temp_b.udid end )+count(case when temp_b.event_code='300024' and temp_b.page ='200003'  then temp_b.udid end ),
count(case when temp_b.event_code='200009' then temp_b.udid end )
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12' and version='3.3.1' and device_type=0 group by udid,create_date) temp_a 
join (select udid,create_date,page,event_code from basis_data.basis_event where create_date>='2015-04-06' and create_date<='2015-04-12' 
and (event_code='200003' or event_code='200009'or(event_code='300009' and page ='200003') or(event_code='300010' and page ='200003') or(event_code='300024' and page ='200003' ))) temp_b
on (temp_a.udid = temp_b.udid and temp_a.create_date=temp_b.create_date) group by temp_a.create_date
 
 
 --创建tb_cms_catalog(记录大全二级页面中的分类信息)
 
CREATE external table if not exists media_resources.tb_cms_catalog (
   id  int,
   create_date string,
   create_time bigint,
   name string,
   remark string,
   status int ,
   type int 
) ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LOCATION 'hdfs://ks01:8020/hive/media_resources/tb_cms_catalog';
  
 --导入表
/usr/local/hadoop/bin/hadoop fs -rm -r /hive/media_resources/tb_cms_catalog

/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://10.10.4.34/media_resources --username dev --password dev@2014 --query "SELECT id,date_format(create_date,'%Y-%m-%d'),unix_timestamp(create_date), name,remark,status,type FROM tb_cms_catalog WHERE \$CONDITIONS" --split-by id --fields-terminated-by '\t' --append --target-dir /hive/media_resources/tb_cms_catalog






--电台流5/22－5.28 收听次数、收听人数、收听时长、完整收听率、关注人数（PGC）

版本 3.1+
select t.create_date,t.radioid,max(t.radio_name),sum(t.play_count),sum(t.play_user_count),sum(t.play_time),sum(t.audio_length),
sum(t.follow_user_count) from (select t1.create_date,t1.radioid,max(t2.radio_name) radio_name,count(t1.udid) play_count,count(distinct t1.udid) play_user_count,
sum(if(t1.play_time>t2.audio_length,t2.audio_length,t1.play_time)) play_time,sum(t2.audio_length) audio_length,0 follow_user_count from 
(select create_date,udid,radioid,audioid,play_time from basis_data.basis_event where create_date>='2015-06-26' and create_date<='2015-07-02' and 
event_code='101010' and play_time>0) t1 join (select l.radio_id,l.audio_id,max(r.name) radio_name,max(l.audio_length) audio_length from 
media_resources.tb_radio_playlist l left join media_resources.tb_radio r on (l.radio_id=r.id) group by l.radio_id,l.audio_id) t2 on 
(t1.radioid=t2.radio_id and t1.audioid=t2.audio_id) group by t1.create_date,t1.radioid union all select a.create_date,a.radioid,b.name as radio_name,
a.play_count,a.play_user_count,a.play_time,a.audio_length,a.follow_user_count from (select create_date,radioid,0 play_count,0 play_user_count,0 play_time,
0 audio_length,count(distinct udid) follow_user_count from basis_data.basis_event where create_date>='2015-06-26' and create_date<='2015-07-02' and 
event_code='400005' group by create_date,radioid) a join media_resources.tb_radio b on (a.radioid=b.id)) t group by t.create_date,t.radioid

版本3.1+（第二部分）
select t1.create_date,t1.radioid,max(t2.radio_name) radio_name,count(t1.udid) play_count,count(distinct t1.udid) play_user_count,
sum(if(t1.play_time>t2.audio_length,t2.audio_length,t1.play_time)) play_time,sum(t2.audio_length) audio_length,0 follow_user_count 
from (select create_date,udid,radioid,audioid,play_time from basis_data.basis_event where create_date>='2015-06-26' and create_date<='2015-07-02'
 and event_code='101010' and play_time>0) t1 left join (select l.radio_id,l.audio_id,max(r.name) 
radio_name,max(l.audio_length) audio_length from media_resources.tb_radio_playlist l left join media_resources.tb_radio r 
on (l.radio_id=r.id) group by l.radio_id,l.audio_id) t2 on (t1.radioid=t2.radio_id and t1.audioid=t2.audio_id) group by t1.create_date,t1.radioid




版本 3.0
select t.create_date,t.radioid,max(t.radio_name),sum(t.play_count),sum(t.play_user_count),sum(t.play_time),sum(t.audio_length),
sum(t.follow_user_count) from (select t1.create_date,t1.radioid,max(t2.radio_name) radio_name,count(t1.udid) play_count,count(distinct t1.udid) play_user_count,
sum(if(t1.play_time>t2.audio_length,t2.audio_length,t1.play_time)) play_time,sum(t2.audio_length) audio_length,0 follow_user_count from 
(select createdate  create_date,udid,playthemeid  radioid,programid audioid,playtime  play_time from default.basis_common where createdate>='2015-04-17' and createdate<='2015-04-23' and 
eventcode='101010' and playtime>0) t1 join (select l.radio_id,l.audio_id,max(r.name) radio_name,max(l.audio_length) audio_length from 
media_resources.tb_radio_playlist l left join media_resources.tb_radio r on (l.radio_id=r.id) group by l.radio_id,l.audio_id) t2 on 
(t1.radioid=t2.radio_id and t1.audioid=t2.audio_id) group by t1.create_date,t1.radioid union all select a.create_date,a.radioid,b.name as radio_name,
a.play_count,a.play_user_count,a.play_time,a.audio_length,a.follow_user_count from (select createdate  create_date,playthemeid  radioid,0 play_count,0 play_user_count,0 play_time,
0 audio_length,count(distinct udid) follow_user_count from default.basis_common where createdate>='2015-04-17' and createdate<='2015-04-23' and 
eventcode='400005' group by createdate,playthemeid) a join media_resources.tb_radio b on (a.radioid=b.id)) t group by t.create_date,t.radioid


版本3.0

select t1.create_date,t1.radioid,max(t2.radio_name) radio_name,count(t1.udid) play_count,count(distinct t1.udid) play_user_count,
sum(if(t1.play_time>t2.audio_length,t2.audio_length,t1.play_time)) play_time,sum(t2.audio_length) audio_length,0 follow_user_count 
from (select createdate create_date,udid,playthemeid  radioid,programid audioid,playtime play_time from default.basis_common where createdate>='2015-04-17' and 
createdate<='2015-04-23' and eventcode='101010' and playtime>0) t1 left join (select l.radio_id,l.audio_id,max(r.name) 
radio_name,max(l.audio_length) audio_length from media_resources.tb_radio_playlist l left join media_resources.tb_radio r 
on (l.radio_id=r.id) group by l.radio_id,l.audio_id) t2 on (t1.radioid=t2.radio_id and t1.audioid=t2.audio_id) group by t1.create_date,t1.radioid




--版本3.3.1 时间4.6-4.12  系统 Android  
条件1 当日的新用户
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数
条件2 次日的留存用户
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数
版本3.4.1 时间4.13-4.19  系统 Android  
条件1 当日的新用户
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数
条件2 次日的留存用户
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数

--新用户
 版本(系统安卓)	日期	推荐页浏览次数	推荐页收听次数	全部页浏览次数	全部页收听次数	电台资料页浏览次数	电台资料页收听次数	全部二级页浏览次数
3.3.1	2015-04-06      10889   1431    37408   1994    32529   3597    23322
3.3.1	2015-04-07      9473    1345    32772   1863    27524   3053    19823
3.3.1	2015-04-08      8703    1312    30514   2025    24933   2918    18291
3.3.1	2015-04-09      9496    1714    31687   1823    28053   2970    19511
3.3.1	2015-04-10      9068    1545    30994   1806    27886   3209    18470
3.3.1	2015-04-11      9812    1912    33184   2003    30187   3382    20203
3.3.1	2015-04-12      10101   1880    33222   2282    30183   3463    20483

3.4.1	2015-04-13      8924    2519    6647    283     8724    1319    4710
3.4.1	2015-04-14      25678   8327    18036   743     25572   3570    13195
3.4.1	2015-04-15      23977   5763    17197   760     21512   2791    12769
3.4.1	2015-04-16      23184   5354    16013   751     21166   2758    12137
3.4.1	2015-04-17      22186   5265    15123   646     19819   2551    10881
3.4.1	2015-04-18      25613   7355    18727   824     25686   3127    13735
3.4.1	2015-04-19      45819   9487    33763   1325    34213   3676    18253
select
tmp_c.create_date, 
count(case when temp_d.event_code='200005' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200005'  then temp_d.udid end ),
count(case when temp_d.event_code='200004' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200004'  then temp_d.udid end ),
count(case when temp_d.event_code='200003' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200003'  then temp_d.udid end ),
count(case when temp_d.event_code='200009' then temp_d.udid end )
from 
(select tmp_a.udid ,tmp_a.create_date from 
(select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12' and version='3.3.1' and device_type=0 ) tmp_a inner join 
(select udid,create_date from kaola_stat.user_stat_day where create_date>='2015-04-06' and create_date<='2015-04-12' and user_new=1) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) ) tmp_c inner join (
select udid,event_code,page,create_date from basis_data.basis_event where  create_date>='2015-04-06' and create_date<='2015-04-12' 
and (event_code='200005' or event_code='200004' or event_code='200003' or event_code='200009' or event_code='300009' 
or event_code='300010' or event_code='300024')
) temp_d on (tmp_c.udid = temp_d.udid and tmp_c.create_date=temp_d.create_date) group by tmp_c.create_date
 
 
--次日留存用户 
版本(系统安卓) 日期	推荐页浏览次数	推荐页收听次数	全部页浏览次数	全部页收听次数	电台资料页浏览次数	电台资料页收听次数	全部二级页浏览次数
3.3.1	2015-04-06      4079    573     13052   790     15720   1604    9914
3.3.1	2015-04-07      4047    650     12563   780     15290   1491    9523
3.3.1	2015-04-08      3619    600     11344   963     13522   1414    8612
3.3.1	2015-04-09      4011    783     12228   770     15358   1359    9581
3.3.1	2015-04-10      3524    603     11410   824     14642   1483    8295
3.3.1	2015-04-11      3682    704     11892   857     15430   1553    9299
3.3.1	2015-04-12      3814    691     11911   945     15012   1637    9199

3.4.1	2015-04-13      3391    1069    2868    131     4850    633     2487
3.4.1	2015-04-14      9643    3569    7468    334     13644   1635    6497
3.4.1	2015-04-15      8600    2282    6806    393     11214   1365    5977
3.4.1	2015-04-16      8193    2022    6519    322     10911   1237    5900
3.4.1	2015-04-17      7567    1919    6050    282     10301   1184    5203
3.4.1	2015-04-18      9104    2974    7482    363     13518   1454    6737
3.4.1	2015-04-19      14454   3391    11877   529     15775   1445    7528
 select
tmp_c.create_date, 
count(case when temp_d.event_code='200005' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200005'  then temp_d.udid end ),
count(case when temp_d.event_code='200004' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200004'  then temp_d.udid end ),
count(case when temp_d.event_code='200003' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200003'  then temp_d.udid end ),
count(case when temp_d.event_code='200009' then temp_d.udid end )
from 
(select tmp_a.udid ,tmp_a.create_date from 
(select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12' and version='3.3.1' and device_type=0 ) tmp_a inner join 
(select udid,create_date from kaola_stat.user_stat_day where create_date>='2015-04-06' and create_date<='2015-04-12' and remain_1_days = 1 ) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) ) tmp_c inner join (
select udid,event_code,page,create_date from basis_data.basis_event where  create_date>='2015-04-06' and create_date<='2015-04-12' 
and (event_code='200005' or event_code='200004' or event_code='200003' or event_code='200009' or event_code='300009' 
or event_code='300010' or event_code='300024')
) temp_d on (tmp_c.udid = temp_d.udid and tmp_c.create_date=temp_d.create_date) group by tmp_c.create_date


--版本3.4.1 时间4.13-4.19  系统 Android  
条件   次日没有来的用户（相对于次日留存用户）
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数
版本3.3.1 时间4.6-4.12  系统 Android  
条件   次日没有来的用户（相对于次日留存用户）
指标   推荐页浏览次数、推荐页收听次数、全部页浏览次数、全部页收听次数、电台资料页浏览次数、电台资料页收听次数、全部二级页浏览次数

 select
tmp_c.create_date, 
count(case when temp_d.event_code='200005' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200005'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200005'  then temp_d.udid end ),
count(case when temp_d.event_code='200004' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200004'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200004'  then temp_d.udid end ),
count(case when temp_d.event_code='200003' then temp_d.udid end ),
count(case when temp_d.event_code='300009' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300010' and temp_d.page ='200003'  then temp_d.udid end )+count(case when temp_d.event_code='300024' and temp_d.page ='200003'  then temp_d.udid end ),
count(case when temp_d.event_code='200009' then temp_d.udid end )
from 
(select tmp_a.udid ,tmp_a.create_date from 
(select udid,create_date from kaola_stat.user_info_day where create_date>='2015-04-06' and create_date<='2015-04-12' and version='3.3.1' and device_type=0 ) tmp_a inner join 
(select udid,create_date from kaola_stat.user_stat_day where create_date>='2015-04-06' and create_date<='2015-04-12' and remain_1_days = 0 and user_new=1 ) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) ) tmp_c inner join (
select udid,event_code,page,create_date from basis_data.basis_event where  create_date>='2015-04-06' and create_date<='2015-04-12' 
and (event_code='200005' or event_code='200004' or event_code='200003' or event_code='200009' or event_code='300009' 
or event_code='300010' or event_code='300024')
) temp_d on (tmp_c.udid = temp_d.udid and tmp_c.create_date=temp_d.create_date) group by tmp_c.create_date



--basis_common表中百度、应用宝渠道，2014-12-01到2014-05各版本的启动次数、人数，进入播放器页的次数、人数

select createdate,version,channel,count(case when eventcode='100010' then udid end) ,count(distinct case when eventcode='100010' then udid end),
count(case when eventcode='202000' then udid end) ,count(distinct case when eventcode='202000' then udid end)
from  default.basis_common where createdate>='2014-12-01' and createdate<='2014-12-05' and (eventcode='100010' or eventcode='202000') and (channel='A-baidu' or channel='A-myapp')
group by createdate,version,channel


--basis_common表中百度、应用宝渠道，2014-12-01到2014-05各版本的启动次数、人数，进入播放器页的次数、人数,新用户的启动次数、人数、新用户的播放次数、人数
select tmp_a.createdate,tmp_a.version,tmp_a.channel,
count(case when tmp_a.eventcode='100010' then tmp_a.udid end) ,
count(distinct case when tmp_a.eventcode='100010' then tmp_a.udid end),
count(case when tmp_a.eventcode='202000' then tmp_a.udid end) ,
count(distinct case when tmp_a.eventcode='202000' then tmp_a.udid end),
count(case when tmp_a.eventcode='100010' and tmp_b.user_new =1 then tmp_a.udid end) ,
count(distinct case when tmp_a.eventcode='100010' and tmp_b.user_new =1 then tmp_a.udid end),
count(case when tmp_a.eventcode='202000'  and tmp_b.user_new =1 then tmp_a.udid end) ,
count(distinct case when tmp_a.eventcode='202000' and tmp_b.user_new =1 then tmp_a.udid end)
from 
(select createdate,version,channel,eventcode,udid
from  default.basis_common where createdate>='2014-12-01' and createdate<='2014-12-05' and (eventcode='100010' or eventcode='202000') and (channel='A-baidu' or channel='A-myapp')) tmp_a 
inner join (select udid,user_new,create_date from kaola_stat.user_stat_day where create_date>='2014-12-01' and create_date<='2014-12-05') tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.createdate=tmp_b.create_date)
group by tmp_a.createdate,tmp_a.version,tmp_a.channel



--basis_event表中百度、应用宝渠道，2015-03-25到2015-03-30各版本的启动次数、人数，进入播放器页的次数、人数

select tmp_a.create_date,tmp_a.version,tmp_a.channel,count(case when tmp_b.event_code='100010' then tmp_a.udid end) ,count(distinct case when tmp_b.event_code='100010' then tmp_a.udid end),
count(case when tmp_b.event_code='200002' then tmp_a.udid end) ,count(distinct case when tmp_b.event_code='200002' then tmp_a.udid end)
from (select udid,version,create_date,channel from basis_data.basis_device_info_day where create_date>='2015-03-25' and create_date<='2015-03-30' and (channel='A-baidu' or channel='A-myapp')) tmp_a 
inner join (select udid ,event_code,create_date from basis_data.basis_event where create_date>='2015-03-25' and create_date<='2015-03-30' and (event_code='100010' or event_code='200002')) tmp_b
on (tmp_a.udid=tmp_b.udid and tmp_a.create_date=tmp_b.create_date) group by tmp_a.create_date,tmp_a.version,tmp_a.channel

--basis_event表中百度、应用宝渠道，2015-03-25到2015-03-30各版本的启动次数、人数，进入播放器页的次数、人数,新用户的启动次数、人数、新用户的播放次数、人数

select tmp_a.create_date,tmp_a.version,tmp_a.channel,
count(case when tmp_b.event_code='100010' then tmp_a.udid end) ,
count(distinct case when tmp_b.event_code='100010' then tmp_a.udid end),
count(case when tmp_b.event_code='200002' then tmp_a.udid end) ,
count(distinct case when tmp_b.event_code='200002' then tmp_a.udid end),
count(case when tmp_b.event_code='100010' and tmp_a.user_new =1 then tmp_a.udid end) ,
count(distinct case when tmp_b.event_code='100010' and tmp_a.user_new =1 then tmp_a.udid end),
count(case when tmp_b.event_code='200002' and tmp_a.user_new =1  then tmp_a.udid end) ,
count(distinct case when tmp_b.event_code='200002' and tmp_a.user_new =1  then tmp_a.udid end)
from (select tmp_a1.udid,tmp_a1.version,tmp_a1.create_date,tmp_a1.channel,tmp_a2.user_new 
from (select udid,version,create_date,channel from basis_data.basis_device_info_day where create_date>='2015-03-25' and create_date<='2015-03-30' and (channel='A-baidu' or channel='A-myapp')) tmp_a1
inner join (
select udid,create_date,user_new from kaola_stat.user_stat_day where create_date>='2015-03-25' and create_date<='2015-03-30'
) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date=tmp_a2.create_date)) tmp_a 
inner join (select udid ,event_code,create_date from basis_data.basis_event where create_date>='2015-03-25' and create_date<='2015-03-30' and (event_code='100010' or event_code='200002')) tmp_b
on (tmp_a.udid=tmp_b.udid and tmp_a.create_date=tmp_b.create_date) group by tmp_a.create_date,tmp_a.version,tmp_a.channel



select tmp_a1.udid,tmp_a1.version,tmp_a1.create_date,tmp_a1.channel,tmp_a2.user_new 
from 
(select udid,version,create_date,channel from basis_data.basis_device_info_day where create_date>='2015-03-25' and create_date<='2015-03-30' and (channel='A-baidu' or channel='A-myapp')) tmp_a1
inner join (
select udid,create_date,user_new from kaola_stat.user_stat_day where create_date>='2015-03-25' and create_date<='2015-03-30'
) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date=tmp_a2.create_date)



--3.4.1  4月22日   安卓系统     ［我的电台｝页 浏览次数    浏览人数   收听次数（直接收听）  收听人数   离线次数   离线人数

select count(case when tmp_b.event_code='200001' then tmp_b.udid end ),count(distinct case when tmp_b.event_code='200001' then tmp_b.udid end ),
count(case when tmp_b.event_code='300009' and tmp_b.page='200001' then tmp_b.udid end ),count(distinct case when tmp_b.event_code='300009' and tmp_b.page='200001' then tmp_b.udid end ),
count(case when tmp_b.event_code='300006' and tmp_b.page='200001' then tmp_b.udid end ),count(distinct case when tmp_b.event_code='300006' and tmp_b.page='200001' then tmp_b.udid end )
from (select udid from basis_data.basis_device_info_day where create_date='2015-04-22' and device_type=0 and version='3.4.1') tmp_a inner join 
(select udid,event_code,page from basis_data.basis_event where create_date='2015-04-22' and (event_code='200001' or event_code='300009' or event_code='300006')) tmp_b 
on (tmp_a.udid = tmp_b.udid)


3.4.1  4月22日 我的电台页浏览次数    浏览人数   收听次数（直接收听）  收听人数   离线次数   离线人数
78235   27480   77244   21612   47845   15699

--考拉FM 公司各个用户的分享次数
create external table if not exists test_db.kaola_dept_udid (
name string,
dept_name string,
udid string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://ks01:8020/hive/test_db/kaola_dept_udid';

select tmp_a.udid,tmp_a.name,tmp_a.dept_name,tmp_b.sum_event_cnt
from test_db.kaola_dept_udid tmp_a left join 
(select udid,event,sum(event_cnt) as sum_event_cnt from kaola_stat.user_event_day where create_date>='2015-08-01' and create_date<='2015-08-31' and event='300008' group by udid,event) tmp_b
on (tmp_a.udid = tmp_b.udid) order by tmp_b.sum_event_cnt desc 




----3.5.1  4月30日~5月6号   安卓系统   推荐页浏览人数	推荐页点播人数	推荐页关注人数	推荐页点击人数（进入详情）	推荐页banner点击人数

select tmp_a.create_date,count(distinct case when tmp_b.event_code='200005' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300009' and tmp_b.page='200005' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='400005' and tmp_b.page='200005' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200003' and tmp_b.page_prev='200005' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300016' and tmp_b.page='200005' then tmp_b.udid end )
from (select udid ,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and device_type=0 and version='3.5.1' group by create_date,udid) tmp_a inner join 
(select udid,event_code,source,create_date,page,page_prev from kaola_stat.user_event_stream_day where create_date>='2015-04-30' and create_date<='2015-05-06' and 
(event_code='200005' or event_code='300009' or event_code='400005' or event_code='200003' or event_code='300016')
) tmp_b  
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date


----3.5.1  4月30日~5月6号   安卓系统   
--全部页浏览人数	全部页点播人数	全部页关注人数	推荐页点击人数（进入详情）	推荐页banner点击人数	全部页进入大全二级页人数	全部页进入直播列表页人数

select tmp_a.create_date,count(distinct case when tmp_b.event_code='200004' then tmp_b.udid end ),
count(distinct case when (tmp_b.event_code='300009' or tmp_b.event_code='300010') and tmp_b.page='200004' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='400005' and tmp_b.page='200004' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200003' and tmp_b.page_prev='200004' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300016' and tmp_b.page='200004' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200009' and tmp_b.page_prev='200004' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200011' and tmp_b.page_prev='200004' then tmp_b.udid end )
from (select udid ,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and device_type=0 and version='3.5.1' group by create_date,udid) tmp_a inner join 
(select udid,event_code,source,create_date,page,page_prev from kaola_stat.user_event_stream_day where create_date>='2015-04-30' and create_date<='2015-05-06' and 
(event_code='200004' or event_code='300009' or event_code='300010' or event_code='400005' or event_code='200003' or event_code='300016' or event_code='200009' or event_code='200011')
) tmp_b  
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date

----3.5.1  4月30日~5月6号   安卓系统 
--我的电台浏览人数	我的电台点播人数	我的电台点击人数（进入详情）	我的电台离线人数	浏览收听历史人数

select tmp_a.create_date,count(distinct case when tmp_b.event_code='200001' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300009' and tmp_b.page='200001' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200003' and tmp_b.page_prev='200001' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300006' and tmp_b.page='200001' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200006' then tmp_b.udid end )
from (select udid ,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and device_type=0 and version='3.5.1' group by create_date,udid) tmp_a inner join 
(select udid,event_code,source,create_date,page,page_prev from kaola_stat.user_event_stream_day where create_date>='2015-04-30' and create_date<='2015-05-06' and 
(event_code='200001' or event_code='300009'  or event_code='200003' or event_code='300006' or event_code='200006')
) tmp_b  
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date

--3.5.1  4月30日~5月6号   安卓系统
--详情页浏览人数	详情页分享人数	详情页关注人数	详情页取消关注人数	详情页收听全部人数	详情页点播人数	详情页离线人数	详情页批量离线人数

select tmp_a.create_date,
count(distinct case when tmp_b.event_code='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300008' and tmp_b.page_prev='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='400005' and tmp_b.page='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='400006' and tmp_b.page='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300009' and tmp_b.page='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300024' and tmp_b.page='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300006' and tmp_b.page='200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300037' and tmp_b.page_prev='200003' then tmp_b.udid end )
from (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and device_type=0 and version='3.5.1' group by udid,create_date) tmp_a 
inner join 
(select udid,event_code,page_prev,create_date,page from kaola_stat.user_event_stream_day where create_date>='2015-04-30' and create_date<='2015-05-06' and 
(event_code='200003' or event_code='300008' or event_code='400005' or event_code='400006' or event_code='300009' or event_code='300025' or event_code='300006' or event_code='300037')) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date


--3.5.1  4月30日~5月6号   安卓系统 进入播放器
--	点赞人数	分享人数	离线人数	暂停人数	暂停后播放人数	拖拽人数	点击上一首人数	点击下一首人数	点击上一个电台人数	点击下一个电台人数	浏览详情人数
select tmp_a.create_date,
count(distinct case when tmp_b.event_code='400007' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300008' and tmp_b.page='200002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300006' and tmp_b.page='200002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300020' and tmp_b.page='200002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300021' and tmp_b.page='200002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300023'  then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300013'  then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300014' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300011' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='300012' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code='200003' and tmp_b.page_prev='200002' then tmp_b.udid end )
from (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and device_type=0 and version='3.5.1' group by udid,create_date) tmp_a inner join 
( 
select udid,event_code,page,page_prev,create_date from kaola_stat.user_event_stream_day where create_date>='2015-04-30' and create_date<='2015-05-06' and 
(event_code='400007' or event_code='300008' or event_code='300006' or event_code='300020' or event_code='300021' or event_code='300023' or event_code='300013' 
or event_code='300014' or event_code='300011' or event_code='300012' or  event_code='200003')
) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date


--3.5.1 4月30日－5月6日  闹钟设置为开的人数

select tmp_a.create_date,count(distinct tmp_a.udid)
from (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' group by udid ,create_date) tmp_a join 
(select udid ,create_date from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and event_code='100004' and type = '5' and result='1') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date


--3.5.1 2015-04-30~2015-05-06 最新按钮使用人数 最热按钮使用人数 二级分类使用人数 断点续听使用人数
select tmp_a.create_date,count(distinct case when tmp_b.areatag='2'  and tmp_b.event_code='200009' then tmp_b.udid end),
count(distinct case when tmp_b.areatag='1'  and tmp_b.event_code='200009' then tmp_b.udid end),
count(distinct case when tmp_b.result !='0'  and tmp_b.event_code='200009' then tmp_b.udid end),
count(distinct case when  tmp_b.event_code='300036' then tmp_b.udid end)
from (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0' group by udid ,create_date) tmp_a join 
(select udid ,create_date,event_code,areatag,result from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and (event_code='200009' or event_code='300036')) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date

select tmp_a.create_date,tmp_b.event_code,tmp_b.areatag,tmp_b.result 
from (select udid,create_date from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0' group by udid ,create_date) tmp_a join 
(select udid ,create_date,event_code,areatag,result from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and (event_code='200009' or event_code='300036')) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) limit 100;


版本3.5.1 日期2015-05-06 非后台部署push数据 推送接收次数	推送接收人数	推送点击次数	推送点击人数
11907   8265    748     642
SELECT  count((case when tmp_b.event_code='101011' then tmp_b.udid end)),count(distinct (case when tmp_b.event_code='101011' then tmp_b.udid end)),
count((case when tmp_b.event_code='300015' then tmp_b.udid end)),count(distinct (case when tmp_b.event_code='300015' then tmp_b.udid end)) 
FROM (select udid from basis_data.basis_device_info_day 
 where create_date='2015-05-07' and version='3.5.1'
) tmp_a join (select tmp_b_2.udid,tmp_b_2.event_code from media_resources.tb_push tmp_b_1 
right join (select udid,event_code,eventid from basis_data.basis_event where create_date='2015-05-07' and (event_code='101011' or event_code='300015')) tmp_b_2 on (tmp_b_1.id = tmp_b_2.eventid) where tmp_b_1.id is null) tmp_b
on (tmp_a.udid = tmp_b.udid);


--关闭自动播放的人数，关闭自动播放的客户端的启动次数 时间是4.30-5.6，版本3.5.1，系统Android
SELECT  tmp_b.create_date,count( distinct case when tmp_b.event_code='100004' then tmp_b.udid end)
FROM (select udid,create_date from basis_data.basis_device_info_day 
 where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0'
) tmp_a join (
select udid,event_code,eventid,create_date from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and event_code='100004' and type='3' and result='0') tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date;

日期	关闭自动播放的人数
2015-04-30      802
2015-05-01      672
2015-05-02      664
2015-05-03      1282
2015-05-04      732
2015-05-05      643
2015-05-06      369


SELECT  tmp_b.create_date,count( case when tmp_b.event_code='100010' then tmp_b.udid end)
FROM (select udid,create_date from basis_data.basis_device_info_day 
 where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0'
) tmp_a join (select tmp_c_1.udid,tmp_c_1.event_code,tmp_c_1.create_date from 
(select udid,event_code,create_date from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and event_code='100010' ) tmp_c_1
left semi join (select udid,event_code,create_date from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and event_code='100004' and type='3' and result='0')  tmp_c_2
on ( tmp_c_1.udid =  tmp_c_2.udid and tmp_c_1.create_date = tmp_c_2.create_date)) tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date;

日期	关闭自动播放的客户端的启动次数
2015-04-30      7280
2015-05-01      4770
2015-05-02      4617
2015-05-03      16156
2015-05-04      3724
2015-05-05      4157
2015-05-06      1693

3.5.1版本，安卓系统 离线模块收听次数分布
时间 	1:00	2:00	3:00	4:00	5:00	6:00	7:00	8:00	9:00	10:00	11:00	12:00	13:00	14:00	15:00	16:00	17:00	18:00	19:00	20:00	21:00	22:00	23:00	0:00
2015/4/30
2015/5/1
2015/5/2
2015/5/3
2015/5/4
2015/5/5
2015/5/6


select tmp_b.create_date,
count(case when tmp_b.hour='01' then tmp_b.udid end) as hour_01_times, 
count(case when tmp_b.hour='02' then tmp_b.udid end) as hour_02_times,
count(case when tmp_b.hour='03' then tmp_b.udid end) as hour_03_times,
count(case when tmp_b.hour='04' then tmp_b.udid end) as hour_04_times,
count(case when tmp_b.hour='05' then tmp_b.udid end) as hour_05_times,
count(case when tmp_b.hour='06' then tmp_b.udid end) as hour_06_times,
count(case when tmp_b.hour='07' then tmp_b.udid end) as hour_07_times,
count(case when tmp_b.hour='08' then tmp_b.udid end) as hour_08_times,
count(case when tmp_b.hour='09' then tmp_b.udid end) as hour_09_times,
count(case when tmp_b.hour='10' then tmp_b.udid end) as hour_10_times,
count(case when tmp_b.hour='11' then tmp_b.udid end) as hour_11_times,
count(case when tmp_b.hour='12' then tmp_b.udid end) as hour_12_times,
count(case when tmp_b.hour='13' then tmp_b.udid end) as hour_13_times,
count(case when tmp_b.hour='14' then tmp_b.udid end) as hour_14_times,
count(case when tmp_b.hour='15' then tmp_b.udid end) as hour_15_times,
count(case when tmp_b.hour='16' then tmp_b.udid end) as hour_16_times,
count(case when tmp_b.hour='17' then tmp_b.udid end) as hour_17_times,
count(case when tmp_b.hour='18' then tmp_b.udid end) as hour_18_times,
count(case when tmp_b.hour='19' then tmp_b.udid end) as hour_19_times,
count(case when tmp_b.hour='20' then tmp_b.udid end) as hour_20_times,
count(case when tmp_b.hour='21' then tmp_b.udid end) as hour_21_times,
count(case when tmp_b.hour='22' then tmp_b.udid end) as hour_22_times,
count(case when tmp_b.hour='23' then tmp_b.udid end) as hour_23_times,
count(case when tmp_b.hour='00' then tmp_b.udid end)  as hour_00_times
from 
(select udid,create_date,hour from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0') tmp_a 
inner join (select udid,create_date,hour,source from kaola_stat.user_event_stream_day 
where create_date>='2015-04-30' and create_date<='2015-05-06' and (event_code='300009' or event_code='300010' or event_code='300024') and source='200007') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date=tmp_b.create_date and tmp_a.hour = tmp_b.hour) group by tmp_b.create_date

3.5.1版本，安卓系统 非离线模块收听次数分布
时间  1:00	2:00	3:00	4:00	5:00	6:00	7:00	8:00	9:00	10:00	11:00	12:00	13:00	14:00	15:00	16:00	17:00	18:00	19:00	20:00	21:00	22:00	23:00	0:00
2015/4/30
2015/5/1
2015/5/2
2015/5/3
2015/5/4
2015/5/5
2015/5/6
select tmp_b.create_date,
count(case when tmp_b.hour='01' then tmp_b.udid end), 
count(case when tmp_b.hour='02' then tmp_b.udid end),
count(case when tmp_b.hour='03' then tmp_b.udid end),
count(case when tmp_b.hour='04' then tmp_b.udid end),
count(case when tmp_b.hour='05' then tmp_b.udid end),
count(case when tmp_b.hour='06' then tmp_b.udid end),
count(case when tmp_b.hour='07' then tmp_b.udid end),
count(case when tmp_b.hour='08' then tmp_b.udid end),
count(case when tmp_b.hour='09' then tmp_b.udid end),
count(case when tmp_b.hour='10' then tmp_b.udid end),
count(case when tmp_b.hour='11' then tmp_b.udid end),
count(case when tmp_b.hour='12' then tmp_b.udid end),
count(case when tmp_b.hour='13' then tmp_b.udid end),
count(case when tmp_b.hour='14' then tmp_b.udid end),
count(case when tmp_b.hour='15' then tmp_b.udid end),
count(case when tmp_b.hour='16' then tmp_b.udid end),
count(case when tmp_b.hour='17' then tmp_b.udid end),
count(case when tmp_b.hour='18' then tmp_b.udid end),
count(case when tmp_b.hour='19' then tmp_b.udid end),
count(case when tmp_b.hour='20' then tmp_b.udid end),
count(case when tmp_b.hour='21' then tmp_b.udid end),
count(case when tmp_b.hour='22' then tmp_b.udid end),
count(case when tmp_b.hour='23' then tmp_b.udid end),
count(case when tmp_b.hour='00' then tmp_b.udid end)
from 
(select udid,create_date,hour from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0') tmp_a 
inner join (select udid,create_date,hour,source from kaola_stat.user_event_stream_day 
where create_date>='2015-04-30' and create_date<='2015-05-06' and (event_code='300009' or event_code='300010' or event_code='300024') and source !='200007') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date=tmp_b.create_date and tmp_a.hour = tmp_b.hour) group by tmp_b.create_date

--播放器页面每天的关注人数 时间是  4月30日－5月6日   安卓系统   3.5.1版本

select tmp_b.create_date, count(distinct tmp_b.udid )
from 
(select udid,create_date,hour from basis_data.basis_device_info_day where create_date>='2015-04-30' and create_date<='2015-05-06' and version='3.5.1' and device_type='0') tmp_a
inner join (select udid ,create_date from basis_data.basis_event where create_date>='2015-04-30' and create_date<='2015-05-06' and event_code='400005' and page= '200002') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date;
安卓系统   3.5.1版本播放器页面每天的关注人数
时间        关注人数
2015-04-30      2469
2015-05-01      2955
2015-05-02      3109
2015-05-03      3413
2015-05-04      3139
2015-05-05      2837
2015-05-06      1407

--获取指定运营位(搞笑)的统计数据（维度：小时)   --所有运营位信息见：media_resources.tb_operate_list表（areatag对应tb_operate_list的type字段）
select tmp_b.areatag,tmp_b.radioid,count(tmp_b.udid),count(distinct tmp_b.udid)
from (select udid from basis_data.basis_device_info_day where create_date='2015-05-21' and version='3.5.2' group by udid) tmp_a  join (
select udid ,hour,areatag,radioid from basis_data.basis_event where create_date='2015-05-21' and
 (event_code = '300024' or event_code = '300009' or event_code = '300010' ) and (page='200004' or page='200005')
 and areatag ='30'
) tmp_b on (tmp_a.udid = tmp_b.udid) group by tmp_b.areatag,tmp_b.radioid


30      1100000000057   18      18
30      1100000000628   21      21
30      1100000000753   31      27
30      1100000001778   20      20
30      1100000012719   1       1
30      1100000038878   3       3

--当前查询结果
30      00      2       2
30      01      1       1
30      02      3       3
30      04      2       2
30      05      1       1
30      06      3       3
30      07      5       4
30      08      4       4
30      09      8       6
30      10      5       5
30      11      4       3
30      12      2       2
30      13      5       4
30      14      3       3
30      15      2       2
30      16      9       5
30      17      1       1
30      18      4       4
30      19      2       2
30      20      7       7
30      21      10      8
30      22      6       6
30      23      5       4

30      94      81

--脚本查询结果
00      30      1       1
01      30      1       1
02      30      3       3
03      30      0       0
04      30      2       2
05      30      0       0
06      30      2       2
07      30      1       1
08      30      3       3
09      30      5       6
10      30      2       2
11      30      2       3
12      30      1       1
13      30      3       4
14      30      3       3
15      30      2       2
16      30      4       5
17      30      0       0
18      30      2       2
19      30      1       1
20      30      6       6
21      30      6       7
22      30      3       3
23      30      3       4

select page,hour from basis_data.basis_event where create_date='2015-05-21' and (event_code = '300024' or event_code = '300009' or event_code = '300010' or (event_code='300025' and page='200003')) and areatag ='30' group by page,hour



--各个分类一天收听次数的分布数据,时间段是5.18-5.24

--专辑
select tmp_c_2.create_date,tmp_c_1.name,
sum(case when tmp_c_2.hour='00' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='01' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='02' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='03' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='04' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='05' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='06' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='07' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='08' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='09' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='10' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='11' then  tmp_c_2.play_times   else 0L end),
sum(case when tmp_c_2.hour='12' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='13' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='14' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='15' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='16' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='17' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='18' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='19' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='20' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='21' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='22' then  tmp_c_2.play_times   else 0L end) ,
sum(case when tmp_c_2.hour='23' then  tmp_c_2.play_times   else 0L end) 
from media_resources.tb_catalog tmp_c_1 left join ( 
select tmp_b.create_date,tmp_b.hour,tmp_a.catalog_id ,sum(tmp_b.play_time) as play_times
from  media_resources.tb_album tmp_a join 
(select count(udid) as play_time,radioid,create_date,hour from basis_data.basis_event where create_date >='2015-05-18' and create_date<='2015-05-24' and appid='10001' and event_code='101010' group by create_date,hour,radioid
union all 
select count(udid) as play_time,columnid as radioid,createdate as create_date,hour from default.basis_common where createdate >='2015-05-18' and createdate<='2015-05-24' and eventcode='101010' group by createdate,hour,columnid
 ) tmp_b on (tmp_a.id = tmp_b.radioid) 
 group by tmp_b.create_date,tmp_b.hour,tmp_a.catalog_id
) tmp_c_2 
on (tmp_c_1.id = tmp_c_2.catalog_id) 
group by tmp_c_2.create_date,tmp_c_1.name





--查一下多app每天的新增和活跃量，时间是  5.10-5.16 （appid为 com.ourapp.app_序号）	
select create_date,count(distinct udid) from basis_data.basis_event where create_date>='2015-05-10' and create_date<='2015-05-16' and appid like 'com.ourapp.app%' and ((event_code='101010' and play_time>0) or event_code='100010') group by create_date;
日期	活跃用户
2015-05-10      369
2015-05-11      373
2015-05-12      370
2015-05-13      322
2015-05-14      383
2015-05-15      377
2015-05-16      479



select sum(play_count) from analysis_of_album_stat_client where stat_date='2015-05-19' and catalog_name='脱口秀' group by catalog_name;



--全部页中各个客户端的分类的收听人数 
select  tmp_b.create_date,tmp_a.name ,tmp_b.user_cnt
from ( select id,name from media_resources.tb_cms_catalog group by id,name) tmp_a join 
(select count(distinct udid) as user_cnt,create_date,source_type from kaola_stat.user_event_stream_day where create_date>='2015-05-11' and create_date<='2015-05-17' and (event_code='300009' or event_code='300010' or event_code='300024') and source='200009' group by source_type,create_date ) tmp_b
on(tmp_a.id = tmp_b.source_type)

select tmp_c_2.create_date,tmp_c_2.name,tmp_c_1.name,tmp_c_2.user_cnt
from media_resources.tb_album tmp_c_1 inner join(
select tmp_b.create_date,tmp_a.name ,tmp_b.radioid,tmp_b.user_cnt
from ( select id,name from media_resources.tb_cms_catalog group by id,name) tmp_a join 
(select count(distinct udid) as user_cnt,create_date,max(source_type) as source_type,radioid from kaola_stat.user_event_stream_day where create_date>='2015-05-11' and create_date<='2015-05-17' and (event_code='300009' or event_code='300010' or event_code='300024') and source='200009' group by radioid,create_date ) tmp_b
on(tmp_a.id = tmp_b.source_type)) tmp_c_2 on (tmp_c_1.id = tmp_c_2.radioid)


--时间	机型	分类	Play AU	收听时长	收听次数  日期 5.11-5.17  top50
select tmp_d.create_date,tmp_d.device_name,tmp_d.catalog_name,tmp_d.sum_user_active_play,tmp_d.sum_play_time,tmp_d.sum_play_count from (
select tmp_c.create_date,tmp_c.device_name,tmp_c.catalog_name,tmp_c.sum_user_active_play,tmp_c.sum_play_time,tmp_c.sum_play_count,dense_rank() over(partition by tmp_c.create_date,tmp_c.catalog_name order by tmp_c.sum_user_active_play desc)  as line_num
from (
select tmp_a.create_date,tmp_a.device_name,tmp_b.catalog_name,sum(tmp_b.user_active_play) as sum_user_active_play,sum(tmp_b.play_time) as sum_play_time,sum(tmp_b.play_count) as sum_play_count
from (select udid,create_date,device_name from kaola_stat.user_info_day where create_date>='2015-05-11' and create_date<='2015-05-17' group by create_date,device_name,udid) tmp_a
inner join (select  create_date,udid,catalog_name,user_active_play,play_time,play_count from kaola_stat.user_play_day where create_date>='2015-05-11' and create_date<='2015-05-17' ) tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date,tmp_a.device_name,tmp_b.catalog_name ) tmp_c )tmp_d where tmp_d.line_num<=50


--时间	机型	分类	专辑	Play AU	 收听时长	收听次数 日期 5.11-5.17  top50

select tmp_d.create_date,tmp_d.device_name,tmp_d.catalog_name,tmp_d.album_name,tmp_d.sum_user_active_play,tmp_d.sum_play_time,tmp_d.sum_play_count from (
select tmp_c.create_date,tmp_c.device_name,tmp_c.catalog_name,tmp_c.album_name,tmp_c.sum_user_active_play,tmp_c.sum_play_time,tmp_c.sum_play_count,dense_rank() over(partition by tmp_c.create_date,tmp_c.album_name order by tmp_c.sum_user_active_play desc)  as line_num
from (
select tmp_a.create_date,tmp_a.device_name,tmp_b.album_name,max(tmp_b.catalog_name) as catalog_name,sum(tmp_b.user_active_play) as sum_user_active_play,sum(tmp_b.play_time) as sum_play_time,sum(tmp_b.play_count) as sum_play_count
from (select udid,create_date,device_name from kaola_stat.user_info_day where create_date>='2015-05-11' and create_date<='2015-05-17' group by create_date,device_name,udid) tmp_a
inner join (select  create_date,udid,catalog_name,album_name,user_active_play,play_time,play_count from kaola_stat.user_play_day where create_date>='2015-05-11' and create_date<='2015-05-17' ) tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date,tmp_a.device_name,tmp_b.album_name ) tmp_c )tmp_d where tmp_d.line_num<=50




--酷派机型每月nu 数据 

日期 机型 渠道号 NU（按打开）

 按机型提取下酷派这两个项目的NU数据，按每月总数罗列，谢谢。
 
8297-T01：2014年12月，2015年1，2，3，4，5月份
8297-T02：2015年3，4，5月份
select tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel from basis_data.basis_device_info_day where create_date like '2014-12%' and device_name like '%8297-T01%' group by create_date,udid,channel
		union all
	  select udid,createdate as create_date, channel from default.basis_common where createdate like '2014-12%' and devicename like '%8297-T01%' group by createdate,udid,channel
) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '2015-05%' and user_new_open=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.channel


hive -e "
select tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel from kaola_stat.user_info_day where create_date like '2014-12%' and device_name like '%8297-T01%' group by create_date,udid,channel
) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '2014-12%' and user_new_open=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.channel
"> kupaiT01_201412.txt


2014年12月1日  到   2015年5月1日 

日期 机型 渠道号 NU（按打开）

select '${month}',tmp_a.device_name,tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel,device_name from kaola_stat.user_info_day where create_date like '${month}%' and (
device_name ='Coolpad 8297-T01' or device_name='Coolpad+8297-T01' or device_name='Coolpad 8297-T02'
) group by create_date,udid,channel,device_name
) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '${month}%' and user_new_open=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.create_date,tmp_a.device_name,tmp_a.channel




select tmp_a.create_date,tmp_a.device_name,tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel,device_name from basis_data.basis_device_info_day where  create_date>='2014-12-01' and create_date<='2014-12-15'  group by create_date,udid,channel,device_name
union all
select udid,createdate as create_date, channel,devicename as device_name from default.basis_common where createdate>='2014-12-01' and createdate<='2014-12-15'   group by createdate,udid,channel,devicename
) tmp_a 
join (select udid,createdate as create_date from default.user_open_day where createdate>='2014-12-01' and createdate<='2014-12-15' and userisnew=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.create_date,tmp_a.device_name,tmp_a.channel





select tmp_a.create_date,tmp_a.device_name,tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel,device_name from kaola_stat.user_info_day where create_date like '${month}%'  group by create_date,udid,channel,device_name
) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '${month}%' and user_new_open=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.create_date,tmp_a.device_name,tmp_a.channel


--渠道H_daliand1 ,H_fanyue,H_dksj1的累计新增用户数，魅族累积新增用户数（1月-6月）

select tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel from kaola_stat.user_info_day where create_date like '2015-05%' and (channel='H_daliandl' or channel='H_fanyue' or channel='H-dksj1') group by create_date,udid,channel) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '2015-05%' and user_new=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.channel

select tmp_a.channel, count(tmp_a.udid)
from (select udid,create_date,channel from kaola_stat.user_info_day where create_date like '2015-05%' and (channel='H_daliandl' or channel='H_fanyue' or channel='H-dksj1') and (device_name like '%MX%' or device_name like '%meizu%') group by create_date,udid,channel) tmp_a 
join (select udid,create_date from kaola_stat.user_stat_day where create_date like '2015-05%' and user_new=1) tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.channel


--5月17日－6月17日
--时间  分类   专辑名称   收听次数   收听人数  收听时长  关注人数   完整收听率


select stat_date,catalog_name,album_name,play_count,active_play_user_count,play_time,follow_user_count,(max_play_time/max_program_length) as  listen_ratio 
from data_st.analysis_of_album_stat_client where stat_date>='2015-05-17' and stat_date<='2015-06-17' and device_type=-2 and album_name !='all'



--统计一下上周五到昨天的用户数，根据IP统计 还有次数  
 芒果TV推广 basis_activity_info;
        2.1、进入首页（100000）
        2.1、点击下载（200000）

select a.create_date,
count(distinct case when a.event_code = '100000' then  a.ip end ),
count( case when a.event_code = '100000' then a.ip end ),
count(distinct case when a.event_code = '200000' then  a.ip end ),
count( case when a.event_code = '200000' then a.ip end )
from basis_data.basis_activity_info a 
where a.create_date >='2015-07-03' and a.create_date<='2015-07-05' and (a.event_code='100000' or a.event_code='200000') and a.acid='20150701'
group by a.create_date




--日期	机型	分类	收听人数	收听次数	收听时长	人均时长（秒）  2015-06-26 2015-07-02


select tmp_c.create_date,
       tmp_c.device_name,
       tmp_c.catalog_name,
       tmp_c.sum_user_count,
       tmp_c.sum_play_count,
       tmp_c.sum_play_time,
       tmp_c.sum_play_time/tmp_c.sum_user_count
  from (select tmp_a.create_date,
               tmp_a.device_name,
               tmp_b.catalog_name,
               count(tmp_b.udid) as sum_user_count,
               sum(tmp_b.play_count) as sum_play_count,
               sum(tmp_b.play_time) as sum_play_time
          from (select udid, create_date, device_name
                  from kaola_stat.user_info_day
                where create_date= '$CURR_DAY'
                 group by create_date, device_name, udid) tmp_a
         inner join (select create_date,
                           udid,
                           catalog_name,
                           round(play_time/1000) as play_time,
                           play_count
                      from kaola_stat.user_play_day
                     where create_date= '$CURR_DAY' and play_time > 0) tmp_b on (tmp_a.udid =
                                                                 tmp_b.udid and
                                                                 tmp_a.create_date =
                                                                 tmp_b.create_date)
         group by tmp_a.create_date, tmp_a.device_name, tmp_b.catalog_name) tmp_c 
		
		
		
 select tmp_c.create_date,tmp_c.device_name,tmp_c.catalog_name,tmp_c.sum_user_count,tmp_c.sum_play_count,round(tmp_c.sum_play_time/60,1),round(tmp_c.sum_play_time/tmp_c.sum_user_count/60,1) from (
 select tmp_a.create_date,tmp_a.device_name,tmp_b.catalog_name,count(tmp_b.udid) as sum_user_count,sum(tmp_b.play_count) as sum_play_count,sum(tmp_b.play_time) as sum_play_time from (
 select tmp_a1.udid, tmp_a1.create_date, tmp_a1.device_name from (select udid,create_date,device_name from kaola_stat.user_info_day where create_date= '"+yesterday.isoformat()+"' ) tmp_a1 
 join (select udid,create_date from kaola_stat.user_stat_day where  create_date= '"+yesterday.isoformat()+"' and user_new = 1 ) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date = tmp_a2.create_date)
 group by tmp_a1.create_date, tmp_a1.device_name, tmp_a1.udid ) tmp_a 
 inner join (select create_date,udid,catalog_name,round(play_time/1000) as play_time,play_count from kaola_stat.user_play_day where create_date= '"+yesterday.isoformat()+"' and play_time > 0) tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date, tmp_a.device_name, tmp_b.catalog_name) tmp_c




 
--日期  系统	离线人数	离线次数	  2015-06-26 2015-07-02

select tmp_a.create_date,case when tmp_a.device_type=0 then 'android' when tmp_a.device_type=1 then 'IOS' end,count(distinct tmp_b.udid),sum(tmp_b.event_cnt)
from (select udid,device_type,create_date from kaola_stat.user_info_day where create_date= '$CURR_DAY') tmp_a
join (select udid,create_date,event_cnt from kaola_stat.user_event_day where create_date= '$CURR_DAY'and (event='300037' or event='300006')) tmp_b
on (tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid ) group by tmp_a.create_date,tmp_a.device_type


----新用户
select tmp_a.create_date,case when tmp_a.device_type=0 then 'android' when tmp_a.device_type=1 then 'IOS' end,count(distinct tmp_b.udid),sum(tmp_b.event_cnt) from (select tmp_a1.udid,tmp_a1.device_type,tmp_a1.create_date from ( select udid,device_type,create_date from kaola_stat.user_info_day  where create_date= '$CURR_DAY' ) tmp_a1 join (select udid,create_date from kaola_stat.user_stat_day  where create_date= '$CURR_DAY' and  user_new=1 ) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date = tmp_a2.create_date)) tmp_a join (select udid,create_date,event_cnt from kaola_stat.user_event_day where create_date= '$CURR_DAY' and (event='300037' or event='300006')) tmp_b on (tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid ) group by tmp_a.create_date,tmp_a.device_type

--时间	版本	系统	热词	人数	次数 
select tmp_c.create_date,tmp_c.version,case when tmp_c.device_type=0 then 'android' when tmp_c.device_type=1 then 'IOS' end,tmp_c.result,count(distinct tmp_c.udid),count(tmp_c.udid)
from (
select tmp_b.result,tmp_a.create_date,tmp_a.device_type,tmp_a.version,tmp_a.udid from 
(select udid,device_type,create_date,version from basis_data.basis_device_info_day where create_date= '$CURR_DAY') tmp_a
join (select udid,create_date,result from basis_data.basis_event where create_date= '$CURR_DAY' and event_code='300029' and type='0') tmp_b 
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid)
union all
 select eventinfo as result,createdate as create_date,devicetype as device_type,version,udid from default.basis_common where createdate='$CURR_DAY' and eventcode='300029' and remarks=0
 ) tmp_c group by tmp_c.create_date,tmp_c.device_type,tmp_c.result,tmp_c.version
 
 
--日期 累计																						
----收听人数	收听次数	累计时长	人均时长		
--点播
----收听人数	收听次数	收听时长	完整收听率	新增关注人数	累计关注人数	点赞次数	离线次数	分享次数	
--直播
----收听人数	最高在线人数	收听时长	预订人数	人均收听时长
--get all verison data

select create_date,total_play_user_cnt,total_play_times, round(total_play_time/60,1),round(total_play_time_per_user/60,1) ,play_user_cnt,play_times,round(play_time/60,1), complete_play_rate,follow_user_count, praise_count, down_count, share_count, online_play_user_cnt,round(online_play_time/60,1), online_book_user_cnt, round(online_play_time_per_user/60,1) from kaola_stat.content_stat_day  where create_date='$CURR_DAY' and version='-1'


CREATE EXTERNAL TABLE kaola_stat.content_stat_day (
  version string,
  total_play_user_cnt int, 
  total_play_times int, 
  total_play_time bigint, 
  total_play_time_per_user float, 
  play_user_cnt int, 
  play_times int, 
  play_time bigint, 
  complete_play_rate float, 
  follow_user_count int, 
  total_follow_user_count int, 
  praise_count int, 
  down_count int, 
  share_count int, 
  online_play_user_cnt int, 
  online_max_online_user_cnt int, 
  online_play_time bigint, 
  online_book_user_cnt int, 
  online_play_time_per_user float
  )
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/kaola_stat/content_stat_day'

  --first when init the table ,you must get  the total_follow_user_count per version , now ignore !!!! 
  --select tmp_a.version,sum(tmp_b.follow_user_count)
  -- from ( select udid,create_date,version from kaola_stat.user_info_day where create_date like '2015-01%' and version='4.0.0') tmp_a
  -- join (select udid,follow_user_count,create_date from kaola_stat.user_play_day where create_date like '2015-06%' ) tmp_b
  --on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.version
 
  
  --get the general data 
insert overwrite table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') select 
tmp_a.version,0,0,0,0,count(distinct tmp_b.udid),sum(tmp_b.play_count),round(sum(tmp_b.play_time)/1000),sum(tmp_b.max_play_time)/sum(tmp_b.time_length),sum(tmp_b.follow_user_count),0,sum(tmp_b.praise_count),sum(tmp_b.down_count),sum(tmp_b.share_count),0,0,0,0,0
from (select udid,create_date,version from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_a
join (select udid,create_date,play_count,play_time,max_play_time,time_length,follow_user_count,praise_count,down_count,share_count from kaola_stat.user_play_day where create_date='$CURR_DAY') tmp_b
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) group by tmp_a.version

--get all version general data

insert into table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') select 
-1,0,0,0,0,count(distinct tmp_b.udid),sum(tmp_b.play_count),round(sum(tmp_b.play_time)/1000),sum(tmp_b.max_play_time)/sum(tmp_b.time_length),sum(tmp_b.follow_user_count),0,sum(tmp_b.praise_count),sum(tmp_b.down_count),sum(tmp_b.share_count),0,0,0,0,0
from (select udid,create_date from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_a
join (select udid,create_date,play_count,play_time,max_play_time,time_length,follow_user_count,praise_count,down_count,share_count from kaola_stat.user_play_day where create_date='$CURR_DAY') tmp_b
on(tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) 



--get the total_follow_user_count,now ignore
--insert overwrite table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') 
--select tmp_a.version,tmp_a.total_play_user_cnt,tmp_a.total_play_times,tmp_a.total_play_time,tmp_a.total_play_time_per_user,tmp_a.play_user_cnt,tmp_a.play_times,tmp_a.play_time,tmp_a.complete_play_rate
--,tmp_a.follow_user_count,(tmp_a.follow_user_count+ if(tmp_b.total_follow_user_count is null ,0 ,tmp_b.total_follow_user_count)),tmp_a.praise_count,tmp_a.down_count,tmp_a.share_count,tmp_a.online_play_user_cnt,tmp_a.online_max_online_user_cnt,
--tmp_a.online_play_time,tmp_a.online_book_user_cnt,tmp_a.online_play_time_per_user from (select * from kaola_stat.content_stat_day where create_date='$CURR_DAY') tmp_a 
--left join (select version,total_follow_user_count from kaola_stat.content_stat_day where create_date='$DAY_BEFORE_ONE' ) tmp_b on (tmp_a.version = tmp_b.version)

--get the online data 

insert overwrite table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') select 
tmp_a.version,tmp_a.total_play_user_cnt,tmp_a.total_play_times,tmp_a.total_play_time,tmp_a.total_play_time_per_user,tmp_a.play_user_cnt,tmp_a.play_times,tmp_a.play_time,tmp_a.complete_play_rate
,tmp_a.follow_user_count,tmp_a.total_follow_user_count,tmp_a.praise_count,tmp_a.down_count,tmp_a.share_count,tmp_b.online_play_user_cnt,tmp_b.online_max_online_user_cnt,
tmp_b.online_play_time,tmp_b.online_book_user_cnt,tmp_b.online_play_time_per_user from (select * from kaola_stat.content_stat_day where create_date='$CURR_DAY') tmp_a
left join (
select '-1' as version,sum(play_user_cnt) as online_play_user_cnt ,sum(max_online_user_cnt) as online_max_online_user_cnt,sum(play_duration) as online_play_time,sum(book_user_cnt) as online_book_user_cnt,sum(play_duration)/sum(play_user_cnt) as online_play_time_per_user
from kaola_stat.live_stat_daily_report where stat_date='$CURR_DAY' 
) tmp_b on (tmp_a.version = tmp_b.version);



-- get the total data

insert  overwrite table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') select 
tmp_a.version,tmp_b.total_play_user_cnt,tmp_b.total_play_times,(tmp_a.play_time+if(tmp_a.online_play_time is null ,0,tmp_a.online_play_time)),((tmp_a.play_time+if(tmp_a.online_play_time is null ,0,tmp_a.online_play_time))/tmp_b.total_play_user_cnt),tmp_a.play_user_cnt,tmp_a.play_times,tmp_a.play_time,tmp_a.complete_play_rate
,tmp_a.follow_user_count,tmp_a.total_follow_user_count,tmp_a.praise_count,tmp_a.down_count,tmp_a.share_count,tmp_a.online_play_user_cnt,tmp_a.online_max_online_user_cnt,
tmp_a.online_play_time,tmp_a.online_book_user_cnt,tmp_a.online_play_time_per_user
from (select * from kaola_stat.content_stat_day where create_date='$CURR_DAY') tmp_a
left join (
select tmp_b1.version,count(distinct tmp_b1.udid) as total_play_user_cnt,sum( tmp_b2.event_cnt) as total_play_times
from (select udid,create_date,version from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_b1
join (select udid,event,event_cnt,create_date from kaola_stat.user_event_day where create_date='$CURR_DAY' and (event='101010' or event='101015')) tmp_b2 
on ( tmp_b1.udid = tmp_b2.udid and tmp_b1.create_date = tmp_b2.create_date ) group by tmp_b1.version
) tmp_b on (tmp_a.version = tmp_b.version);


--get all version totaldata

insert  into table kaola_stat.content_stat_day partition(create_date='$CURR_DAY') select 
tmp_a.version,tmp_b.total_play_user_cnt,tmp_b.total_play_times,(tmp_a.play_time+if(tmp_a.online_play_time is null ,0,tmp_a.online_play_time)),((tmp_a.play_time+if(tmp_a.online_play_time is null ,0,tmp_a.online_play_time))/tmp_b.total_play_user_cnt),tmp_a.play_user_cnt,tmp_a.play_times,tmp_a.play_time,tmp_a.complete_play_rate
,tmp_a.follow_user_count,tmp_a.total_follow_user_count,tmp_a.praise_count,tmp_a.down_count,tmp_a.share_count,tmp_a.online_play_user_cnt,tmp_a.online_max_online_user_cnt,
tmp_a.online_play_time,tmp_a.online_book_user_cnt,tmp_a.online_play_time_per_user
from (select * from kaola_stat.content_stat_day where create_date='$CURR_DAY' and version ='-1' ) tmp_a
left join (
select '-1' as version ,count(distinct tmp_b1.udid) as total_play_user_cnt,sum( tmp_b2.event_cnt) as total_play_times
from (select udid,create_date from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_b1
join (select udid,event_cnt,create_date from kaola_stat.user_event_day where create_date='$CURR_DAY' and (event='101010' or event='101015')) tmp_b2 
on ( tmp_b1.udid = tmp_b2.udid and tmp_b1.create_date = tmp_b2.create_date )
) tmp_b on (tmp_a.version = tmp_b.version);


--merge all data
insert  overwrite table kaola_stat.content_stat_day partition(create_date='$CURR_DAY')select 
tmp_a.version,max(tmp_a.total_play_user_cnt),max(tmp_a.total_play_times),max(tmp_a.total_play_time),max(tmp_a.total_play_time_per_user),max(tmp_a.play_user_cnt),max(tmp_a.play_times),max(tmp_a.play_time),max(tmp_a.complete_play_rate)
,max(tmp_a.follow_user_count),max(tmp_a.total_follow_user_count),max(tmp_a.praise_count),max(tmp_a.down_count),max(tmp_a.share_count),max(tmp_a.online_play_user_cnt),max(tmp_a.online_max_online_user_cnt),
max(tmp_a.online_play_time),max(tmp_a.online_book_user_cnt),max(tmp_a.online_play_time_per_user) from kaola_stat.content_stat_day tmp_a  where  create_date='$CURR_DAY' group by tmp_a.version  







19 个字段  累加 4个 点播 9个  直播5个

  
		
--搜索排名	日期	专辑	分类	收听次数	收听时长(秒)	收听人数	人均时长(秒)	完整收听率 （搜索前50名 round(play_time/1000)

select search_rank,create_date,album_name,catalog_name,play_times,play_time,play_user_cnt,play_time_per_user,complete_play_rate from kaola_stat.search_stat_day where create_date='$CURR_DAY' and search_rank<=50 order by search_rank asc;

CREATE EXTERNAL TABLE kaola_stat.search_stat_day (
  search_rank int,
  album_id bigint,
  album_name string,
  catalog_id bigint,
  catalog_name string,
  play_times int,
  play_time bigint,
  play_user_cnt  int,
  play_time_per_user float,
  complete_play_rate float
  )
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/kaola_stat/search_stat_day'
  
--搜索排名，专辑id,收听次数，收听人数
insert overwrite table kaola_stat.search_stat_day partition(create_date='$CURR_DAY') 
select dense_rank() over(partition by tmp_c.create_date order by tmp_c.play_times desc) as search_rank,tmp_c.radioid,'',0,'',tmp_c.play_times,0,tmp_c.play_user_cnt,0.0,0.0
 from ( select tmp_a.create_date,tmp_b.radioid,count(tmp_b.udid) as play_times,count(distinct tmp_b.udid) as play_user_cnt
from (select udid,create_date from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_a
join (select udid,radioid,create_date from kaola_stat.user_event_stream_day where create_date='$CURR_DAY'  and event_code='101013' and source='200008' and radioid like '11%' ) tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date,tmp_b.radioid ) tmp_c 

--专辑名称，分类id,收听时长，完整收听率
insert into table kaola_stat.search_stat_day partition(create_date='$CURR_DAY') 
select 0,tmp_a1.radioid,max(tmp_a2.album_name) as album_name,max(tmp_a2.catalog_id) as catalog_id,'',0,sum(tmp_a1.play_time) as play_time,0,0,
sum(case when tmp_a1.max_play_time>cast (round(tmp_a2.time_length/1000) as bigint) then cast(round(tmp_a2.time_length/1000) as bigint) else tmp_a1.max_play_time end)/round(sum(tmp_a2.time_length)/1000) as complete_play_rate 
from (select c.catalog_id,c.id album_id,c.name album_name,d.id audio_id,d.time_length from media_resources.tb_album c inner join media_resources.tb_album_audio d on ( c.id =d.album_id))tmp_a2 join (
select tmp_a.create_date,tmp_a.radioid,tmp_a.audioid,sum(tmp_b.play_time) as play_time,max(tmp_b.play_time) as max_play_time
from (select playid,radioid,audioid,create_date from kaola_stat.user_event_stream_day where create_date='$CURR_DAY' and event_code='101013' and source='200008' and radioid like '11%') tmp_a
join (select playid,play_time,create_date from basis_data.basis_event where create_date='$CURR_DAY' and event_code='101010' and radioid like '11%') tmp_b
on (tmp_a.playid = tmp_b.playid and tmp_a.create_date = tmp_b.create_date ) group by tmp_a.create_date,tmp_a.radioid,tmp_a.audioid
) tmp_a1 on (tmp_a1.radioid = tmp_a2.album_id and tmp_a1.audioid = tmp_a2.audio_id) group by tmp_a1.radioid,tmp_a1.create_date


insert into table kaola_stat.search_stat_day partition(create_date='" + yesterday.isoformat() + "') select 0,tmp_a1.radioid,max(tmp_a2.album_name) as album_name,max(tmp_a2.catalog_id) as catalog_id,'',0,
sum(tmp_a1.play_time) as play_time,0,0, sum(case when tmp_a1.max_play_time>cast (round(tmp_a2.time_length/1000) as bigint) then cast(round(tmp_a2.time_length/1000) as bigint) else tmp_a1.max_play_time end)/round(sum(tmp_a2.time_length)/1000) 
as complete_play_rate,1 from (
select c.catalog_id,c.id album_id,c.name album_name,d.id audio_id,d.time_length from media_resources.tb_album c inner join media_resources.tb_album_audio d on ( c.id =d.album_id))tmp_a2 
join ( 
select tmp_a.create_date,tmp_a.radioid,tmp_a.audioid,sum(tmp_b.play_time) as play_time,max(tmp_b.play_time) as max_play_time from 
(select tmp_a12.playid,tmp_a12.radioid,tmp_a12.audioid,tmp_a12.create_date from 
(select udid,create_date from kaola_stat.user_stat_day where create_date='" + yesterday.isoformat() + "' and user_new = 1) tmp_a11 join 
(select playid,radioid,audioid,create_date,udid from kaola_stat.user_event_stream_day where create_date='" + yesterday.isoformat() + "' and event_code='101013' and source='200008' and radioid like '11%') tmp_a12
on (tmp_a11.udid = tmp_a12.udid and tmp_a11.create_date = tmp_a12.create_date)
) tmp_a 
join (
select playid,play_time,create_date from basis_data.basis_event where create_date='" + yesterday.isoformat() + "' and event_code='101010' and radioid like '11%') 
tmp_b on (tmp_a.playid = tmp_b.playid and tmp_a.create_date = tmp_b.create_date 
) group by tmp_a.create_date,tmp_a.radioid,tmp_a.audioid ) tmp_a1 on (tmp_a1.radioid = tmp_a2.album_id and tmp_a1.audioid = tmp_a2.audio_id) group by tmp_a1.radioid,tmp_a1.create_date

--group by album_id
insert overwrite table kaola_stat.search_stat_day partition(create_date='$CURR_DAY') 
select max(search_rank),album_id,max(album_name),max(catalog_id),max(catalog_name),max(play_times),max(play_time),max(play_user_cnt),max(play_time_per_user),max(complete_play_rate),data_type
from kaola_stat.search_stat_day where create_date='$CURR_DAY' group by album_id,data_type

--分类 name ,人均时长(秒)

insert overwrite table kaola_stat.search_stat_day partition(create_date='$CURR_DAY') 
select tmp_a.search_rank,tmp_a.album_id, tmp_a.album_name,tmp_a.catalog_id,tmp_b.name as catalog_name,tmp_a.play_times,tmp_a.play_time,tmp_a.play_user_cnt,tmp_a.play_time_per_user,tmp_a.complete_play_rate
from (select search_rank,album_id,album_name,catalog_id,catalog_name,play_times,play_time,play_user_cnt,(play_time/play_user_cnt) as play_time_per_user,complete_play_rate from kaola_stat.search_stat_day where  create_date='$CURR_DAY') tmp_a 
left join media_resources.tb_catalog tmp_b on (tmp_a.catalog_id=tmp_b.id)



 
 
select tmp_e.search_rank,tmp_e.create_date,tmp_f.album_name,tmp_f.catalog_name,tmp_f.play_count,round(tmp_f.play_time/1000),tmp_f.play_user_cnt,round(tmp_f.play_time_per_user/1000),tmp_f.compelte_listen_rate
from (
select create_date,radioid,search_cnt,search_rank from (
select tmp_c.create_date,tmp_c.radioid,tmp_c.search_cnt,dense_rank() over(partition by tmp_c.create_date order by tmp_c.search_cnt desc) as search_rank
 from ( select tmp_a.create_date,tmp_b.radioid,count(tmp_b.udid) as search_cnt
from (select udid,create_date,version from kaola_stat.user_info_day where create_date='$CURR_DAY') tmp_a
join (select udid,radioid,playid,create_date from kaola_stat.user_event_stream_day where create_date='$CURR_DAY'  and event_code='101013' and source='200008' and radioid like '11%' ) tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date,tmp_b.radioid ) tmp_c ) tmp_d where tmp_d.search_rank<= 50 ) tmp_e
join (
select * from (
select tmp_a1.create_date,tmp_a2.album_id,max(tmp_a2.album_name) as album_name,max(tmp_a2.catalog_name) as catalog_name,sum(tmp_a2.play_count) as play_count,
sum(tmp_a2.max_play_time) as play_time,count(distinct tmp_a2.udid) as play_user_cnt,sum(tmp_a2.max_play_time)/count(distinct tmp_a2.udid) as play_time_per_user,sum(tmp_a2.max_play_time)/sum(tmp_a2.time_length) as compelte_listen_rate 
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-06-26' and create_date<='2015-06-26' and version>='4.0.0') tmp_a1
join (select udid,create_date,catalog_name,album_id,album_name,play_count,max_play_time,time_length from kaola_stat.user_play_day where create_date>='2015-06-26' and create_date<='2015-07-02') tmp_a2
on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date = tmp_a2.create_date) group by tmp_a1.create_date,tmp_a2.album_id
 ) tmp_f1
) tmp_f on (tmp_e.create_date=tmp_f.create_date and tmp_e.radioid=tmp_f.album_id) order by tmp_e.search_rank asc;






日期	地域（到省） DAU（Android）	DAU（IOS）	DAU（累计）

select temp_c.create_date,a.areaname,temp_c.android_dau,temp_c.ios_dau,temp_c.dau
from  (select areaname,areacode from default.code_area where parentareacode=156 and areacode !='156000000')  a left join (
select temp_a.create_date,min(temp_a.area_code) as area_code,count( distinct case when temp_a.device_type=0 then temp_b.udid end ) as android_dau,
count(distinct case when temp_a.device_type=1 then temp_b.udid end ) as ios_dau,count(distinct temp_b.udid) as dau
from 
(select udid,create_date,area_code,device_type from kaola_stat.user_info_day where create_date='$CURR_DAY' and length(area_code)=6) temp_a 
left join (select udid,create_date from kaola_stat.user_stat_day where create_date='$CURR_DAY' and user_active='1') temp_b
on ( temp_a.udid = temp_b.udid and temp_a.create_date = temp_b.create_date ) group by substr(temp_a.area_code,0,2),temp_a.create_date
) temp_c on (a.areacode = temp_c.area_code) 


日期	地域（到市） NU	AU
select temp_c.stat_date,a.areacode,a.areaname,temp_c.nu,temp_c.au
from (select areaname,areacode from default.code_area where arearank=2)  a inner join (
select stat_date,area_code,sum(new_user_count) as nu,sum(active_user_count) as au from data_st.analysis_of_area_client 
where stat_date>='2015-08-06' and stat_date<='2015-08-12' group by stat_date,area_code
) temp_c on (a.areacode = temp_c.area_code) 

--用户拥有哪些角色和权限(有效)
用户名称 用户id 角色id 角色名称 权限id 权限名称

select tmp_b1.username,tmp_b1.id,tmp_b1.role_id,tmp_b1.role_name,tmp_b3.privilege_name,tmp_b3.operation_code   
from (
select tmp_a1.username,tmp_a1.id,tmp_a1.role_id,tmp_a2.role_name
from (
select tmp_a.username,tmp_a.id,tmp_b.role_id
from user tmp_a left join user_role_mapping tmp_b 
on (tmp_a.id = tmp_b.user_id and  tmp_b.isvalid=0) 
where tmp_a.isvalid=0 and tmp_a.username='root'
) tmp_a1
left join role tmp_a2 on (tmp_a1.role_id = tmp_a2.id and tmp_a2.isvalid=0)
) tmp_b1 left join role_privilege_mapping tmp_b2 on (tmp_b1.role_id = tmp_b2.role_id and tmp_b2.isvalid=0) left join privilege tmp_b3 
on (tmp_b2.privilege_id = tmp_b3.id and tmp_b3.isvalid=0) ;


--专辑统计信息3.18~4.18(分类，收听次数，收听人数，收听时长，关注人数，完整收听率)
select stat_date,catalog_name,album_name,play_count,active_play_user_count,play_time,follow_user_count,(max_play_time/max_program_length) as  listen_ratio 
from data_st.analysis_of_album_stat_client where stat_date>='2015-05-17' and stat_date<='2015-06-17' and device_type=-2 and album_name !='all'


--分类：	资讯						资讯						情感						等等
日期	收听人数	收听次数	收听时长	关注人数	完整收听率	次日留存率	收听人数	收听次数	收听时长	关注人数	完整收听率	次日留存率	收听人数	收听次数	收听时长	关注人数	完整收听率	次日留存率


select tmp_a.create_date,max(tmp_b.catalog_name) as catalog_name,count(distinct tmp_a.udid) as play_user_cnt,sum(tmp_b.play_count) as play_cnt,round(sum(tmp_b.play_time),2) as play_time,
sum(tmp_b.follow_user_count) as follow_user_cnt,count(distinct case when tmp_b.remain_1_days_play=1 then tmp_a.udid end)/count(distinct tmp_a.udid),
sum(tmp_b.remain_1_days_play)/sum(tmp_b.play_count)
from (select udid,create_date from kaola_stat.user_stat_day where create_date='2015-07-13' and user_new=1) tmp_a
join (
select create_date,udid,play_count,catalog_id,catalog_name,play_time/60000 as play_time,follow_user_count,time_length/60000 as time_length,remain_1_days_play from kaola_stat.user_play_day where create_date='2015-07-13'
) tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date=tmp_b.create_date) group by tmp_a.create_date,tmp_b.catalog_id


select tmp_a1.udid, tmp_a1.create_date,tmp_a1.version,tmp_a1.device_type from (select udid,create_date,device_type,version from kaola_stat.user_info_day where create_date= '"+yesterday.isoformat()+"' ) tmp_a1 
 join (select udid,create_date from kaola_stat.user_stat_day where  create_date= '"+yesterday.isoformat()+"' and user_new = 1 ) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date = tmp_a2.create_date)


select tmp_c.create_date,tmp_c.version,case when tmp_c.device_type=0 then 'android' when tmp_c.device_type=1 then 'IOS' end,
tmp_c.result,count(distinct tmp_c.udid),count(tmp_c.udid) from ( 
select tmp_b.result,tmp_a.create_date,tmp_a.device_type,tmp_a.version,tmp_a.udid from (
select tmp_a1.udid, tmp_a1.create_date,tmp_a1.version,tmp_a1.device_type from (select udid,create_date,device_type,version from kaola_stat.user_info_day where create_date= '"+yesterday.isoformat()+"' ) tmp_a1 
 join (select udid,create_date from kaola_stat.user_stat_day where  create_date= '"+yesterday.isoformat()+"' and user_new = 1 ) tmp_a2 on (tmp_a1.udid = tmp_a2.udid and tmp_a1.create_date = tmp_a2.create_date) ) tmp_a 
join (
select udid,create_date,result from basis_data.basis_event where create_date= '" + yesterday.isoformat() + "' and event_code='300029' and type='0') tmp_b on (tmp_a.create_date = tmp_b.create_date and tmp_a.udid = tmp_b.udid) 
union all 
select eventinfo as result,createdate as create_date,devicetype as device_type,version,udid from default.basis_common where createdate='" + yesterday.isoformat() + "' and eventcode='300029' and remarks=0 ) tmp_c 
group by tmp_c.create_date,tmp_c.device_type,tmp_c.result,tmp_c.version



 
 --日期	  Android			IOS					累计		
--     MNU	MOU	 MAU	MNU	MOU	MAU			MNU	MOU	MAU 
select count(case when tmp_c.device_type=0 and tmp_c.user_new_or_active = 1 then tmp_c.udid end ) as android_mnu,
count(case when tmp_c.device_type=0 and tmp_c.user_new_or_active = 0 then tmp_c.udid end ) as android_mou,
count(case when tmp_c.device_type=0 then tmp_c.udid end ) as android_mau,
count(case when tmp_c.device_type=1 and tmp_c.user_new_or_active = 1 then tmp_c.udid end ) as iso_mnu,
count(case when tmp_c.device_type=1 and tmp_c.user_new_or_active = 0 then tmp_c.udid end ) as ios_mou,
count(case when tmp_c.device_type=1 then tmp_c.udid end ) as ios_mau,
count(case when tmp_c.user_new_or_active = 1 then tmp_c.udid end ) as mnu,
count(case when tmp_c.user_new_or_active = 0 then tmp_c.udid end ) as mou,
count(tmp_c.udid) as mau
from (
select tmp_b.udid,tmp_a.device_type ,max(tmp_b.user_new_or_active) as user_new_or_active
from (select udid,device_type,create_date from kaola_stat.user_info_day where create_date like '2015-10%' ) tmp_a
join (select udid,case when user_new = 1 then 1 when user_active = 1 and user_new = 0 then 0 end as user_new_or_active,create_date 
from kaola_stat.user_stat_day where create_date like '2015-10%') tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.udid,tmp_a.device_type ) tmp_c;

日期	 android_mnu	android_mou	android_mau	ios_mnu	ios_mou	ios_mau	累计_mnu	累计_mou	累计_mau
2015-10	776268	696017	1472285	49011	144007	193018	825279	840024	1665303
2015-09	800089	729120	1529209	46594	152874	199468	846695	881995	1728690
2015-08	785773	808849	1594622	78885	162190	241075	864663	971040	1835703
2015-07	732978	816120	1549098	43765	174828	218593	776745	990949	1767694

 
 
 
  --日期	 MNU	MOU	 MAU  	渠道：A-baidu A-360 A-myapp appstore
select '2015-09',tmp_c.channel, count( distinct case when tmp_c.user_new_or_active = 1 then tmp_c.udid end ) as mnu,
count( distinct case when  tmp_c.user_new_or_active = 0 then tmp_c.udid end ) as mou,
count( distinct tmp_c.udid) as mau
from (
select tmp_b.udid,tmp_a.channel ,max(tmp_b.user_new_or_active) as user_new_or_active
from (select udid,channel,create_date from kaola_stat.user_info_day where create_date like '2015-09%' and (channel='A-baidu' or channel='A-360' or channel='A-myapp' or channel='appstore' ) ) tmp_a
join (select udid,case when user_new = 1 then 1 when user_active = 1 and user_new = 0 then 0 end as user_new_or_active,create_date 
from kaola_stat.user_stat_day where create_date like '2015-09%') tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.udid,tmp_a.channel ) tmp_c group by tmp_c.channel;



select '$month',tmp_c.channel, count( distinct case when tmp_c.user_new_or_active = 1 then tmp_c.udid end ) as mnu,
count( distinct case when  tmp_c.user_new_or_active = 0 then tmp_c.udid end ) as mou,
count( distinct tmp_c.udid) as mau from ( select tmp_b.udid,tmp_a.channel ,max(tmp_b.user_new_or_active) as user_new_or_active 
from (select udid,channel,create_date from kaola_stat.user_info_day where create_date like '${month}%' and (channel='A-baidu' or channel='A-360' or channel='A-myapp' or channel='appstore' ) ) tmp_a 
join (select udid,case when user_new = 1 then 1 when user_active = 1 and user_new = 0 then 0 end as user_new_or_active,create_date from kaola_stat.user_stat_day where create_date like '${month}%') tmp_b on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.udid,tmp_a.channel ) tmp_c group by tmp_c.channel;

2015-08
日期	 android_mnu	android_mou	android_mau	ios_mnu	ios_mou	ios_mau	累计_mnu	累计_mou	累计_mau
2015-07	732978	816120	1549098	43765	174828	218593	776745	990949	1767694

日期	渠道	MNU	MOU	MAU
2015-01	A-360	27809	31335	59144
2015-01	A-baidu	84430	56057	140487
2015-01	A-myapp	39650	21161	60811
2015-01	appstore	78416	94994	173410
2015-02	A-360	43368	26446	69814
2015-02	A-baidu	55578	48081	103659
2015-02 A-myapp 31077   20810   51887
2015-02 appstore        45392   87690   133082

日期	渠道	MNU	MOU	MAU
2015-08 A-360	23264	28795	52059
2015-08	A-baidu	74516	58713	133229
2015-08	A-myapp	38411	31299	69710
2015-08	appstore	78201	125527	203728
2015-09	A-360	20507	26537	47044
2015-09	A-baidu	67205	59796	127001
2015-09	A-myapp	34833	31137	65970
2015-09	appstore	46287	122942	169229


 --搜索 排名 前100  搜索次数 搜索人数
 
 select temp_c.create_date ,temp_c.rank_seq,temp_c.result,temp_c.search_cnt,temp_c.search_user_cnt from (
 select temp_a.create_date,temp_a.result,sum(temp_a.search_cnt) as search_cnt,sum(temp_a.search_user_cnt) as search_user_cnt,dense_rank() over(order by sum(temp_a.search_cnt) desc) as rank_seq
from (
	select create_date,result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from basis_data.basis_event where 
	create_date='" + yesterday.isoformat() + "' and event_code='300029' and result is not null and result != 'null'  group by create_date,result
	union all
	select createdate as create_date,eventinfo as result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from default.basis_common where 
	 createdate = '" + yesterday.isoformat() + "' and eventcode='300026' and eventinfo is not null and eventinfo != 'null' group by createdate,eventinfo
) temp_a group by temp_a.create_date,temp_a.result ) temp_c where temp_c.rank_seq <=100
 
 
select temp_c.create_date ,temp_c.rank_seq,temp_c.result,temp_c.search_cnt,temp_c.search_user_cnt from ( select temp_a.create_date,temp_a.result,sum(temp_a.search_cnt) as search_cnt,sum(temp_a.search_user_cnt) as search_user_cnt,dense_rank() over(order by sum(temp_a.search_cnt) desc) as rank_seq from ( select create_date,result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from basis_data.basis_event where  create_date='" + yesterday.isoformat() + "' and event_code='300029' and result is not null and result != 'null'  group by create_date,result union all select createdate as create_date,eventinfo as result,count(udid) as search_cnt,count(distinct udid) as search_user_cnt from default.basis_common where  createdate = '" + yesterday.isoformat() + "' and eventcode='300026' and eventinfo is not null and eventinfo != 'null' group by createdate,eventinfo ) temp_a group by temp_a.create_date,temp_a.result ) temp_c where temp_c.rank_seq <=100
 
 
--日期	厂商	机型	NU	Play NU	DAU	Play AU	总时长(秒)	人均时长（秒）	完整收听率

select tmp_a.create_date,tmp_a.producer,tmp_a.device_name,sum(tmp_b.user_new),sum(tmp_b.user_new_play),sum(tmp_b.user_active),sum(tmp_b.user_active_play),sum(tmp_b.play_time),sum(tmp_b.play_time)/count(tmp_a.udid),sum(tmp_b.play_time)/sum(tmp_b.program_length)
from (select udid,create_date,producer,device_name from basis_data.basis_device_info_day where create_date='' and appid='10001' ) tmp_a 
join (
select udid,create_date,user_new,user_new_play,user_active,user_active_play,play_time,program_length from kaola_stat.user_stat_day where create_date = '' and appid='10001'
) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date ,tmp_a.producer,tmp_a.device_name


insert  overwrite table kaola_stat.device_stat_day partition(create_date='" + yesterday.isoformat() + "') select tmp_a.create_date,tmp_a.producer,tmp_a.device_name,sum(tmp_b.user_new),sum(tmp_b.user_new_play),sum(tmp_b.user_active),sum(tmp_b.user_active_play),sum(tmp_b.play_time),sum(tmp_b.play_time)/count(tmp_a.udid),sum(tmp_b.play_time)/sum(tmp_b.program_length) from (select udid,create_date,producer,device_name from basis_data.basis_device_info_day where create_date='" + yesterday.isoformat() + "' and appid='10001' ) tmp_a join ( select udid,create_date,user_new,user_new_play,user_active,user_active_play,play_time,program_length from kaola_stat.user_stat_day where create_date = '" + yesterday.isoformat() + "' and appid='10001') tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date ,tmp_a.producer,tmp_a.device_name

CREATE EXTERNAL TABLE kaola_stat.device_stat_day (
  producer string,
  device_name string,
  nu int,
  play_nu int,
  dau int,
  play_au int,
  play_time int,
  play_time_per_user  int,
  complete_play_rate float
  )
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/kaola_stat/device_stat_day'


--日期	按DAUTOP20机型	分类	收听次数	收听时长(秒)	完整收听率

select tmp_c.create_date,tmp_c.device_name,tmp_c.catalog_name,tmp_c.sum_play_count,tmp_c.sum_play_time,tmp_c.sum_play_time/tmp_c.sum_time_length
from (select tmp_a.create_date,tmp_a.device_name,tmp_b.catalog_name,sum(tmp_b.play_count) as sum_play_count,sum(tmp_b.play_time) as sum_play_time,sum(tmp_b.time_length) as sum_time_length
from (select tmp_a2.udid, tmp_a2.create_date, tmp_a2.device_name from (
select tmp.* from( select device_name ,create_date,rank() over(order by dau desc) as rank_seq from kaola_stat.device_stat_day where create_date = '" + yesterday.isoformat() + "' ) tmp where tmp.rank_seq <= 20) tmp_a1 
join (select udid, create_date, device_name from  kaola_stat.user_info_day where create_date= '" + yesterday.isoformat() + "') tmp_a2 
on (tmp_a1.device_name = tmp_a2.device_name and tmp_a1.create_date = tmp_a2.create_date)) tmp_a
inner join (select create_date,udid,catalog_name,round(play_time/1000) as play_time,play_count,round(time_length/1000)  as time_length from kaola_stat.user_play_day where create_date= '" + yesterday.isoformat() + "' and play_time > 0) tmp_b on (tmp_a.udid =
tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date, tmp_a.device_name, tmp_b.catalog_name) tmp_c 
		
		
		
select tmp_c.create_date,tmp_c.device_name,tmp_c.catalog_name,tmp_c.sum_play_count,tmp_c.sum_play_time,tmp_c.sum_play_time/tmp_c.sum_time_lengthfrom (select tmp_a.create_date,tmp_a.device_name,tmp_b.catalog_name,sum(tmp_b.play_count) as sum_play_count,sum(tmp_b.play_time) as sum_play_time,sum(tmp_b.time_length) as sum_time_length from (select tmp_a2.udid, tmp_a2.create_date, tmp_a2.device_name from ( select tmp.* from( select device_name ,create_date,rank() over(order by dau desc) as rank_seq from kaola_stat.device_stat_day where create_date = '" + yesterday.isoformat() + "' ) tmp where tmp.rank_seq <= 20) tmp_a1  join (select udid, create_date, device_name from  kaola_stat.user_info_day where create_date= '" + yesterday.isoformat() + "') tmp_a2  on (tmp_a1.device_name = tmp_a2.device_name and tmp_a1.create_date = tmp_a2.create_date)) tmp_a inner join (select create_date,udid,catalog_name,round(play_time/1000) as play_time,play_count,round(time_length/1000)  as time_length from kaola_stat.user_play_day where create_date= '" + yesterday.isoformat() + "' and play_time > 0) tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date, tmp_a.device_name, tmp_b.catalog_name) tmp_c 







select '"+yesterday.isoformat()+"', a.name, case when a.link_type=0 then '无' when a.link_type=1 then '内容资源' when a.link_type=2 then '网页链接--考拉FM内部打开' when a.link_type=8 then '网页链接--外部浏览器打开' end, 
b.splash_cnt,b.splash_user_cnt, b.splash_cnt / b.launch_cnt, b.splash_click_cnt, b.splash_click_user_cnt,b.splash_click_cnt / b.splash_cnt,
(case when instr(a.platform, '0') > 0 and instr(a.platform, '1') > 0 then 'IOS、Android' when instr(a.platform, '0') > 0 then 'Android' when instr(a.platform, '1') > 0 then 'IOS' end),from_unixtime(cast(a.valid_start_date as bigint)), 
 show_time_length from (select * from media_resources.tb_splash_screen) a join (select temp_a.eventid,temp_b.launch_cnt,temp_a.splash_cnt,temp_a.splash_user_cnt,temp_a.splash_click_cnt,temp_a.splash_click_user_cnt               
from (select count(udid) as launch_cnt from basis_data.basis_event where create_date = '"+yesterday.isoformat()+"' and event_code = '100010') temp_b join (SELECT eventid, count((case when event_code = '100011' then udid end)) as splash_cnt, 
count(distinct(case when event_code = '100011' then udid end)) as splash_user_cnt, count((case when event_code = '300000' then udid end)) as splash _click_cnt, count(distinct(case when event_code = '300000' then udid end)) as splash_click_user_cnt 
FROM basis_data.basis_event where create_date = '"+yesterday.isoformat()+"' and (event_code = '100011' or event_code = '300000') group by eventid) temp_a) b on (a.id = b.eventid) order by b.splash_cnt desc
		
		
--2015-07-29 新用户 日期 启动人数	接收推送人数	点击推送人数	接收闪屏人数	点击闪屏人数
select '2015-07-29',
count(distinct case when tmp_b.event = '100010' then tmp_b.udid end ),
count(distinct case when tmp_b.event = '101011' then tmp_b.udid end ),
count(distinct case when tmp_b.event = '300015' then tmp_b.udid end ),
count(distinct case when tmp_b.event = '100011' then tmp_b.udid end ),
count(distinct case when tmp_b.event = '300000' then tmp_b.udid end )
from (select udid from kaola_stat.user_stat_day where create_date ='2015-07-29' and user_new = 1 group by udid) tmp_a 
join (
select udid,event from kaola_stat.user_event_day where create_date = '2015-07-29' and (event = '100010' or event ='101011' or event ='300015' or event ='100011' or event ='300000')
) tmp_b
on(tmp_a.udid = tmp_b.udid)

日期	启动人数	接收推送人数	点击推送人数	接收闪屏人数	点击闪屏人数
2015-07-29	26492	8221	794	16939	7



-- 版本4+ 新用户 注册人数	 登录人数	收听人数	收藏人数(赞)	订阅人数（关注）	离线人数	分享人数	定时人数	搜索人数	启动人数	接收推送人数	点击推送人数	接收闪屏人数	点击闪屏人数

select tmp_b.create_date,
count(distinct case when tmp_b.event_code = '400002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '400003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300024'  then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '400007' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '400005' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300006' or tmp_b.event_code='300037' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300008' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300040' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300029' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '100010' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '101011' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300015' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '100011' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '300000' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200014' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200001' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200007' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200010' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200020' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200009' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200003' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200002' then tmp_b.udid end ),
count(distinct case when tmp_b.event_code = '200006' then tmp_b.udid end )
from (select tmp_a1.udid,tmp_a1.create_date from (select udid,create_date from kaola_stat.user_info_day where create_date ='2015-10-19' and version like '4.2%' group by udid,create_date) tmp_a1
join 
(select udid,create_date from kaola_stat.user_stat_day where create_date ='2015-10-19' and user_new = 1 group by udid,create_date) tmp_a2
on(tmp_a1.create_date = tmp_a2.create_date and tmp_a1.udid = tmp_a2.udid)
)tmp_a 
join (
select udid,event_code,create_date from basis_data.basis_event where create_date = '2015-10-19' 
and (event_code='400002' or event_code='400003' or  event_code='300024' or event_code='400007' or event_code='400005' or event_code='300006' or event_code='300037' or event_code='300008' or event_code='300040' or event_code='300029' or event_code = '100010' or event_code ='101011' or event_code ='300015' or event_code ='100011' or event_code ='300000' 
or event_code='200014'  or event_code='200001' or event_code='200007' or event_code='200010' or event_code='200020' or event_code='200009' or event_code='200003' or event_code='200002' or event_code='200006')
) tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date)
group by tmp_b.create_date

日期	注册人数	 登录人数	收听人数	收藏人数(赞)	订阅人数（关注）	离线人数	分享人数	定时人数	搜索人数	启动人数	接收推送人数	点击推送人数	接收闪屏人数	点击闪屏人数	进入发现页人数	我的电台	离线页	设置页	精选页	全部页	详情页	播放器页	收听历史人数		
2015-10-19	0	0	4553	365	686	885	92	262	5177	13498	2944	554	4293	0	10621	3998	3538	2842	4062	2259	4521	6397	1170
2015-10-19	0	0	4553	365	686	885	92	262	5177	13498	2944	554	4293	0
2015-07-29      0       13      4609    529     884     1062    123     258     4400    12427   4830    637     12055   0

日期	闪屏接收人数
2015-09-23      3790
2015-09-24      4830
2015-09-25      5235
2015-09-26      4314
2015-09-27      4359
2015-10-10      3557
2015-10-11      4058
2015-10-12      3727
2015-10-13      3603
2015-10-14      3698
2015-10-15      3770
2015-10-16      3871
2015-10-17      4230
2015-10-18      4333

 create_date >='2015-07-28'  and create_date <= '2015-09-08'
 
select tmp_b.create_date,
count(distinct case when tmp_b.event_code = '100011' then tmp_b.udid end )
from (select tmp_a1.udid,tmp_a1.create_date from (select udid,create_date from kaola_stat.user_info_day where create_date >='2015-08-18'  and create_date <= '2015-09-08' and version like '4%' group by udid,create_date) tmp_a1
join 
(select udid,create_date from kaola_stat.user_stat_day where create_date >='2015-08-18'  and create_date <= '2015-09-08' and user_new = 1 group by udid,create_date) tmp_a2
on(tmp_a1.create_date = tmp_a2.create_date and tmp_a1.udid = tmp_a2.udid)
)tmp_a 
join (
select udid,event_code,create_date from basis_data.basis_event where create_date >='2015-08-18'  and create_date <= '2015-09-08'
and (event_code='100011')
) tmp_b
on(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date)
group by tmp_b.create_date



-- create  tmp_basis_car

CREATE EXTERNAL TABLE tmp_basis_car(
  appid string, 
  event_code string, 
  udid string, 
  uid string, 
  installid string, 
  sessionid string, 
  playid string, 
  page string, 
  time string, 
  action string, 
  network string, 
  operator string, 
  lon string, 
  lat string, 
  imsi string, 
  speed string, 
  type string, 
  radioid bigint, 
  audioid bigint, 
  play_time bigint, 
  flow string, 
  request_agent string, 
  request_referer string, 
  remarks1 string, 
  remarks2 string, 
  remarks3 string, 
  remarks4 string, 
  remarks5 string, 
  ip bigint, 
  area_code int, 
  create_time string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs://ks01:8020/hive/basis_data/tmp_basis_car'





--banner 点击人数 点击次数


select create_date,refer,eventid,count(udid),count(distinct udid) from basis_data.basis_event 
where create_date='"+yesterday.isoformat()+"' and event_code='300016' and refer !='null' and areatag=33 
and refer is not null group by create_date,refer,eventid;




/usr/local/hadoop/bin/hadoop fs -rm -r /hive/media_resources/tb_feature

/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://10.10.4.34/media_resources --username dev --password dev@2014 --query "SELECT id,name,content_num,type_id,content_type,status FROM tb_feature WHERE \$CONDITIONS" --split-by id --fields-terminated-by '\t' --append --target-dir /hive/media_resources/tb_feature



select tmp_a.create_date,tmp_a.version,case when tmp_a.device_type=0 then 'android' when tmp_a.device_type=1 then 'IOS' else 'UNKOWN' end,tmp_a.refer,case when tmp_a.page='200014' then '发现页' when tmp_a.page='200020' then '分类精选页' end,tmp_b.name,tmp_a.n,tmp_a.user_n
from (
select t_a.create_date,t_a.refer,t_a.radioid,t_a.page,t_b.version,t_b.device_type,count(t_a.udid) as n,count(distinct t_a.udid) as user_n from 
(select udid,create_date,event_code,refer,case when radioid is null then eventid when (eventid is null or eventid = 'null') then cast(radioid as string) end as radioid,page from basis_data.basis_event where create_date='2015-08-17' and event_code='300016' and refer !='null' and refer is not null and (areatag=33 or areatag=67)) t_a 
join (select udid,version,device_type,create_date from kaola_stat.user_info_day where create_date='2015-08-17' group by create_date,udid,version,device_type) t_b on(t_a.udid = t_b.udid and t_a.create_date = t_b.create_date) 
group by t_a.create_date,t_a.refer,t_a.radioid,t_a.page,t_b.version,t_b.device_type ) tmp_a left join(
select id,title as name from  media_resources.tb_album_audio union all select id,name from media_resources.tb_album union all select id,name from media_resources.tb_radio
union all select id,name from media_resources.tb_feature
) tmp_b on (tmp_a.radioid = tmp_b.id);


select tmp_a.create_date,tmp_a.refer,case when tmp_a.page='200014' then '发现页' when tmp_a.page='200020' then '分类精选页' end,tmp_b.name,tmp_a.n,tmp_a.user_n from ( select create_date,refer,radioid,page,count(udid) as n,count(distinct udid) as user_n from basis_data.basis_event where create_date='"+yesterday.isoformat()+"' and event_code='300016' and refer !='null' and (areatag=33 or areatag=67) and refer is not null group by create_date,refer,radioid,page ) tmp_a left join( select id,title as name from  media_resources.tb_album_audio union all select id,name from media_resources.tb_album union all select id,name from media_resources.tb_radio union all select id,name from media_resources.tb_feature) tmp_b on (tmp_a.radioid = tmp_b.id)

焦点呈现栏目banner	帧数  点击次数 点击人数
2015-08-06      6       67      59
2015-08-06      1       1613    1311
2015-08-06      2       2106    1824
2015-08-06      3       773     668
2015-08-06      4       994     836
2015-08-06      5       671     579


焦点呈现栏目banner	帧数  点击次数 点击人数
2015-08-12	3	890	794
2015-08-12	4	740	647
2015-08-12	5	593	516
2015-08-12	6	139	117
2015-08-12	1	1783	1571
2015-08-12	2	1029	907

select create_date,areatag,count(udid),count(distinct udid)
from basis_data.basis_event where create_date='2015-08-06' and event_code='300016' and refer !='null' and refer is not null group by create_date,areatag;

banner	帧数  点击次数 点击人数
2015-08-06          6       67      59
2015-08-06          1       3887    2906
2015-08-06          2       3400    2819
2015-08-06          3       1662    1393
2015-08-06          4       1852    1546
2015-08-06          5       917     781



1.我的电台把订阅（300043）和收藏的一键播放（300033）分拆统计。（下一版本确认）


select create_date,event,sum(event_cnt),count(distinct udid)
from kaola_stat.user_event_day where create_date='2015-08-09' and (event='300043' or event='300033') group by create_date , event;

日期	事件	次数	人数
2015-08-09      300033(收藏)  6786    1394
2015-08-09      300043(订阅)  922     632

2.进入搜索页的人数、次数—>搜索人数、次数—>搜索结果播放人数、播放次数（出统计脚本）

select tmp_a.create_date,tmp_b.event,tmp_a.version,sum(tmp_b.event_cnt),count(distinct tmp_b.udid)
from (select udid,version,create_date from user_info_day where create_date='2015-08-09') tmp_a join (
select event,udid,event_cnt,create_date from kaola_stat.user_event_day where create_date='2015-08-09' and (event='200008' or event='300029')) tmp_b
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date ,tmp_b.event,tmp_a.version;


select tmp_a.version,tmp_b.event_code,count(tmp_b.udid),count(distinct tmp_b.udid)
from (select udid,version from basis_data.basis_device_info_day where create_date ='2015-08-09' group by udid,version) tmp_a
join (select event_code,udid from basis_data.basis_event where create_date='2015-08-09' and (event_code='200008' or event_code='300029')) tmp_b
on(tmp_a.udid = tmp_b.udid)
 group by tmp_b.event_code,tmp_a.version;
 

2015-08-09      16120   32392   39427   219314  2261    3923
select create_date,
count(distinct case when event_code='200008' then udid end),
count(case when event_code='200008' then udid end),
count(distinct case when event_code='300029' then udid end),
count( case when event_code='300029' then udid end),
count(distinct case when (event_code='300024' and (page='200008' or page_prev='200008')) or (event_code='300010' and page='200008') then udid end),
count(case when (event_code='300024' and (page='200008' or page_prev='200008')) or (event_code='300010' and page='200008') then udid end)
from kaola_stat.user_event_stream_day where create_date='2015-08-09' and (event_code='200008' or event_code ='300029' or event_code='300024' or event_code='300010') group by create_date;



2015-08-09      16120   32392   9278    47798  2887    7855
select create_date,
count(distinct case when event_code='200008' then udid end),
count(case when event_code='200008' then udid end),
count(distinct case when event_code='300029' and (page = '200008' or page_prev='200008') then udid end),
count( case when event_code='300029' and (page = '200008' or page_prev='200008') then udid end),
count(distinct case when event_code='101013' and page_prev='200008' then udid end),
count(case when event_code='101013' and page_prev='200008' then udid end)
from kaola_stat.user_event_stream_day where create_date='2015-08-09' and (event_code='200008' or event_code ='300029' or event_code='101013' ) group by create_date;




select count(case when tmp_a.event_code='200008' then tmp_a.udid end),count(distinct case when tmp_a.event_code='200008' then tmp_a.udid end),count( case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end),count(distinct case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end)  from (select udid,event_code,page,page_prev,create_date from kaola_stat.user_event_stream_day where create_date='$CURR_DAY' and (event_code='200008' or event_code ='300029')) tmp_a join (SELECT udid,channel,version,device_type FROM kaola_stat.user_info_day WHERE create_date='$CURR_DAY' and version>='4.0.0') tmp_b on (tmp_a.udid = tmp_b.udid)


4.增加无搜索结果的关键词的上报（下一版本确认） 未上报

5.精选页栏目运营位的点击数据上报（下一版本确认） 已上报

6.专辑详情页的播放全部按钮（确认是否已有）点击播放事件未上报

7.播放器的播放列表按钮数据统计（人数、次数）（出统计脚本）

select create_date,sum(event_cnt),count(distinct udid)
from kaola_stat.user_event_day where create_date='2015-08-09' and event='200015' group by create_date ;

日期	次数	人数
2015-08-09      52577   15833

8.发现页运营位时段数据统计。（暂时不出）







 SELECT '2015-08-10',-1,0,0,0,0,0,0,0,0,0,0,0,0,
 COUNT((CASE WHEN a.event_code='200001' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.event_code='200001' THEN a.udid END)),
 COUNT((CASE WHEN a.page_prev='200001' AND a.event_code='300024' AND a.source_type!='2' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.page_prev='200001' AND a.event_code='300024' AND a.source_type!='2' THEN a.udid END)),
 COUNT((CASE WHEN a.page_prev='200001' AND a.event_code='300024' AND a.source_type='2' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.page_prev='200001' AND a.event_code='300024' AND a.source_type='2' THEN a.udid END)),
 COUNT((CASE WHEN a.page_prev='200001' AND a.event_code='300024' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.page_prev='200001' AND a.event_code='300024' THEN a.udid END)),
 COUNT((CASE WHEN a.page_prev='200001' AND a.event_code='300043' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.page_prev='200001' AND a.event_code='300043' THEN a.udid END)),
 COUNT((CASE WHEN a.event_code='200006' THEN a.udid END)),
 COUNT(DISTINCT (CASE WHEN a.event_code='200006' THEN a.udid END)),0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 FROM (SELECT udid,event_code,page_prev,source,source_type FROM kaola_stat.user_event_stream_day WHERE create_date='2015-08-10' AND event_code is not null AND (event_code='200001' or event_code='200006' or page_prev='200001')) a JOIN (SELECT udid,channel,version,device_type FROM kaola_stat.user_info_day WHERE create_date='2015-08-10' and version>='4.0.0') b ON (a.udid=b.udid)



SELECT COUNT((CASE WHEN event_code='200014' THEN udid END)),
COUNT(DISTINCT (CASE WHEN event_code='200014' THEN udid END)),
COUNT((CASE WHEN (page='200014' AND (event_code='300016')) or (page='200019' AND (event_code='200019')) or (page_prev='200014' and (event_code='300016' or event_code='200003' or event_code='200009' or event_code='200020' or event_code='200011' or event_code='200012' or event_code='200019' or event_code='200021')) THEN udid END)),
COUNT(DISTINCT (CASE WHEN (page='200014' AND (event_code='300016')) or (page='200019' AND (event_code='200019')) or (page_prev='200014' and (event_code='300016' or  event_code='200003' or event_code='200009' or event_code='200020' or event_code='200011' or event_code='200012' or event_code='200019' or event_code='200021')) THEN udid END))
 FROM kaola_stat.user_event_stream_day WHERE create_date='2015-08-09' AND event_code is not null
 
 
 


SELECT COUNT((CASE WHEN event_code='200014' THEN udid END)),
COUNT(DISTINCT (CASE WHEN event_code='200014' THEN udid END)),
COUNT((CASE WHEN (page='200014' AND event_code='300016') OR 
((event_code='200009' OR event_code='200020') and page_prev='200014' AND event_prev!='300016') 
OR (event_code='200011' and event_prev!='300016') OR (event_code='200019' and areatag='43') OR ((event_code='200011' OR event_code='200012') AND page_prev='200014')
OR (event_code='200019' AND areatag=167) THEN udid END)),
COUNT( DISTINCT (CASE WHEN (page='200014' AND event_code='300016') OR 
((event_code='200009' OR event_code='200020') and page_prev='200014' AND event_prev!='300016') 
OR (event_code='200011' and event_prev!='300016') OR (event_code='200019' and areatag='43') OR ((event_code='200011' OR event_code='200012') AND page_prev='200014')
OR (event_code='200019' AND areatag=167) THEN udid END))
FROM kaola_stat.user_event_stream_day WHERE create_date='2015-08-11' AND event_code is not null;



 
PV UV  下载数  下载人数
 http://m.kaolafm.com/plays.html?oid=1100000012730&channel=weixinchuanbo&refer=
时间 8月11日
 s
 

select create_date,count(case when event_code='100000' then udid end ) ,count(distinct case when event_code='100000' then udid end),
count( case when event_code='1005' then udid end ),count( distinct case when event_code='1005' then udid end )
from basis_data.basis_html where (create_date='2015-10-17' or create_date ='2015-10-18') and request_referer = 'http://www.kaolafm.com/mobile.html?channel=H_nhduanzi' group by create_date;
 
 2015-08-11      4       3       0       0
 
PV UV  下载数  下载人数
2015-10-17      158376  1       0       0
2015-10-18      733     1       0       0
 
select create_date,count(udid) ,count(distinct udid),count( case when event_code='1005' then udid end ),count( distinct case when event_code='1005' then udid end )
from basis_data.basis_html where create_date='2015-08-11' and request_referer like 'http://m.kaolafm.com/plays.html?oid=1100000012730&channel=weixinchuanbo&refer=%' group by create_date;
2015-08-11      51      25      0       0

--pv uv
select create_date,count(case when event_code='100000' then udid end ) ,count(distinct case when event_code='100000' then udid end)
from basis_data.basis_html where create_date='2015-08-12' and channel='weixinchuanbo' and page='3000' group by create_date;
--下载数	下载人数
select count(udid),count(distinct udid)  from basis_down where create_date='2015-08-12' and request_referer='http://m.kaolafm.com/plays.html?oid=1100000012730&channel=weixinchuanbo&refer=';
日期	PV	UV	下载数  下载人数
2015-08-12	48	31	0	0
2015-08-11	30	25	0	0


--导航页分析
CREATE EXTERNAL TABLE test_db.analysis_of_page_client(
  stat_date string, 
  data_type int, 
  pv_200014 int, 
  uv_200014 int, 
  click_count_200014 int, 
  click_user_count_200014 int, 
  pv_200020 int, 
  uv_200020 int, 
  click_count_200020 int, 
  click_user_count_200020 int, 
  pv_200009 int, 
  uv_200009 int, 
  click_count_200009 int, 
  click_user_count_200009 int, 
  pv_200001 int, 
  uv_200001 int, 
  subscribe_count_200001 int, 
  subscribe_user_count_200001 int, 
  collection_count_200001 int, 
  collection_user_count_200001 int, 
  total_count_200001 int, 
  total_user_count_200001 int, 
  play_count_200001 int, 
  play_user_count_200001 int, 
  history_count int, 
  history_user_count int, 
  pv_200007 int, 
  uv_200007 int, 
  play_count_200007 int, 
  play_user_count_200007 int, 
  pv_200003 int, 
  uv_200003 int, 
  play_count_200003 int, 
  play_user_count_200003 int, 
  subscribe_count_200003 int, 
  subscribe_user_count_200003 int, 
  collection_count_200003 int, 
  collection_user_count_200003 int, 
  share_count_200003 int, 
  share_user_count_200003 int, 
  down_all_count_200003 int, 
  down_all_user_count_200003 int, 
  pv_200002 int, 
  uv_200002 int, 
  down_count_200002 int, 
  down_user_count_200002 int, 
  subscribe_count_200002 int, 
  subscribe_user_count_200002 int, 
  collection_count_200002 int, 
  collection_user_count_200002 int, 
  share_count_200002 int, 
  share_user_count_200002 int, 
  play_order_count int, 
  play_order_user_count int, 
  more_count_200002 int, 
  more_user_count_200002 int, 
  info_count_200002 int, 
  info_count_user_200002 int, 
  timing_count_200002 int, 
  timing_user_count_200002 int, 
  bell_count_200002 int, 
  bell_user_count_200002 int, 
  pv_200010 int, 
  uv_200010 int, 
  click_count_200010 int, 
  click_user_count_200010 int,
  pv_200008 int,
  uv_200008 int,
  search_count_200008 int ,
  search_user_count_200008 int
  )
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/test_db/analysis_of_page_client'
 
1、播放器页面加入 播放列表页点击次数、人数；
2、加入搜索页的 pv uv 搜索人数 搜索次数；

alter table test_db.analysis_of_page_client ADD COLUMNS( pv_200008 int,uv_200008 int,search_count_200008 int ,search_user_count_200008 int);


select tmp_b.create_date,tmp_a.version,sum(tmp_b.event_cnt),count(distinct tmp_a.udid) from (select udid,version,create_date from kaola_stat.user_info_day where create_date>='2015-08-10' and create_date<='2015-08-16' and device_type='1' ) tmp_a 
join (select udid,event,event_cnt,create_date from kaola_stat.user_event_day where create_date>='2015-08-10' and create_date<='2015-08-16' and event='400000') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.version,tmp_b.create_date



select tmp_b.create_date,sum(tmp_b.event_cnt),count(distinct tmp_a.udid) from (select udid,version,create_date from kaola_stat.user_info_day where create_date>='2015-08-13' and create_date<='2015-08-15' and device_type='1' ) tmp_a 
join (select udid,event,event_cnt,create_date from kaola_stat.user_event_day where create_date>='2015-08-13' and create_date<='2015-08-15' and event='400000') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date



select tmp_b.create_date,tmp_a.version,count(tmp_b.udid),count(distinct tmp_b.udid) from (select udid,version,create_date from kaola_stat.user_info_day where create_date>='2015-08-14' and create_date<='2015-08-19' and device_type='0' and version >='4.0.0' group by udid,version,create_date) tmp_a 
join (select udid,create_date,message from basis_data.basis_error where create_date>='2015-08-14' and create_date<='2015-08-19' ) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date,tmp_a.version


select tmp_b.create_date,tmp_a.version,tmp_b.udid,tmp_b.message from (select udid,version,create_date from kaola_stat.user_info_day where create_date>='2015-08-13' and create_date<='2015-08-16' and device_type='1' group by udid,version,create_date) tmp_a 
join (select udid,create_date,message from basis_data.basis_error where create_date>='2015-08-13' and create_date<='2015-08-16') tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) where tmp_a.version >='3.6.1' limit 300;



select tmp_a.create_date ,count(udid) from kaola_stat.user_info_day where  create_date>='2015-08-13' and create_date<='2015-08-15' and device_type='1' group by create_date;




select tmp_a.create_date,tmp_a.version,count(distinct case when tmp_b.user_new=1 then tmp_b.udid end),count(distinct case when tmp_b.user_active=1 then tmp_b.udid end),count(distinct case when tmp_b.user_new_play=1 then tmp_b.udid end)
from (select udid ,create_date,version  from kaola_stat.user_info_day where  create_date>='2015-08-12' and create_date<='2015-08-16' and device_type=1) tmp_a 
join (select udid,user_new,user_active,user_new_play,create_date from kaola_stat.user_stat_day where create_date>='2015-08-12' and create_date<='2015-08-16' ) tmp_b on 
(tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_a.create_date,tmp_a.version



渠道	人数
H-shijilong1 1410
H_sinafy4 1

explain INSERT INTO TABLE test_db.analysis_of_page_client SELECT '$CURR_DAY',1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,count(case when tmp_a.event_code='200008' then tmp_a.udid end),count(distinct case when tmp_a.event_code='200008' then tmp_a.udid end),count( case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end),count(distinct case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end) FROM (SELECT udid,event_code,page,page_prev,source FROM kaola_stat.user_event_stream_day WHERE create_date='$CURR_DAY' AND event_code is not null AND (event_code='200008' or event_code='300029')) tmp_a JOIN (SELECT t1.* FROM (SELECT udid,channel,version,device_type FROM kaola_stat.user_info_day WHERE create_date='$CURR_DAY' and version>='4.0.0') t1 JOIN (SELECT udid,user_new_open,user_new_play,user_new FROM kaola_stat.user_stat_day WHERE create_date='$CURR_DAY' and user_new=1) t2 ON (t1.udid=t2.udid)) tmp_b ON (tmp_a.udid=tmp_b.udid)




select tmp_a.create_date,count(tmp_a.udid) from (select udid,create_date,version from kaola_stat.user_info_day   where create_date >='2015-08-10' and create_date<='2015-08-19' ) tmp_a join (
select udid,event,event_cnt,create_date from kaola_stat.user_event_day where create_date >='2015-08-15' and create_date<='2015-08-19' and event='101013') tmp_b on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) 
group by tmp_a.create_date
















INSERT INTO TABLE test_db.analysis_of_page_client SELECT '$CURR_DAY',-1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,COUNT(case when tmp_a.event_code='200008' then tmp_a.udid end),count(distinct case when tmp_a.event_code='200008' then tmp_a.udid end),count( case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end),count(distinct case when tmp_a.event_code='300029' and (tmp_a.page = '200008' or tmp_a.page_prev='200008') then tmp_a.udid end)  from (select udid,event_code,page,page_prev,create_date from kaola_stat.user_event_stream_day where create_date='$CURR_DAY' and (event_code='200008' or event_code ='300029')) tmp_a join (SELECT udid,channel,version,device_type FROM kaola_stat.user_info_day WHERE create_date='$CURR_DAY' and version>='4.0.0') tmp_b on (tmp_a.udid = tmp_b.udid)


--收听直播的新用户的人数
select count(distinct tmp_a.udid),sum(tmp_b.event_cnt)
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_new=1 group by udid) tmp_a join (
select udid,event,event_cnt from kaola_stat.user_event_day where create_date = '2015-08-25' and event='101015') tmp_b on (tmp_a.udid = tmp_b.udid)

--今天（8月25日）新增用户收听直播的新用户的人数，和每个小时的新用户，增长时段（按小时）。
select tmp_b.hour,count(distinct case when tmp_b.event_code='101015' then tmp_a.udid end) ,count(distinct tmp_a.udid)
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_new=1 group by udid) tmp_a join (
select udid,hour,event_code from basis_data.basis_event where create_date = '2015-08-25') tmp_b on (tmp_a.udid = tmp_b.udid) group by tmp_b.hour

--创建临时表存储：各个小时的新增用户
create table test_db.tmp_user_hour_stat as select '0' as data_type,tmp_d.hour,tmp_d.udid from (select tmp_c.hour,tmp_c.udid from (select tmp_b.hour,tmp_b.udid,dense_rank() over(partition by tmp_b.udid order by tmp_b.hour) as rank_seq
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_new=1 group by udid) tmp_a join (
select udid,hour from basis_data.basis_event where create_date = '2015-08-25' and (event_code='100010' or event_code='101010')) tmp_b on (tmp_a.udid = tmp_b.udid) 
group by tmp_b.hour,tmp_b.udid ) tmp_c where tmp_c.rank_seq = 1 ) tmp_d

select tmp_d.hour,count(distinct tmp_e.udid),count(distinct tmp_d.udid) from test_db.tmp_user_hour_stat as tmp_d  
left join (select udid,hour from basis_data.basis_event where create_date='2015-08-25' and event_code='101015' group by udid,hour) tmp_e 
on(tmp_d.hour = tmp_e.hour and tmp_d.udid = tmp_e.udid) 
group by tmp_d.hour



--今天（8月25日）新增用户进入直播间（刘诗诗和逆袭两个直播间）的用户数（按小时）
select tmp_b.hour,case when tmp_b.eventid='1455203112' then '考拉现场' when tmp_b.eventid='1419005201' then '柴鸡蛋逆袭真爱QQ群的私人电台' end ,count(distinct tmp_a.udid),count(tmp_b.udid)
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_new=1 group by udid) tmp_a join (
select udid,hour,eventid from basis_data.basis_event where create_date = '2015-08-25' and event_code='101015' and (eventid='1455203112' or eventid='1419005201')) tmp_b on (tmp_a.udid = tmp_b.udid) group by tmp_b.hour,tmp_b.eventid


select tmp_e.hour,case when tmp_e.eventid='1455203112' then '考拉现场' when tmp_e.eventid='1419005201' then '柴鸡蛋逆袭真爱QQ群的私人电台' end ,count(distinct tmp_e.udid)
from test_db.tmp_user_hour_stat as tmp_d  join (
select udid,hour,eventid from basis_data.basis_event where create_date='2015-08-25' and event_code='101015' and (eventid='1455203112' or eventid='1419005201') group by udid,hour,eventid) tmp_e 
on (tmp_d.hour = tmp_e.hour and tmp_d.udid = tmp_e.udid) group by tmp_e.hour,tmp_e.eventid

--今天（8月25日）活跃用户进入直播间的用户数（按小时）
select tmp_b.hour,count(distinct tmp_a.udid),count(tmp_b.udid)
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_active=1 group by udid) tmp_a join (
select udid,hour from basis_data.basis_event where create_date = '2015-08-25' and event_code='101015') tmp_b on (tmp_a.udid = tmp_b.udid) group by tmp_b.hour

--创建临时表
create table test_db.tmp_user_hour_new as select '1' as data_type,tmp_d.hour,tmp_d.udid from (select tmp_c.hour,tmp_c.udid from (select tmp_b.hour,tmp_b.udid,dense_rank() over(partition by tmp_b.udid order by tmp_b.hour) as rank_seq
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_active=1 group by udid) tmp_a join (
select udid,hour from basis_data.basis_event where create_date = '2015-08-25' and (event_code='100010' or event_code='101010')) tmp_b on (tmp_a.udid = tmp_b.udid) 
group by tmp_b.hour,tmp_b.udid ) tmp_c where tmp_c.rank_seq = 1 ) tmp_d


select tmp_d.hour,count(distinct tmp_e.udid) ,count(distinct tmp_d.udid)
from test_db.tmp_user_hour as tmp_d left join (
select udid,hour from basis_data.basis_event where create_date='2015-08-25' and event_code='101015' group by udid,hour) tmp_e on 
(tmp_d.hour = tmp_e.hour and tmp_d.udid = tmp_e.udid) group by tmp_d.hour

--获取每小时的新增用户


select tmp_b.hour,count(distinct case when tmp_b.event_code='101015' then tmp_a.udid end) ,count(distinct tmp_a.udid)
from (select udid from kaola_stat.user_stat_day where create_date='2015-08-25' and user_new=1 group by udid) tmp_a join (
select udid,hour,event_code from basis_data.basis_event where create_date = '2015-08-25') tmp_b on (tmp_a.udid = tmp_b.udid) group by tmp_b.hour





select d.channel,c.coefficient,d.new_open_user_count,d.new_play_user_count,round(d.max_play_time*100/d.max_program_length,2),
round(d.remain_1_days_open*100/d.new_open_user_count,2),round(d.remain_1_days_play*100/d.new_play_user_count,2),
round(d.remain_3_days_open*100/d.new_open_user_count,2),round(d.remain_3_days_play*100/d.new_play_user_count,2),
round(d.remain_7_days_open*100/d.new_open_user_count,2),round(d.remain_7_days_play*100/d.new_play_user_count,2) 
from kaola_tj.channel_coefficient c join (select channel,device_type,sum(new_open_user_count) new_open_user_count,
sum(new_play_user_count) new_play_user_count,sum(max_play_time) max_play_time,sum(max_program_length) max_program_length,
sum(remain_1_days_open) remain_1_days_open,sum(remain_3_days_open) remain_3_days_open,sum(remain_7_days_open) remain_7_days_open,
sum(remain_1_days_play) remain_1_days_play,sum(remain_3_days_play) remain_3_days_play,sum(remain_7_days_play) remain_7_days_play 
from data_st.analysis_of_channel_stat_client where stat_date='2015-08-30' and (new_open_user_count>0 or new_play_user_count>0) 
group by channel,device_type) d on (c.channel=d.channel and c.devicetype=d.device_type);


select tmp_b.appid,tmp_a.version,tmp_a.device_type,count(distinct tmp_a.udid),count(tmp_a.udid)
from (select udid,version,device_type from basis_data.basis_device_info_day where create_date='2015-09-06' group by udid,version,device_type) tmp_a
join (select appid,udid from basis_data.basis_event where create_date='2015-09-06' and event_code='400007' ) tmp_b 
on(tmp_a.udid = tmp_b.udid) group by tmp_a.version,tmp_a.device_type,tmp_b.appid





select '10001',null,udid,null,null,b.catalog_id,null,radioid,null,radioid,b.name,audioid,
(case when event_code='400007' then 1 else 0 end),(case when event_code='400007' then 1 else 0 end)
from media_resources.tb_album b join (select appid,udid,radioid,audioid,event_code from basis_data.basis_event 
where create_date='2015-09-06' and appid='10001' and (event_code='400007')) a on a.radioid=b.id


--单栏banner 点击统计
select create_date,areatag,count(distinct udid),count(udid) from basis_data.basis_event where create_date='2015-09-07' and event_code='300016' and areatag in ('196','167','197') group by create_date,areatag;


这个是IOS的各个单栏banner的点击情况：
日期     banner名称    点击人数    点击次数
2015-09-07      单栏banner1     72      139
2015-09-07      单栏banner3     11      12
2015-09-07      单栏banner2     49      56

--直播列表页面的pv,uv
select create_date, case when type=1 then '考拉直播' when type='2' then '私人直播' end,count(udid) ,count(distinct udid) from basis_data.basis_event where create_date='2015-09-07' and event_code='200011' 
group by create_date,type;

日期	直播列表	PV	UV
2015-09-07      考拉直播        18682   10954
2015-09-07      私人直播        3164    2210

select create_date count(udid) ,count(distinct udid) from basis_data.basis_event where create_date='2015-09-07' and event_code='200011' 
group by create_date;

select tmp_a.device_type,tmp_a.version,count(tmp_a.udid),count(distinct tmp_a.udid) 
from (select udid,device_type,version from basis_device_info_day where create_date='2015-09-13') tmp_a join (
select udid from basis_event where create_date='2015-09-13' and event_code='300037' and (eventid = 'null' or eventid is null)) tmp_b
on (tmp_a.udid = tmp_b.udid) group by tmp_a.device_type,tmp_a.version


select tmp_a.device_type,tmp_a.version,count(tmp_a.udid),count(distinct tmp_a.udid) 
from (select udid,device_type,version from basis_device_info_day where create_date='2015-09-16') tmp_a join (
select udid from basis_event where create_date='2015-09-16' and (event_code='300037' or event_code='300006')) tmp_b
on (tmp_a.udid = tmp_b.udid) group by tmp_a.device_type,tmp_a.version


--评论数据 
--评论条数、评论人数、评论点赞次数、评论点赞人数 数据

select tmp_c.create_date,tmp_c.refer,tmp_c.a,tmp_c.b,tmp_d.c,tmp_d.d
from (
select create_date,eventid,refer,count(udid) as a,count(distinct udid) as b
from basis_data.basis_event where create_date='2015-09-14' and (event_code='400012')
group by create_date,eventid,refer) tmp_c left join (
select create_date,eventid,count(udid)as c,count(distinct udid) as d
from basis_data.basis_event where create_date='2015-09-14' and (event_code='400013')
group by create_date,eventid
) tmp_d on (tmp_c.eventid = tmp_d.eventid)

--各专题页面PV、UV统计
--专题页面PV、UV、播放次数、播放人数
select create_date,count(case when event_code='200021' then udid end),count(distinct case when event_code='200021' then udid end),
count(case when event_code='300024' and page='200021' then udid end),count(distinct case when event_code='300024' and page='200021' then udid end)
from basis_data.basis_event where create_date='2015-09-15' and (event_code='200021' or (event_code='300024' and page='200021')) group by create_date


--链接的PV UV 下载数 下载人数 www.kaolafm.com?channel=H_suparc
select create_date,count(case when event_code='2000' then udid end),count(distinct case when event_code='2000' then udid end),
count(case when event_code='1005' then udid end),count(distinct case when event_code='1005' then udid end)
from basis_data.basis_html where create_date='2015-09-16' and request_referer like 'http://www.kaolafm.com/?channel=H_suparc%' group by create_date;
日期	PV	UV	下载数	下载人数
2015-09-16      207     185     2       2

		
select t2.device_type,t2.channel,t2.version,t2.source,'',t1.bun,t1.bn,t2.event_code,'',t2.uun,t2.un 
from (
select device_type,channel,version,source,count(udid) bn,count(distinct udid) bun 
from test_db.tmp_page_event 
group by device_type,channel,version,source
) t1 join (
select device_type,channel,version,source,event_code,count(udid) un,count(distinct udid) uun 
from test_db.tmp_page_event 
group by device_type,channel,version,source,event_code
) t2 
on (t1.device_type=t2.device_type 
and t1.channel=t2.channel 
and t1.version=t2.version 
and t1.source=t2.source)


select device_type,channel,version,source,event_code,count(udid) un,count(distinct udid) uun 
from test_db.tmp_page_event group by event_code,version,channel,device_type,source

SELECT t1.m,t1.createdTime,t1.phoneNum,t1.orderStatus,t2.proName,t2.city FROM (select o.*,s.provinceId,s.regionId from (select MONTH(createdTime) as m,createdTime,cellNumber,HEX(cellNumber) phoneNum,(case when `status`=1 then '已下订单' when `status`=2 then '订单生效' when `status`=3 then '订单失效' else '其它' end) orderStatus from t_order  where createdTime>='"+yesterday.isoformat()+"') o LEFT JOIN t_segment s ON ((o.cellNumber>>16)=s.id)) t1 LEFT JOIN (SELECT p.id porid,r.id cityid,p.`name` proName,r.`name` city FROM t_province p,t_region r where p.id=r.provinceId) t2 ON (t1.regionId=t2.cityid);


select a.appid,a.device_type,a.udid,a.user_new_play,a.user_active_play,b.catalog_id,null,a.radio_id,null,b.album_id,b.album_name,b.audio_id,b.audio_name,(case when a.play_time> b.time_length then b.time_length else a.play_time end),(case when a.play_time> b.time_length then b.time_length else a.play_time end),b.time_length,b.time_length,a.play_count,a.follow_user_count,a.follow_count,a.praise_user_count,a.praise_count,a.down_user_count,a.down_count,b.source,a.user_active,a.play_history_7day_radio,a.other,a.share_user_count,a.share_count,a.remain_1_days_play from (select * from kaola_stat.user_play_day where create_date='2015-09-23') a inner join (select c.catalog_id,c.id album_id,c.name album_name,c.source,d.id audio_id,d.title audio_name,d.time_length from  media_resources.tb_album c inner join media_resources.tb_album_audio d on c.id=d.album_id) b on a.audio_id=b.audio_id



select  tmp_a.title,tmp_b.audio_cnt,tmp_b.audio_person from media_resources.tb_album_audio tmp_a join (
select audioid,count(udid) as audio_cnt,count(distinct udid) as audio_person from basis_data.basis_event where create_date='2015-10-06' and event_code='400007' and  page='200003' group by audioid ) tmp_b
on (tmp_a.id = tmp_b.audioid);

--分类标签300034事件说明：
	1.type 表示一级分类 result 表示二级分类 areatag 表示 areatag运营位ID
	2.如果没有二级分类，那么上报的数据中result字段有值为一级分类的名称，type无值，areatag为运营位的名称
	3.如果有二级分类，那么上报的数据中result字段的值为二级分类的名称，type为一级分类的名称，areatag为运营位的名称，比如吐槽为二级分类名称，他的一级分类名称为脱口秀
	
	
	--
	日期	时间戳	udid	事件代码	playid	page值	网络	radioid	audioid	播放时长
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-09-27' and udid = '0FCCF7B9E1810B733115AD63C884E30D' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-09-27' and udid = '1BE4801E242153CA6C4C0B8DE9B18779' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-09-28' and udid = '10734618a36252278fb9eb82700845e7' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-09-30' and udid = '1b17f011f7097ac4b816df9707927940' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-10-05' and udid = '328fe781d90622b95ab20479b1d41766' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-10-06' and udid = '743669f029bce8f0fa6e630b621cb00a' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-10-06' and udid = '899ded107b18f066b0d7ce9140c4822d' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-10-03' and udid = 'aa022855ac112b9a2226485b75c11bc0' order by timestamp
	
	select create_date,timestamp,udid,event_code,playid,page,network,radioid,audioid,play_time from basis_data.basis_event where 
	create_date='2015-09-24' and udid = 'fd67326d1374bcf771f937d6a913de26' order by timestamp
	
	
	
CREATE EXTERNAL TABLE basis_data.tmp_basis_ad(
  acid string,
  device_type string,
  rid string,
  service_name string,
  create_time string,
  ip string,
  proxy_ip string,
  url string,
  request_agent string,
  request_referer string,
  mac string,
  mac1 string,
  imei string,
  idfa string,
  androidid string,
  openuuid string,
  udid string,
  installid string
 )
PARTITIONED BY (create_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/basis_data/tmp_basis_ad';
  
  
LOAD DATA LOCAL INPATH '/data/kaolatj/ad-transform/output/20151024.log' OVERWRITE INTO TABLE basis_data.tmp_basis_ad partition(create_date='2015-10-24');
LOAD DATA LOCAL INPATH '/data/kaolatj/ad-transform/output/20151023.log' OVERWRITE INTO TABLE basis_data.tmp_basis_ad partition(create_date='2015-10-23');
LOAD DATA LOCAL INPATH '/data/kaolatj/ad-transform/output/20151025.log' OVERWRITE INTO TABLE basis_data.tmp_basis_ad partition(create_date='2015-10-25');
LOAD DATA LOCAL INPATH '/data/kaolatj/ad-transform/output/20151026.log' OVERWRITE INTO TABLE basis_data.tmp_basis_ad partition(create_date='2015-10-26');

日期范围：10月23日-10月26日
数据内容：
（1）闪屏广告：曝光数量、曝光人数、点击数量、点击人数
select create_date,
count(case when service_name='/view/s' then concat(device_type,imei,idfa) end),
count( distinct case when service_name='/view/s' then concat(device_type,imei,idfa) end),
count(case when service_name='/record/s' then concat(device_type,imei,idfa) end),
count(distinct case when service_name='/record/s' then concat(device_type,imei,idfa) end)
from basis_data.tmp_basis_ad 
where rid='3res' and ((device_type='0' and imei !='null') or (device_type='1' and idfa!='null')) 
group by create_date;
（1）闪屏广告：日期 、曝光数量、曝光人数、点击数量、点击人数
2015-10-23	23351	23348	10041	10041
2015-10-25	131806	131806	7564	7564
2015-10-24	1348455	1348455	96994	96994
2015-10-26	67	67	0	0

（2）banner广告：曝光数量、曝光人数、点击数量、点击人数
select create_date,
count(case when service_name='/view/b' then concat(device_type,imei,idfa) end),
count( distinct case when service_name='/view/b' then concat(device_type,imei,idfa) end),
count(case when service_name='/record/b' then concat(device_type,imei,idfa) end),
count(distinct case when service_name='/record/b' then concat(device_type,imei,idfa) end)
from basis_data.tmp_basis_ad 
where rid='3res' and ((device_type='0' and imei !='null') or (device_type='1' and idfa!='null')) 
group by create_date;

（2）banner广告：日期 、曝光数量、曝光人数、点击数量、点击人数
2015-10-23	0	0	0	0
2015-10-25	41	41	0	0
2015-10-24	9349	9349	0	0
2015-10-26	1384835	1384835	44557	44557

第三方	闪屏曝光	http://gg.kaolafm.com/view/s?r=3res&os=&ip=&m0=&m1=&m2=&m3=&m4=&m5=	可以返回一张固定图片
		闪屏点击	http://gg.kaolafm.com/record/s?r=3res&os=&ip=&m0=&m1=&m2=&m3=&m4=&m5=	可以返回一个固定网址
		banner曝光	http://gg.kaolafm.com/view/b?r=3res&os=&ip=&m0=&m1=&m2=&m3=&m4=&m5=	nginx根据src参数动态返回图片
		banner点击	http://gg.kaolafm.com/record/b?r=3res&os=&ip=&m0=&m1=&m2=&m3=&m4=&m5=	nginx根据src参数重定向
考拉FM	banner曝光	http://gg.kaolafm.com/view/b?r=1kao&udid=&installid=&src=	nginx根据src参数动态返回图片
		banner点击	http://gg.kaolafm.com/record/b?r=1kao&udid=&installid=&src=
		
		

select create_date,count(case when service_name='/view/b' then concat(device_type,imei,idfa) end),count( distinct case when service_name='/view/b' then concat(device_type,imei,idfa) end),
count(case when service_name='/record/b' then concat(device_type,imei,idfa) end),count(distinct case when service_name='/record/b' then concat(device_type,imei,idfa) end)
from basis_data.tmp_basis_ad where rid='3res' and ((device_type='1' and imei !='null') or (device_type='0' and idfa!='null')) group by create_date;

select create_date,hour,count(udid),count(distinct udid) from basis_data.basis_event where create_date='2015-10-26' and event_code='101013' group by hour;

00-12小时间的波峰和波谷
SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT create_date,hour,COUNT(CASE WHEN event_code='101013' THEN udid END) play_count FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour <='12' GROUP BY create_date,hour
) t) o 
WHERE o.rn in (1,2,12);

13-23小时间的波峰和波谷
SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT create_date,hour,COUNT(CASE WHEN event_code='101013' THEN udid END) play_count FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour >'12' GROUP BY create_date,hour
) t) o 
WHERE o.rn in (1,2,11);

有声小说、搞笑、情感三大类内容用户每日收听波峰、波谷时段		
有声小说 124 、搞笑 143、情感 122
media_resources.tb_album where catalog_id='122' or catalog_id='124' or catalog_id='143'	

--有声小说			
00-12小时间的波峰和波谷
SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='124') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour <='12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,12);

SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='124') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour >'12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,11);

--搞笑
SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='143') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour <='12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,12);

SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='143') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour >'12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,11);

--情感
SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='122') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour <='12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,12);

SELECT o.create_date,o.hour,o.play_count 
FROM (SELECT t.create_date,t.hour,t.play_count,ROW_NUMBER() OVER(PARTITION BY t.create_date ORDER BY t.play_count DESC) AS rn FROM (
SELECT tmp_b.create_date,tmp_b.hour,COUNT(tmp_b.udid) play_count FROM (SELECT distinct id FROM media_resources.tb_album WHERE catalog_id='122') tmp_a JOIN (
SELECT create_date,hour,udid,radioid FROM kaola_stat.user_event_stream_day WHERE create_date>='2015-10-01' AND create_date<='2015-10-26' AND event_code='101013' AND hour >'12' 
) tmp_b ON (tmp_a.id = tmp_b.radioid) GROUP BY tmp_b.create_date,tmp_b.hour
) t) o 
WHERE o.rn IN (1,2,11);





select b.udid,b.channel,b.channel_class,b.channel_type,b.device_type,b.device_name,b.version,b.os_version,b.carrier_operator,b.network,b.resolution,b.ip,b.area_code,a.login_date,a.last_date,a.first_open_date,a.first_play_date,a.create_time,a.first_open_or_play_date 
from (select udid,
from_unixtime(min(unix_timestamp(login_date,'yyyy-MM-dd')),'yyyy-MM-dd') login_date,
from_unixtime(max(unix_timestamp(last_date,'yyyy-MM-dd')),'yyyy-MM-dd')  last_date,
from_unixtime(min(unix_timestamp(first_open_date,'yyyy-MM-dd')),'yyyy-MM-dd') first_open_date,
from_unixtime(min(unix_timestamp(first_play_date,'yyyy-MM-dd')),'yyyy-MM-dd') first_play_date,
min(create_time) create_time,
from_unixtime(min(unix_timestamp(first_open_or_play_date,'yyyy-MM-dd')),'yyyy-MM-dd') first_open_or_play_date 
from kaola_stat.user_info 
group by udid) a 
inner join (
select udid,channel,channel_class,channel_type,device_type,device_name,version,os_version,carrier_operator,network,resolution,ip,area_code,login_date,last_date,first_open_date,first_play_date,create_time 
from kaola_stat.user_info) b  
on a.udid=b.udid and a.create_time=b.create_time


--优化后
set mapreduce.input.fileinputformat.split.maxsize=67108864; set mapreduce.job.reduces=30;SELECT b.udid,b.channel,b.channel_class,b.channel_type,b.device_type,b.device_name,b.version,b.os_version,b.carrier_operator,b.network,b.resolution,b.ip,b.area_code,
FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.login_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') login_date,
FROM_UNIXTIME(MAX(UNIX_TIMESTAMP(b.last_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd')  last_date,
FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.first_open_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_open_date,
FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.first_play_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_play_date,
MIN(create_time) OVER(PARTITION BY b.udid) create_time,
FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(first_open_or_play_date,'yyyy-MM-dd'))  OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_open_or_play_date 
FROM kaola_stat.user_info b;


SELECT b.udid,b.channel,b.channel_class,b.channel_type,b.device_type,b.device_name,b.version,b.os_version,b.carrier_operator,b.network,b.resolution,b.ip,b.area_code,FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.login_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') login_date,FROM_UNIXTIME(MAX(UNIX_TIMESTAMP(b.last_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd')  last_date,FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.first_open_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_open_date,FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(b.first_play_date,'yyyy-MM-dd')) OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_play_date,MIN(create_time) OVER(PARTITION BY b.udid) create_time,FROM_UNIXTIME(MIN(UNIX_TIMESTAMP(first_open_or_play_date,'yyyy-MM-dd'))  OVER(PARTITION BY b.udid),'yyyy-MM-dd') first_open_or_play_date FROM kaola_stat.user_info b;
	
		
		

CREATE EXTERNAL TABLE kaola_stat.tmp_user_info(
  udid string, 
  channel string, 
  channel_class string, 
  channel_type string, 
  device_type int, 
  device_name string, 
  version string, 
  os_version string, 
  carrier_operator int, 
  network string, 
  resolution string, 
  ip string, 
  area_code string, 
  login_date string, 
  last_date string, 
  first_open_date string, 
  first_play_date string, 
  create_time bigint, 
  first_open_or_play_date string,
  mac string,
  imei string,
  idfa string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/kaola_stat/tmp_user_info'

  

  
SELECT 
IF(u.udid is NULL,t.udid,u.udid),
IF(u.udid is NULL,t.channel,u.channel),
'','',
IF(u.udid is NULL,t.device_type,u.device_type),
IF(u.udid is NULL,t.device_name,u.device_name),
IF(u.udid is NULL,t.version,u.version),
IF(u.udid is NULL,t.os_version,u.os_version),
IF(u.udid is NULL,t.carrier_operator,u.carrier_operator),
IF(t.udid is NULL,u.network,t.network),
IF(u.udid is NULL,t.resolution,u.resolution),
IF(t.udid is NULL,u.ip,t.ip),
IF(t.udid is NULL,u.area_code,t.area_code),
IF(u.udid is NULL,t.login_date,u.login_date),
IF(t.udid is NULL,u.last_date,t.last_date),
IF(u.udid is NULL,t.first_open_date,u.first_open_date),
IF(u.udid is NULL,t.first_play_date,u.first_play_date),
IF(u.udid is NULL,t.create_time,u.create_time),
IF(u.udid is NULL,t.first_open_or_play_date,u.first_open_or_play_date)
FROM kaola_stat.user_info u FULL OUTER JOIN kaola_stat.tmp_user_info t ON (u.udid=t.udid)



insert overwrite table kaola_stat.tmp_user_info select a.udid,a.channel,a.channel_class,a.channel_type,a.device_type,a.device_name,a.version,a.os_version,a.carrier_operator,a.network,a.resolution,a.ip,a.area_code,a.create_date,a.create_date,(case when b.open>0 then a.create_date else '9999-09-09' end),(case when b.play>0 then a.create_date else '9999-09-09' end),a.create_time,(case when b.open>0 or b.play>0 then a.create_date else '9999-09-09' end),'','','' from (select * from kaola_stat.user_info_day where create_date='2015-10-29') a left join (select udid,max(case when event='101010' then 1 else 0 end) play,max(case when event='100010' then 1 else 0 end) open from kaola_stat.user_event_day where create_date='2015-10-29' and (event='101010' or event='100010') group by udid) b on a.udid=b.udid



select appid,event_code,udid,uid,installid,sessionid,playid,page,timestamp,action,network,operator,lon,lat,imsi,speed,eventid,type,result,refer,radioid,audioid,play_time,duration,starttime,endtime,request_agent,request_referer,deviceid,modelid,areatag,remarks4,remarks5,ip,area_code,create_time from basis_data.basis_event where create_date='2015-11-01' and event_code!='101011' and event_code!='101015' and event_code!='100003' and event_code!='101010' distribute by udid


CREATE EXTERNAL TABLE kaola_stat.test_user_info(
  udid string, 
  channel string, 
  channel_class string, 
  channel_type string, 
  device_type int, 
  device_name string, 
  version string, 
  os_version string, 
  carrier_operator int, 
  network string, 
  resolution string, 
  ip string, 
  area_code string, 
  login_date string, 
  last_date string, 
  first_open_date string, 
  first_play_date string, 
  create_time bigint, 
  first_open_or_play_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ks01:8020/hive/kaola_stat/test_user_info'
  
  
 
insert overwrite table kaola_stat.user_info SELECT IF(u.udid is NULL,t.udid,u.udid),IF(u.udid is NULL,t.channel,u.channel),IF(t.udid is NULL,u.channel_class,t.channel_class),'',IF(u.udid is NULL,t.device_type,u.device_type),IF(u.udid is NULL,t.device_name,u.device_name),IF(t.udid is NULL,u.version,t.version),IF(t.udid is NULL,u.os_version,t.os_version),IF(t.udid is NULL,u.carrier_operator,t.carrier_operator),IF(t.udid is NULL,u.network,t.network),IF(u.udid is NULL,t.resolution,u.resolution),IF(t.udid is NULL,u.ip,t.ip),IF(t.udid is NULL,u.area_code,t.area_code),IF(u.udid is NULL,t.login_date,u.login_date),IF(t.udid is NULL,u.last_date,t.last_date),IF(u.udid is NULL,t.first_open_date,u.first_open_date),IF(u.udid is NULL,t.first_play_date,u.first_play_date),IF(u.udid is NULL,t.create_time,u.create_time),IF(u.udid is NULL,t.first_open_or_play_date,u.first_open_or_play_date) FROM kaola_stat.user_info u FULL OUTER JOIN kaola_stat.tmp_user_info t ON (u.udid=t.udid)

	

--1.四川地区用户使用的时间段分布（需要了解用户在什么时间使用考拉最多）

select create_date,hour,count(distinct udid)
from basis_data.basis_event where create_date>='2015-10-31' and create_date<='2015-11-01' and area_code like '51%' and length(area_code) = 6 group by create_date,hour;
--2.四川地区用户在考拉fm内收听最多节目。（可以排出top20的节目即可）


select tmp_d.create_date,tmp_d.album_name,tmp_d.sum_count,tmp_d.sum_count_user,tmp_d.r from (
select tmp_c.create_date,tmp_c.album_name,tmp_c.sum_count,tmp_c.sum_count_user,dense_rank() over(partition by tmp_c.create_date order by tmp_c.sum_count desc)  as r 
from (
select tmp_b.create_date,tmp_b.album_name,count(tmp_b.udid) as sum_count,count(distinct tmp_b.udid) as sum_count_user
from (select udid,create_date from kaola_stat.user_info_day where create_date>='2015-10-31' and create_date<='2015-11-01' and area_code like '51%' and length(area_code) = 6 )tmp_a 
join (select udid,album_id,album_name,create_date from  kaola_stat.user_play_day where create_date>='2015-10-31' and create_date<='2015-11-01' ) tmp_b 
on (tmp_a.udid = tmp_b.udid and tmp_a.create_date = tmp_b.create_date) group by tmp_b.create_date,tmp_b.album_name  ) tmp_c ) tmp_d where tmp_d.r <=20

--3.四川地区用户使用wifi时间段分布（需要了解什么时间段内使用wifi用户最多）
select create_date,hour,count(distinct udid)
from basis_data.basis_event where create_date>='2015-10-31' and create_date<='2015-11-01' and area_code like '51%' and length(area_code) = 6 and network='1' group by create_date,hour; 


select album_id,max(album_name),catalog_id,max(catalog_name),sum(play_count) play_count,ROUND(SUM(play_time)/60) from data_st.analysis_of_album_stat_client  where stat_date>='2015-10-12' and stat_date<='2015-10-18' and catalog_id=127 and device_type=-2 and album_id!=-1  GROUP BY catalog_id,album_id ORDER BY play_count desc LIMIT 50;
