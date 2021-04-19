create table if not exists liu.ods_gantry_transaction(
  GantryId string COMMENT '门架编号',
  MediaType string COMMENT '收费介质类型',
  TransTime timestamp COMMENT '交易时间',
  PayFee int COMMENT '收费金额',
  SpecialType string COMMENT '交易特情类型'
) partitioned by(month string)
row format delimited fields terminated by ','
lines terminated by '\n'
STORED AS ORC


CREATE TABLE `user_visit_action`(
 `date` string,
 `user_id` bigint,
 `session_id` string,
 `page_id` bigint,
 `action_time` string,
 `search_keyword` string,
 `click_category_id` bigint,
 `click_product_id` bigint,
 `order_category_ids` string,
 `order_product_ids` string,
 `pay_category_ids` string,
 `pay_product_ids` string,
 `city_id` bigint)
row format delimited fields terminated by ','

load data local inpath 'input/user_visit_action.txt' overwrite into table user_visit_action

CREATE TABLE `product_info`(
 `product_id` bigint,
 `product_name` string,
 `extend_info` string)
row format delimited fields terminated by ','

load data local inpath 'input/product_info.txt' overwrite into table product_info

CREATE TABLE `city_info`(
 `city_id` bigint,
 `city_name` string,
 `area` string)
row format delimited fields terminated by ','

load data local inpath 'input/city_info.txt' overwrite into table city_info


select
  *
from (
    select
        *,
        rank() over( partition by area order by clickCnt desc ) as rank
    from (
        select
            area,
            product_name,
            count(*) as clickCnt
        from (
            select
                a.*,
                p.product_name,
                c.area,
                c.city_name
            from user_visit_action a
            join product_info p on a.click_product_id = p.product_id
            join city_info c on a.city_id = c.city_id
            where a.click_product_id > -1
        ) t1 group by area, product_name
    ) t2
) t3 where rank <= 3