drop type metadata_type cascade;
create type metadata_type as(
asin text,
salesRank text,
imUrl text,
categories text[],
title text,
description text,
bought_together text[],
also_bought text[],
also_viewed text[],
buy_after_viewing text[],
price numeric,
brand text
);

CREATE OR REPLACE FUNCTION metadata_string_to_type (data text)
  RETURNS metadata_type
AS $$
  import ast
  raw = ast.literal_eval(data)
  return [
  raw.get("asin"),
  raw.get("salesRank"),
  raw.get("imUrl"),
  list(set([item for sublist in raw.get("categories",[]) for item in sublist if not item.strip()])),
  raw.get("title"),
  raw.get("description"),
  raw.get('related',{}).get("bought_together",[]),
  raw.get('related',{}).get("also_bought",[]),
  raw.get('related',{}).get("also_viewed",[]),
  raw.get('related',{}).get("buy_after_viewing",[]),
  raw.get("price"),
  raw.get("brand")]
$$ LANGUAGE plpythonu;



drop external table if exists metadata_raw;
create external table metadata_raw(
content text
)
location('pxf://hdpnn:51200/data/metadata/?profile=HdfsTextSimple')
format 'TEXT'(DELIMITER E'\001' ESCAPE 'OFF' NULL E'');

drop table if exists metadata_cleaned;
explain analyze
create table metadata_cleaned 
with()
as (
    select (content).asin,
    (content).salesRank,
    (content).imUrl,
    (content).categories,
    (content).title,
    (content).description,
    (content).bought_together,
    (content).also_bought,
    (content).also_viewed,
    (content).buy_after_viewing,
    (content).price,
    (content).brand
    from (
        select metadata_string_to_type(content) as content from metadata_raw
        ) as raw
)
distributed randomly;
