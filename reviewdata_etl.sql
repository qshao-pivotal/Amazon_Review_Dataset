DROP TYPE reviewdata_type cascade;
CREATE TYPE reviewdata_type AS(
reviewerID text,
asin text,
reviewerName text,
helpful integer[],
reviewText text,
overall numeric,
summary text,
unixReviewTime numeric,
reviewTime text
);

CREATE OR REPLACE FUNCTION reviewdata_string_to_type (data text)
  RETURNS reviewdata_type
AS $$
  import ast
  raw = ast.literal_eval(data)
  return [
  raw.get("reviewerID"),
  raw.get("asin"),
  raw.get("reviewerName"),
  [int(score) for score in raw.get("helpful",[])],
  raw.get("reviewText"),
  raw.get("overall"),
  raw.get("summary"),
  raw.get("unixReviewTime"),
  raw.get("reviewTime")]
$$ LANGUAGE plpythonu;



drop external table if exists reviewdata_raw;
create external table reviewdata_raw(
content text
)
location('s3://s3.amazonaws.com/pivotal-2015/qishao/amazon_reviews/reviewdata/ config=/home/gpadmin/s3.conf')
format 'TEXT'(DELIMITER E'\001' ESCAPE 'OFF' NULL E'');

drop table if exists reviewdata_cleaned;
create table reviewdata_cleaned as (
    select (content).reviewerID,
    (content).asin,
    (content).reviewerName,
    (content).helpful,
    (content).reviewText,
    (content).overall,
    (content).summary,
    to_timestamp((content).unixReviewTime::numeric) as review_timestamp,
    to_date((content).reviewTime,'MM DD,YYYY') as review_date
    from (
        select reviewdata_string_to_type(content) as content from reviewdata_raw
        ) as raw
)
distributed randomly;
