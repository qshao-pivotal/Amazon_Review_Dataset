DROP TABLE IF EXISTS product;
CREATE TABLE product
  as (
    select asin,
    title,
    description,
    brand,
    price,
    imurl
    from metadata_cleaned
  )
DISTRIBUTED RANDOMLY;


DROP TABLE IF EXISTS product_related;
CREATE TABLE product_related
  as (
    select asin,
    bought_together,
    also_bought,
    also_viewed,
    buy_after_viewing
    from metadata_cleaned
  )
DISTRIBUTED RANDOMLY;


DROP TABLE IF EXISTS reviews;
CREATE TABLE reviews
  as (
    select reviewerid,
    asin,
    helpful,
    overall,
    summary,
    reviewtext,
    review_timestamp,
    review_date::date
    from reviewdata_cleaned
  )
DISTRIBUTED RANDOMLY;

DROP TABLE IF EXISTS reviewer;
CREATE TABLE reviewer
  as (
    select
    distinct
    reviewerID,
    reviewerName
    from reviewdata_cleaned
  )
DISTRIBUTED RANDOMLY;
