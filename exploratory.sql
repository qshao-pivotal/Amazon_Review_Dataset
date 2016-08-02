\timing on
set search_path to amazon;
---------------------------- Metadata Exploratory ----------------------------
-- -- Find the category distribution of items
-- DROP TABLE IF EXISTS item_category_distribution;
-- EXPLAIN ANALYZE
-- CREATE TABLE item_category_distribution 
-- WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
-- AS
--   WITH q1 as (SELECT unnest(categories) AS category FROM metadata)
--   (SELECT category,Count(*) AS item_count
--    FROM q1
--    GROUP BY category) 
-- DISTRIBUTED RANDOMLY;

-- Assign unique id to each category;


---------------------------- Kmeans Clustering Category ----------------------------
-- Assign unique id to each category
DROP TABLE IF EXISTS category_mapping;
EXPLAIN ANALYZE
CREATE TABLE category_mapping 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS
  WITH q1 as (SELECT distinct unnest(categories) AS category FROM metadata)
  (
    SELECT category,ROW_NUMBER() OVER (ORDER BY category) as cat_id from q1
  )
DISTRIBUTED RANDOMLY;

-- Assign unique id to each asin
DROP TABLE IF EXISTS asin_mapping;
EXPLAIN ANALYZE
CREATE TABLE asin_mapping 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS
(
  SELECT asin,ROW_NUMBER() OVER (ORDER BY asin) as asin_id from metadata
)
DISTRIBUTED RANDOMLY;

-- Create category-asin matrix
DROP TABLE IF EXISTS category_asin_matrix;
EXPLAIN ANALYZE
CREATE TABLE category_asin_matrix 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS
  WITH q1 as (SELECT asin, unnest(categories) AS category FROM metadata)
  (
    SELECT cat_id,asin_id, 1 as val 
    from q1 join asin_mapping q2
    on q1.asin = q2.asin
    join category_mapping q3
    on q1.category = q3.category
  )
DISTRIBUTED RANDOMLY;

-- Convert sparse matrix to a dense one using madlib
DROP TABLE IF EXISTS category_asin_matrix_dense;
explain analyze SELECT madlib.matrix_densify('category_asin_matrix', 'row=cat_id, col=asin_id, val=val','category_asin_matrix_dense');

-- Kmeans Clustering the
SELECT * FROM madlib.kmeanspp( 'category_asin_matrix_dense','val',
                               2,
                               'madlib.squared_dist_norm2',
                               'madlib.avg',
                               20,
                               0.001
                             );

---------------------------- Kmeans Clustering Category ----------------------------
















-- Find the category distribution of items
DROP TABLE IF EXISTS item_category_distribution;
EXPLAIN ANALYZE
CREATE TABLE item_category_distribution 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS
  WITH q1 as (SELECT unnest(categories) AS category FROM metadata)
  (SELECT category,Count(*) AS item_count
   FROM q1
   GROUP BY category) 
DISTRIBUTED RANDOMLY;



-- Find some daily statistics of review
DROP TABLE IF EXISTS daily_statistics;
EXPLAIN ANALYZE
CREATE TABLE daily_statistics 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS 
  (SELECT 
    review_date,
    Min(overall) as min_score,
    Max(overall) as max_score,
    Avg(overall) as avg_score,
    Median(overall) as median_score,  
    Count(*) AS review_count 
   FROM reviewdata 
   WHERE Extract(year FROM review_date) BETWEEN 1995 AND 2015 
   GROUP BY review_date) 
DISTRIBUTED RANDOMLY; 

-- Find the distribution of ratings 1-5
DROP TABLE IF EXISTS overall_distribution;
EXPLAIN ANALYZE 
CREATE TABLE overall_distribution 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS 
  (SELECT overall,
          Count(*) AS overall_count 
   FROM reviewdata 
   WHERE Extract(year FROM review_date) BETWEEN 1995 AND 2015 
   GROUP BY overall) 
DISTRIBUTED RANDOMLY; 



-- Find the reviews distribution based on category
DROP TABLE IF EXISTS category_review_distribution;
EXPLAIN ANALYZE
CREATE TABLE category_review_distribution 
WITH(APPENDONLY=TRUE,ORIENTATION=ROW,COMPRESSTYPE=QUICKLZ)
AS
  WITH q1 as (SELECT asin,unnest(categories) AS category FROM metadata),
       q2 as (SELECT asin,count(*) as review_count from reviewdata group by asin)
  (SELECT category,sum(review_count) AS review_count
   FROM q1 join q2
   ON q1.asin = q2.asin
   GROUP BY category) 
DISTRIBUTED RANDOMLY;
