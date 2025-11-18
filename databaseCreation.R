# Load required libraries
#install.packages("duckdb")
library(DBI)
library(duckdb)
library(dplyr)

# ***
# PART 1: LOADING DATABASES
# ***

con <- dbConnect(duckdb::duckdb(), dbdir = "C:/Users/jakov/OneDrive/Desktop/video_games.db")

# ***
# PART 2: UPDATING SALES FROM PLATFORM-SPECIFIC TABLES
# ***

dbExecute(con,"
CREATE TABLE video_games AS 
  SELECT * FROM read_csv('C:/Users/jakov/OneDrive/Desktop/Video_Games_Sales_as_at_22_Dec_2016_refined.csv',
  delim=';', decimal_separator=',', header=true, auto_detect=true);
CREATE TABLE ps4_games AS
  SELECT * FROM read_csv_auto('C:/Users/jakov/OneDrive/Desktop/PS4_GamesSales.csv');
CREATE TABLE xbox_games AS
  SELECT * FROM read_csv_auto('C:/Users/jakov/OneDrive/Desktop/XboxOne_GameSales.csv');
          ")

# Checking whether tables were imported well
#dbGetQuery(con, "DESCRIBE ps4_games")
#dbGetQuery(con, "DESCRIBE xbox_games")

# PS4
dbExecute(con, "
  UPDATE video_games
  SET 
    NA_Sales = ps4.\"North America\",
    EU_Sales = ps4.Europe,
    JP_Sales = ps4.Japan,
    Other_Sales = ps4.\"Rest of World\",
    Global_Sales = ps4.Global
  FROM ps4_games ps4
  WHERE LOWER(TRIM(video_games.Name)) = LOWER(TRIM(ps4.Game))
    AND video_games.Platform = 'PS4'
          ")

# XboxOne
dbExecute(con, "
  UPDATE video_games
  SET 
    NA_Sales = xb.\"North America\",
    EU_Sales = xb.Europe,
    JP_Sales = xb.Japan,
    Other_Sales = xb.\"Rest of World\",
    Global_Sales = xb.Global
  FROM xbox_games xb
  WHERE LOWER(TRIM(video_games.Name)) = LOWER(TRIM(xb.Game))
    AND video_games.Platform = 'XOne'
")

# ***
# PART 3: UPDATING MISSING YEARS FROM PS4 AND XBOX TABLES
# ***

print(dbGetQuery(con, "
  SELECT COUNT(*) as count
  FROM video_games
  WHERE Year_of_Release IS NULL
"))

print(dbGetQuery(con, "
  SELECT 
    vg.Name as game_name,
    vg.Platform,
    vg.Year_of_Release as old_year,
    TRY_CAST(ps4.Year AS BIGINT) as new_year
  FROM video_games vg
  INNER JOIN ps4_games ps4 ON LOWER(TRIM(vg.Name)) = LOWER(TRIM(ps4.Game))
  WHERE vg.Year_of_Release IS NULL
    AND ps4.Year IS NOT NULL
"))

print(dbGetQuery(con, "
  SELECT 
    vg.Name as game_name,
    vg.Platform,
    vg.Year_of_Release as old_year,
    TRY_CAST(xb.Year AS BIGINT) as new_year
  FROM video_games vg
  INNER JOIN xbox_games xb ON LOWER(TRIM(vg.Name)) = LOWER(TRIM(xb.Game))
  WHERE vg.Year_of_Release IS NULL
    AND xb.Year IS NOT NULL
"))

# only one match for both and it's a match from a year we agreed not to consider
# which means no updates for blank Year data will be done

# ***
# PART 4: DATABASE CREATION
# ***

# dimension tables
dbExecute(con, "
  CREATE TABLE dim_platforms AS
  SELECT ROW_NUMBER() OVER () AS platform_id, Platform
  FROM (SELECT DISTINCT Platform FROM video_games WHERE Platform IS NOT NULL)
")

dbExecute(con, "
  CREATE TABLE dim_genres AS
  SELECT ROW_NUMBER() OVER () AS genre_id, Genre
  FROM (SELECT DISTINCT Genre FROM video_games WHERE Genre IS NOT NULL)
")

dbExecute(con, "
  CREATE TABLE dim_ratings AS
  SELECT ROW_NUMBER() OVER () AS rating_id, Rating
  FROM (SELECT DISTINCT Rating FROM video_games WHERE Rating IS NOT NULL)
")

dbExecute(con, "
  CREATE TABLE dim_publishers AS
  SELECT ROW_NUMBER() OVER () AS publisher_id, Publisher
  FROM (SELECT DISTINCT Publisher FROM video_games WHERE Publisher IS NOT NULL)
")

dbExecute(con, "
  CREATE TABLE dim_developers AS
  SELECT ROW_NUMBER() OVER () AS developer_id, Developer
  FROM (SELECT DISTINCT Developer FROM video_games WHERE Developer IS NOT NULL)
")

# fact table
dbExecute(con, "
  CREATE TABLE fact_sales AS
  SELECT 
    ROW_NUMBER() OVER () AS sales_id,
    vg.Name,
    p.platform_id,
    vg.Year_of_Release,
    g.genre_id,
    r.rating_id,
    pub.publisher_id,
    dev.developer_id,
    NULL AS developer2_id,
    vg.NA_Sales,
    vg.EU_Sales,
    vg.JP_Sales,
    vg.Other_Sales,
    vg.Global_Sales,
    vg.Critic_Score,
    vg.Critic_Count,
    vg.User_Score,
    vg.User_Count
  FROM video_games vg
  LEFT JOIN dim_platforms p ON vg.Platform = p.Platform
  LEFT JOIN dim_genres g ON vg.Genre = g.Genre
  LEFT JOIN dim_ratings r ON vg.Rating = r.Rating
  LEFT JOIN dim_publishers pub ON vg.Publisher = pub.Publisher
  LEFT JOIN dim_developers dev ON vg.Developer = dev.Developer
")

dbExecute(con, "DROP TABLE video_games")
dbExecute(con, "DROP TABLE ps4_games")
dbExecute(con, "DROP TABLE xbox_games")

dbListTables(con)