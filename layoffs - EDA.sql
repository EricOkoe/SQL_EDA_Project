-- In this Project we are going to perform EXPLORATORY DATA ANALYSIS to understand the dataset

-- We will start of by doing a little cleaning on the dataset

-- Removing duplicates in the dataset
WITH duplicate_cte AS (SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging3
ORDER BY company)

SELECT *
FROM duplicate_cte;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging3;

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;





-- Next, we are going to to explore the data with the following codes

-- Total number of rows
SELECT COUNT(*) AS total_rows FROM layoffs_staging2;


-- Summary statistics for numerical columns
SELECT 
  AVG(total_laid_off) AS avg_laid_off, 
  MIN(total_laid_off) AS min_laid_off, 
  MAX(total_laid_off) AS max_laid_off,
  AVG(funds_raised_millions) AS avg_funds_raised,
  MIN(funds_raised_millions) AS min_funds_raised,
  MAX(funds_raised_millions) AS max_funds_raised
FROM layoffs_staging2;


-- Data Distribution
-- Count of unique values for categorical columns
SELECT 
  COUNT(DISTINCT company) AS unique_companies,
  COUNT(DISTINCT location) AS unique_locations,
  COUNT(DISTINCT industry) AS unique_industries,
  COUNT(DISTINCT country) AS unique_countries
FROM layoffs_staging2;


-- Top 5 layoffs
SELECT MAX(total_laid_off) AS Highest_layoff, MAX(percentage_laid_off) AS Highest_pct_layoff
FROM world_layoffs.layoffs_staging2
LIMIT 5;


-- Laid off based on the funds raised
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Total laid off by on the company
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Total laid off based on the industry of the company
SELECT SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2;


-- Total laid off based on the country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- Total laid off by year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


-- Total laid off based on the stage of the company
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY stage
ORDER BY 2 DESC;


-- Total laid off by month
SELECT substring(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY substring(`date`, 1, 7)
ORDER BY 1;


-- Segmentation by industry
SELECT 
    industry,
    COUNT(*) AS num_records,
    AVG(total_laid_off) AS avg_laid_off
FROM
    layoffs_staging2
GROUP BY industry
ORDER BY avg_laid_off DESC;


-- Rolling totals by month laid off
WITH rolling_cte AS (
SELECT substring(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total_layoff
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY substring(`date`, 1, 7)
ORDER BY 1
)


SELECT `Month`, total_layoff,
SUM(total_layoff) OVER(ORDER BY `Month`) AS rolling_total_layoff
FROM rolling_cte
;


-- Month on Month percentage change in layoff
WITH month_on_month_cte AS (SELECT substring(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total_layoff
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY substring(`date`, 1, 7)
ORDER BY 1)

SELECT  `Month`, total_layoff,
LAG(total_layoff) OVER(ORDER BY `Month`) AS previous_month_layoff,
CASE 
WHEN LAG(total_layoff) OVER(ORDER BY `Month`) IS NULL THEN 0
ELSE ((total_layoff - LAG(total_layoff) OVER(ORDER BY `Month`))/ LAG(total_layoff) OVER(ORDER BY `Month`)) * 100
END AS MoM_percentage_layoff
FROM month_on_month_cte;


-- Top 5 companies with the largest layoffs per year
WITH t1 AS 
(SELECT company, YEAR(`date`) AS `Year`, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NOT NULL AND YEAR(`date`) IS NOT NULL
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC),
t2 AS 
(SELECT *,
DENSE_RANK() OVER(PARTITION BY `YEAR` ORDER BY total_layoffs DESC) AS Ranking
FROM t1)

SELECT * 
FROM t2
WHERE Ranking <= 5;


-- Top 5 industries with the highest layoffs per year
WITH t3 AS
(SELECT industry, YEAR(`date`) AS `Year`, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NOT NULL AND YEAR(`date`) IS NOT NULL
GROUP BY industry, YEAR(`date`)
ORDER BY 3 DESC),
t4 AS 
(SELECT *,
DENSE_RANK() OVER(PARTITION BY `YEAR` ORDER BY total_layoffs DESC) AS Ranking
FROM t3)

SELECT * 
FROM t4
WHERE Ranking <= 5;


-- Layoffs trend over time
SELECT 
  date,
  SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY date
ORDER BY date;


-- Layoffs trend over time
SELECT `date`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE `date` IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY date
ORDER BY date;


-- Difference in layoff percentages between industries
SELECT 
  industry,
  AVG(CASE WHEN percentage_laid_off >= 0.1 THEN 1 ELSE 0 END) AS high_layoff_percentage,
  AVG(CASE WHEN percentage_laid_off < 0.1 THEN 1 ELSE 0 END) AS low_layoff_percentage
FROM (
  SELECT 
    industry,
    total_laid_off / SUM(total_laid_off) OVER(PARTITION BY industry ORDER BY industry) AS percentage_laid_off
  FROM layoffs_staging2
) AS subquery
GROUP BY industry
ORDER BY industry;

