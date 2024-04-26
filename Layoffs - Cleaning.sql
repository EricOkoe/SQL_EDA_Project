-- Creating a copy of main table for the cleaning exercise
CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Removing duplicates
WITH duplicate_cte AS (
SELECT *,
ROW_Number() Over(Partition By company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT * 
FROM duplicate_cte
Where row_num > 1;

CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

INSERT INTO layoffs_staging2
SELECT *,
ROW_Number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2 
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num = 1;


-- Standardizing the data
SELECT *
FROM layoffs_staging2;

-- Standardizing Company Name
UPDATE layoffs_staging2
SET company = trim(company);

-- Standardizing Location
UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location = 'DÃƒÂ¼sseldorf';

UPDATE layoffs_staging2
SET location = 'Florianopolis'
WHERE location = 'FlorianÃ³polis';

UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location = 'MalmÃƒÂ¶';

-- Standardizing industry
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry = 'CryptoCurrency';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry = 'CryptoCurrency';

-- Standardizing country
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.';

-- Method 2
SELECT DISTINCT(country), trim(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = trim(TRAILING '.' FROM country)
WHERE country = 'United States%';


-- Changing the date format from text to date
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Handling NULL and Blank values

-- Replacing Blank with NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Replacing the Blanks in the industry column with existing record
SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
AND t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1 
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- remove any rows we have to 
DELETE
FROM layoffs_staging2
WHERE total_laid_off is NULL AND percentage_laid_off IS NULL;

-- Drop column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

