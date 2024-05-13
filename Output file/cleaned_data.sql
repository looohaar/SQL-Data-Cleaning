-- Data Cleaning 

USE world_layoffs;
SELECT * FROM layoffs;

-- Steps involved

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null value or Blank value treatement
-- 4. Remove irrelevant data


-- 1.Remove duplicates
-- Making a copy of the raw data set 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

-- Adding values to layoffs_staging

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging;

-- Create a Row Number column for finding duplicate rows
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)
-- Determine duplicates
SELECT * 
FROM duplicate_cte
WHERE row_num>1;

-- Create a duplicate of layoffs_staging to delete the duplicates

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num>1;

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

SELECT * 
FROM layoffs_staging2;


-- 2.Standardize the data 

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Remove blank spaces from left and right side of the company values
UPDATE layoffs_staging2
SET company= TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Group Crypto industries with a single value "Crypto"
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto %'; 

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto %';

SELECT DISTINCT industry 
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Remove the issue with contry "United States."
UPDATE layoffs_staging2
SET country= TRIM( TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


-- Update the datatype of date column from text to date
UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

-- 3.Null value and Blank value treatement 

-- Industry column 
UPDATE layoffs_staging2
SET industry= NULL
WHERE industry= '';


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company= t2.company
SET t1.industry= t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove irrelevant data

-- Drop row_num
ALTER TABLE  layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;




