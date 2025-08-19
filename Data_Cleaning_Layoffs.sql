-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- 1. Create new tables to store and handle data.

CREATE TABLE layoffs
(company text,
location text,
industry text,
total_laid_off int,
percentage_laid_off text,
`date` text,
stage text,
country text,
funds_raised_millions int);

-- Load raw data into created table.

LOAD DATA LOCAL INFILE '/Users/thanh/Downloads/layoffs.csv'
INTO TABLE layoffs
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions);

-- Create staging tables to handle data without affecting raw data.

CREATE TABLE layoffs_staging
LIKE worlds_layoffs.layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 2. Remove duplicates.

-- Check for duplicates. Any record that has row_num > 1 is a potential duplicates.

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
	AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Create another staging table with one column serving as ID for the records.

CREATE TABLE `layoffs_staging2` (
  `company` varchar(35) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `location` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `industry` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `stage` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
	AS row_num
FROM layoffs_staging;

-- Delete duplicates which has row_num greater than 1.

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 3. Standardize data.

-- Remove blank space in the values.

SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- Check for different values with similar meaning and turn them into one.

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Transform date column (text data) into (date data)

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;
 
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Handle NULL and blank values.

-- Transform null and blank values in the industry columns.

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry ='';

-- Let's take a look at these. There are some companies with multiple records, but Bally's Interactive is the only one. 
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Turn all blank values into NULL values for easy queries.

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Look at an overview of null and non-null values.

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry = '');

-- Update layoffs_staging2: if there is null value in industry, replace it with a non-null value in industry.

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Check again for the results.

SELECT *
FROM layoffs_staging2;

-- Spot Bally's Interactive as the only one with NULL value because there is no other record.

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- 4. Remove unnecessary columns or rows

-- Check for useless data. Because of these missing values, we cannot use it, so we can confidently delete it in the STAGING TABLE.

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop the row_num column we created to remove duplicates and no longer use.

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
