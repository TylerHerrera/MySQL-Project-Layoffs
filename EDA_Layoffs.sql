-- SQL Project - Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- Look at the time frame of the data. The data spans from 2020 to 2023.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Look at how big the layoffs were in absolute terms as well as relatively to the company.
-- Some companies went out of business with percentage_laid_off = 1. While a company laid off up to 12000 employees.
SELECT MAX(total_laid_off), MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM layoffs_staging2;

-- Some companies that went out of business even if they raised billions of dollars.
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Some big tech companies such as Amazon, Google, Meta... laid off the mosst  
SELECT company, industry, MAX(total_laid_off), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, industry
ORDER BY 4 DESC;

-- Lets look at the total_laid_off by industry, country, stage of fund raising, and year.
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 3 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- This query allows us to look at total_laid_off by each month across the time frame.
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1;

-- Base on the previous view, we created a rolling total of layoffs per months with CTE. 
WITH rolling_total AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS month_total
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1
)
SELECT `MONTH`, month_total, SUM(month_total) OVER(ORDER BY `MONTH`)
FROM rolling_total;

-- This query is for looking at total_laid_off of a company each year.
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- Use the previous query as a CTE and create another CTE based on that to rank companies that laid off the most by year.
WITH year_laid_off (company, years, total_laid_off) AS
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) ranking
FROM year_laid_off
WHERE years IS NOT NULL
)
SELECT *
FROM year_rank
WHERE ranking <= 5;