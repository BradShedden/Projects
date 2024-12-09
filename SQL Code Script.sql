-- Identifying datasets Columns and Data
SELECT *
FROM layoffs;

-- Creating a staging table so I am not working with the raw dataset
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Confirming table structure duplication was sucessful
SELECT *
FROM layoffs_staging;

-- Inseting duplicate of the original data into the staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Identifying duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- confirming if returned duplicates are accurate
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';
-- RESULT: Found that not all returned columns are actual duplicates, meaning I need to revise the query and partition by more columns

-- Revised query
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off
, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Creating a new table with row numbers so duplicates can be deleted
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

-- Inserting Data with unique row number query written above
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off
, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Confirming table creation worked
SELECT *
FROM layoffs_staging2;

-- Deleting duplicate rows
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Double checking rows were deleted
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardising data

-- identifying incorrect spacing in the comapany columnn
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Fixing spacing
UPDATE layoffs_staging2
SET company = TRIM(company);

-- identifying multiple instances of the same industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;
-- RESULT: found 3 different crypto instances

-- specifying how the crypto data presents to decide how to proceed
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- standardising all instances of Crypto variance into one format
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Identifying multiple instances of the same country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- Found 2 instances of 'United States' 

-- Removing full stop error
SELECT DISTINCT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Reformating the 'date' column
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Changing the 'date' column from a text to a date data type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- ADRESSING NULL AND BLANK VALUES

-- Populating blank and null fields in the 'industry' column with pre existing data
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
OR company = 'Carvana'
OR company = 'Juul'
OR company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Setting all Blank values to NULL to make updating the easier
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Updating with pre existing data
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Identifying and deleting rows due to non helpful data
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- REMOVING UNECESSARY DATA

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- EXPLORATORY DATA ANALYSIS STAGE 


-- Looking over database for EDA
SELECT *
FROM layoffs_staging2;

-- Finding the total amount of layoffs
SELECT SUM(total_laid_off)
FROM layoffs_staging2;

-- Finding total amount of companies
SELECT SUM(company)
FROM layoffs_staging2;

-- Finding the average percent of a company's workforce laid off
SELECT AVG(percentage_laid_off)
FROM layoffs_staging2;

-- Finding the first and last instance of layoffs in the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Querying maximum amount of total_laid_off and percentage_laid_off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Further exploring what companies laid off everyone
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY percentage_laid_off DESC;

-- filtering for large companies to be at the top
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Findiing out how many companies laid off everyone
SELECT COUNT(company)
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-- Finding what companies have laid off the most people
SELECT company, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
GROUP BY company
ORDER BY TLO DESC;

-- Dense ranking of the top 5 companys by layoff for each year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) AS TLO
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5
;

-- Finding what industies the most people have been laid off from
SELECT industry, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
WHERE industry != 'Other'
GROUP BY industry
ORDER BY TLO DESC;

-- Finding what industies the least people have been laid off from
SELECT industry, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
WHERE industry != 'Other'
GROUP BY industry
ORDER BY TLO ASC;

-- Finding what country laid off the most people 
SELECT country, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
GROUP BY country
ORDER BY TLO DESC;

-- How the lay offs are distributed between the 3 years
SELECT YEAR(`date`), SUM(total_laid_off) AS TLO
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Discovering what months the most layoffs happen
SELECT SUBSTRING(`date`,6,2) AS `Month`, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
GROUP BY `Month`
ORDER BY TLO DESC;

-- Rolling Sum of lay offs by month
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY `Month` ASC
)
SELECT `Month`, TLO, SUM(TLO) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total;

-- FINISHED