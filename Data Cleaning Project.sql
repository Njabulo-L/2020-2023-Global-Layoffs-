-- Data Cleaning 

-- Select all records from the layoffs table
SELECT *
FROM layoffs;

-- Objectives 
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove unnecesarry Columns

-- Create a new table to store duplicate data
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Select all records from the new staging table
SELECT *
FROM layoffs_staging;

-- Insert data from layoffs table into staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Removing Duplicates 

-- Step 1: Identify duplicates using ROW_NUMBER 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Use CTE to find and list duplicate rows
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Check for specific duplicates (eg. Company, industry etc)
SELECT *
FROM layoffs_staging
WHERE company = 'Casper'; #To hceck if the query works well

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

-- Step 2: Create a table for duplicated data 
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

-- Selete all records from the new duplicated table 
SELECT *
FROM layoffs_staging2;

-- Step 3: Insert the duplicated data into the new table 
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Identify what is being deleted
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Delete duplicated rows based on row_num
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 5: Verify if the duplicates have been deleted
SELECT *
FROM layoffs_staging2;

-- 2. Standardizing Data (Fixing issues in the data)

-- Trim white space from company names 
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

-- Update company names by trimming white space
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Check distinct industry names 
SELECT DISTINCT industry
FROM layoffs_staging2;

-- STandardize industry names 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Trim trailing periods from country names 
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Update country names to eemove trailing periods
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

-- Convert date fromat from string to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update date column to standard date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Verify date column change 
SELECT `date`
FROM layoffs_staging2;

-- Alter date definition from text to date in schemas 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null Values and Blank Values

-- Identify records where industry is null or blank 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Set industry to null where industry is blank 
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

-- Select records where company = Airbnb
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Find matching industry values with the same company 
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Update null values in industry using matching company records 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4. Removal of unnecessary rows and columns
-- Select records with null values for total_laid_off and Percentage_laid_off
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete records with null values for total_laid_off and Percentage_laid_off
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Select all records from the cleaned table 
SELECT *
FROM layoffs_staging2;

-- Drop the row_num column 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- END




