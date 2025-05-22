SELECT * FROM word_layoffs.layoffs;

CREATE TABLE Layoffs_staging
LIKE layoffs;

SELECT *
FROM Layoffs_staging;

INSERT INTO Layoffs_staging
SELECT *
FROM Layoffs;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `Date`, stage, country, funds_raised_millions
) AS row_num
FROM Layoffs_staging)

SELECT *
FROM duplicate_cte
WHERE company = 'casper';

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

SELECT*
FROM layoffs_staging2
WHERE row_num >1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `Date`, stage, country, funds_raised_millions
) AS row_num
FROM Layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT  company, TRIM(company)
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto' 
WHERE industry LIKE 'crypto%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET Country = TRIM(TRAILING '.' FROM country) 
WHERE Country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT*
FROM layoffs_staging2
WHERE industry IS NULL
 OR industry = '';
 
 SELECT *
 FROM layoffs_staging2
 WHERE company = 'Airbnb';
 
 UPDATE layoffs_staging2
 SET industry =  NULL
 WHERE industry = '';
 
 
 SELECT *
 FROM layoffs_staging2 AS t1
 JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE  layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
 WHERE t1.industry IS NULL 
 AND t2.industry IS NOT NULL;
 
 SELECT*
FROM layoffs_staging2;
 
 SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

 DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

 SELECT*
FROM layoffs_staging2; 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


 SELECT*
FROM layoffs_staging2;
 
 
 
 