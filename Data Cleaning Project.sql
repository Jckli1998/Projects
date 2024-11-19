-- data cleaning

select * from
layoffs;

-- 1. remove duplicates
-- 2. standardise the data
-- 3. Null values or blank values
-- 4. remove any columns

-- 1. remove duplicates

create table layoffs_staging
like layoffs;


select * from
layoffs_staging;

insert layoffs_staging
select* from
layoffs;

select *,
 row_number() over(
 PARTITION BY company,industry, total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging
;


with duplicate_cte as 
(
select *,
 row_number() over(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;

select * from 
layoffs_staging
where company = "casper" ;


with duplicate_cte as 
(
select *,
 row_number() over(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
delete  
from duplicate_cte
where row_num > 1;


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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2
where row_num  > 1;

insert into layoffs_staging2
select *,
 row_number() over(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging ;

delete  
from layoffs_staging2
where row_num  > 1;

select *
from layoffs_staging2;

-- 2. standardise the data

select company, trim(company) 
from layoffs_staging2
;

update layoffs_staging2
set company = trim(company);

select distinct industry 
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select *
from layoffs_staging2
where country like 'United States%'
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoffs_staging2;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER table layoffs_staging2
modify column `date` DATE;

SELECT 
* FROM layoffs_staging2;

-- 3. Null values or blank values

SELECT *
FROM layoffs_staging2
where total_laid_off IS null
and percentage_laid_off IS null
;

SELECT *
FROM layoffs_staging2
where industry is null or industry = '';

select * from layoffs_staging2
where company like 'Bally%';


select * from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
	and t1.location = t2.location
where (t1.industry is null or t1.industry ='') 
and t2.industry is not null;

-- changing blanks to null
update layoffs_staging2
set industry = NULL	
WHERE industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

SELECT *
FROM layoffs_staging2
where total_laid_off IS null
and percentage_laid_off IS null
;

delete from
layoffs_staging2
where total_laid_off IS null
and percentage_laid_off IS null;

SELECT *
FROM layoffs_staging2;

-- 4. remove any columns
alter table layoffs_staging2
drop column row_num;