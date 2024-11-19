-- exploratory data analysis

select* from 
layoffs_staging2;

SELECT MAX(total_laid_off), max(percentage_laid_off)
from
layoffs_staging2;


select* from 
layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;


select company , sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`) 
from layoffs_staging2;


select YEAR (`date`), sum(total_laid_off)
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;


select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;



select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- rolling total laidoff

SELECT substring(`date`, 1, 7) AS `Month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`, 1, 7) is not NULL
group by `Month`
order by 1 asc
;

with Rolling_total as 
(
SELECT substring(`date`, 1, 7) AS `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1, 7) is not NULL
group by `Month`
order by 1 asc
)
select `Month`, total_off, sum(total_off) over(ORDER BY `Month`) as Rolling_total
from Rolling_total;

-- see company layoff per year
SELECT company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc
;

SELECT company, YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by 3 desc;

with Company_year (company, years, total_laid_off) as -- 1st cte
(
SELECT company, YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
), Company_Year_Rank as -- another cte

(select *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off desc) as ranking
from Company_year
where years is not null
)
select * 
from Company_year_rank
where ranking <= 5
;
