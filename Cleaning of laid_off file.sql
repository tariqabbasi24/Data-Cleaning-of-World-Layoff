-- DATA CLEANING
/*select * 
from layoffs_staging;
-- 1. REMOVE the duplicates 
-- 2. Standerize the Data 
-- 3. Null values or blank values
-- 4. Remove any columns or rows

	/*create table layoffs_staging
    like layoffs;
    insert layoffs_staging
    select * 
    from layoffs;*/

with duplicate_cte as (select * ,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select  *  
from  duplicate_cte 
where row_num >1;

select * 
from layoffs_staging
where company = 'Oracle';






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
  row_num  INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;
insert into layoffs_staging2
select * ,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;
select  *  
from  layoffs_staging2
where row_num >1;

DELETE  
from  layoffs_staging2
where row_num >1;

select  *  
from  layoffs_staging2;

-- 2. Standerizing  the Data ----------
select  company, (trim(company)) 
from  layoffs_staging2;

update layoffs_staging2
set company = trim(company); 


select  distinct industry 
from  layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select  distinct industry 
from  layoffs_staging2;

select  distinct country
from  layoffs_staging2
order by 1;

select  distinct country
from  layoffs_staging2
where country like 'United States%';

select   distinct country, trim( TRAILING '.' from country)
from  layoffs_staging2
order by 1;
 
update layoffs_staging2 
set country = trim( TRAILING '.' from country)
where country like 'United States%';

select  date
from  layoffs_staging2;

update layoffs_staging2 
set date = str_to_date(date, '%m/%d/%Y');


alter table layoffs_staging2
modify column date  DATE;

select  *
from  layoffs_staging2;

-- 3. Null values or blank values---
select  *
from  layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

update layoffs_staging2
set industry = null 
where industry =  '';
select *
from  layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from  layoffs_staging2 t1
join  layoffs_staging2 t2 on t1.company = t2.company and t1.location =t2.location
where t1.industry is null and t2.industry is not null ;

update  layoffs_staging2 t1
join  layoffs_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null  and t2.industry is not null;


select *
from  layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from  layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select *
from  layoffs_staging2;
-- delete the extra row_num from data
alter table layoffs_staging2
drop column row_num;

select *
from  layoffs_staging2;

