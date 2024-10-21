use world_layoffs;

select * from layoffs;

-- 1. create a duplicate table

create table layoffs_staging
like layoffs; 

insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;

-- 2. see if any dupicate records

select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
as row_num from layoffs_staging;

with s as(
select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 
date, stage, country, funds_raised_millions)
as row_num from layoffs_staging
) select * from s where row_num>1;

select * from layoffs_staging where company="cazoo";

-- 3. Remove Duplicates
-- we can not update any CTE. i.e. update, insert, delete

-- created a table layoffs_staging2

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
  `row_num`  int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

-- inserting the records with row_num included

insert into layoffs_staging2
select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 
date, stage, country, funds_raised_millions)
as row_num 
from layoffs_staging;

select * from layoffs_staging2 where row_num>1;

-- deleting the duplicate records--
Delete 	from layoffs_staging2 where row_num>1;
set sql_safe_updates=0;

-- 4. Standardizing the data
-- finding issues in your data & fixing it

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company= trim(company);

select * from layoffs_staging2;
--
select distinct(industry )
from layoffs_staging2 order by 1;

-- here we have crypto, cuypto currency, cryptocurrency, need to update those to crypo.

select * from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select * from layoffs_staging2
where industry like 'crypto%';

--

select distinct(location)
from layoffs_staging2 order by 1;

select distinct(country)
from layoffs_staging2 order by 1;

select * from layoffs_staging2
where country like 'united states%';

select distinct country, trim( trailing '.' from country)
from layoffs_staging2 ORDER BY 1;

update layoffs_staging2
set country= trim( trailing '.' from country)
where country like 'United States%';

-- converting date column from text to date format --> str_to_date()

select date, str_to_date(date, "%m/%d/%Y")
from layoffs_staging2;

-- updating the date column

update layoffs_staging2
set date= str_to_date(date, "%m/%d/%Y");

select * from layoffs_staging2;

alter table layoffs_staging2
modify column date Date;

-- 5. Null and Blank values-- 

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2
WHERE industry is null
or industry= '';

select * from layoffs_staging2
where company= 'juul';

update layoffs_staging2
set industry= 'Travel'
where company='Airbnb';

Select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- easy to understand:
Select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- update the blank industry column
update layoffs_staging2
set industry= null
where industry='';

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company=t2.company
    set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select distinct company from layoffs_staging2 order by 1;

select * from layoffs_staging2
where company like 'bally%';

select * from layoffs_staging2;

--

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;
-- next we will do EDA --
