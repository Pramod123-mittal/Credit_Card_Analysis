--Data Transformation
alter table credit_card_transactions alter column transaction_date datetime;
alter table credit_card_transactions alter column amount bigint;

--Preliminary Steps
select * from credit_card_transactions;
select card_type, sum(amount) as total_by_card  from credit_card_transactions
group by card_type;

--Data Insights
--1- write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 
with cte1 as (
 SELECT city, total_spend,total_monthly_spend from 
((select city , sum(amount) total_spend from credit_card_transactions
group by city) A  cross JOIN (select sum(amount) as total_monthly_spend from credit_card_transactions) B) )
select * from (
select city, percentage_contri ,DENSE_RANK() over (order by percentage_contri desc) AS RNK from (
select city, (1.0*total_spend/total_monthly_spend)*100 as percentage_contri from cte1 )A)B where rnk <=5;

--2- write a query to print highest spend month and amount spent in that month for each card type
with cte1 as (
select card_type,DATEPART(year, transaction_date) as yr, DATEPART(month, transaction_date) as mth, sum(amount) as total
,DENSE_RANK() over (partition by card_type  order by sum(amount) desc) as rnk
from credit_card_transactions group by card_type,DATEPART(year, transaction_date) , DATEPART(month, transaction_date) )
select card_type,total,mth,yr from cte1 where rnk = 1;


--3- write a query to print the transaction details(all columns from the table) for each card type when
--it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
with cte1 as (
select * from (
select *, sum(amount) over (partition by card_type order by transaction_date, transaction_id) as c_sum from 
credit_card_transactions) A where c_sum >= 1000000 )
select * from (
select * , DENSE_RANK() over (partition by card_type order by c_sum) as rnk from cte1) A
where rnk = 1;

--4- write a query to find city which had lowest percentage spend for gold card type
select top 1 city, sum(amount) as total_spend
,(sum(case when card_type = 'gold' then amount else 0 end)*1.0/sum(amount))*100.0 as gold_contribution
from credit_card_transactions group by city
having sum(case when card_type = 'gold' then amount else 0 end)>0
order by gold_contribution;


--5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)
with cte1 as(
select city , exp_type, sum(amount) as total_spend
from credit_card_transactions
group by city,exp_type),
cte2 as (
select * from (
select *, dense_rank() over (partition by city order by total_spend ) as lower_rnk,
dense_rank() over (partition by city order by total_spend desc ) as upper_rnk
from cte1) A where lower_rnk = 1 or upper_rnk = 1)
select city,
min(case when lower_rnk = 1 then exp_type end) as lowest_expense,
max(case when upper_rnk = 1 then exp_type end) as highest_expense
from cte2
group by city;

--6- write a query to find percentage contribution of spends by females for each expense type

with cte1 as (
select exp_type, 
sum(case when gender = 'F' then amount else 0 end) as female_Spend,
sum(case when gender = 'M' then amount else 0 end) as male_Spend
from credit_card_transactions
group by exp_type)
select exp_type, (female_Spend*1.0/(male_Spend+female_Spend)*100.0) as female_contribution
from cte1;


--7- which card and expense type combination saw highest month over month growth in Jan-2014
with cte1 as(
select card_type, DATEPART(year,transaction_date) as yr, DATEPART(month,transaction_date) as mth, exp_type, sum(amount) as total
from credit_card_transactions --where DATEPART(year,transaction_date) = 2014 and DATEPART(month,transaction_date) = 01
group by card_type,DATEPART(year,transaction_date) ,DATEPART(month,transaction_date) , exp_type)
select * , (total-prev_sales) as mom_growth from (
select *, lag(total,1) over (partition by card_type ,exp_type order by yr,mth) as prev_sales
from cte1) A
where prev_sales is not null and yr = 2014 and mth = 1
order by mom_growth desc;



-- 8- during weekends which city has highest total spend to total no of transcations ratio 
select * from (
select city, count(1) as total_transaction, sum(amount) as total_city_spend , sum(amount)/count(1) as ratio
, rank() over (order by sum(amount)/count(1) desc) as rnk
from credit_card_transactions
where DATEPART(weekday, transaction_date) in (1,7)
group by city) A where rnk = 1;


--9- which city took least number of days to reach its 500th transaction after the first transaction in that city
with cte1 as (
select * , row_number() over (partition by city order by transaction_date) as rnk
from credit_card_transactions),
cte2 as(
select city, min(transaction_date) as first_transaction, max(transaction_date) as f00_transaction,
DATEDIFF(day,min(transaction_date), max(transaction_date)) as days_btw_transaction
from cte1
where rnk in (1,500)
group by city
having count(*) = 2)
select * from (
select *, rank() over (order by days_btw_transaction) as rnk from cte2) A where rnk = 1;
