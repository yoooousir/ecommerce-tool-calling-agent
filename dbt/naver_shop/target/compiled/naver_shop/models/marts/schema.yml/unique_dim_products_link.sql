
    
    

select
    link as unique_field,
    count(*) as n_records

from main."dim_products"
where link is not null
group by link
having count(*) > 1


