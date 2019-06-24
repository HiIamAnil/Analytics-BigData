# sql queries 
sanat.bhat18@outlook.com

outlook00sS@

# how to find the null count of a column using this self join left
select count(*) from 
( (select * from people ) a
left outer join 
(select id, birthdate from people) b
on a.id = b.id)
where b.birthdate is null
