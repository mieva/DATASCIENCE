select m.id, tin.age, m.coins_needed, tin.power
from Wands as m
inner join Wands_property wp
on m.code = wp.code
inner join 
(
    select age, min(coins_needed) as min_galeons, power
    from Wands w
    inner join Wands_property wp
    on w.code = wp.code
    where is_evil = 0
    group by age, power
    ) tin
on tin.min_galeons = m.coins_needed and m.power = tin.power and tin.age = wp.age
order by tin.power desc, tin.age desc

////////////////////////////////////////////

SELECT *
 FROM
 (
     SELECT ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Point) as RowNum, *
     FROM Table
 ) X 
 WHERE RowNum = 1