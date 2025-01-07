select 
    exhibition_id, 
    exhibition_name, 
    exhibition_description, 
    d.department_name, 
    f.floor_name, 
    exhibition_start_date, 
    public_id 
from exhibition 
join department d using(department_id) 
join floor f using(floor_id);