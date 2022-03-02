select 
	Customer.CustomerId, 
    Customer.CustomerName 
from 
	Customer 
inner join orders 
	on orders.customerId = Customer.CustomerId 
where orders.Quantity >1;


select 
	distinct
	Customer.CustomerId, 
    Customer.CustomerName 
from 
	Customer 
inner join orders 
	on orders.customerId = Customer.CustomerId 
where monthname(orders.orderDate) = 'June';