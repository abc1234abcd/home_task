## home_task  

__comments__:
  
1. Loggings: I will delete unnecessary loggings in production to improve performance.

   
2. Asyncio: I use asyncio is becasue the task is I/O bounded.

 
3. Flatten: Nested portfolio is flattened as the dependency is not usful in this task, and flattened portfolio reduces latency in updating prices.
   
4. Hashmaps:  
        the designed calculator will be very effcient in updating price by maintaining one {portfolio: price} hashmap:
```python
 #weight is a direct hash value due to flatten:
                                 price += price_delta * weight
```

  Plus, reverted_hashmap {stock_name:portfolios} to enable o(1) constant time enquiry. 

  __Instructions to run this scripts__:
  1. git clone 'url'
  2. pip install requirements.txt
  3. in main.py, please give your three input parameters
  4. run: python main.py
