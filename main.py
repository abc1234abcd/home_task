import asyncio
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, Set

class PortfolioPriceCalculator:
    def __init__(self, prices_file_path, portf_file_path, portf_price_file_path):
        self.prices_file_path = prices_file_path
        self.prices = defaultdict(float)
        self.portf_file_path = portf_file_path
        self.portfolios  = defaultdict(dict)
        self.portf_price_file_path = portf_price_file_path
        #for efficient price update: keep a hashmap for calculated portf_prices, then later update price += price_delta*weight
        self.completed_portfolios = defaultdict(float)
        #for o(1) enquiry: given price_name, return calcaulatable 'portf_definition'
        self.reverted_hashmap = defaultdict(set) 

    #load and flatten portfolios.csv: 
    async def portfolios_file_loader(self) -> Dict[str, Dict[str, float]]:
        portf_definition_set = set()
        curr_definition = None
        with open(self.portf_file_path, 'r') as file:
            try:
                next(file)
                for line in file:
                    if not line:
                        continue
                    splits = line.strip().split(',')
                    if len(splits) == 2:
                        name  = str(splits[0])
                        if not splits[1]:
                            curr_definition = name
                            self.portfolios[curr_definition] = {}
                            portf_definition_set.add(curr_definition)
                        else:
                            name, weight = str(splits[0]), float(splits[1])
                            #flatten nested portfolios
                            if name in portf_definition_set:
                                nested_portf = { k : float(v)*weight for k, v in self.portfolios[name].items()}
                                self.portfolios[curr_definition].update(nested_portf)
                            else:
                                self.portfolios[curr_definition][name] = weight
                    else:
                        logging.error(f"unexpected number of values in {splits}.")
                logging.info("portfolios.csv loading success!")
            except Exception as e:
                logging.error(f"exception error in portfolio_file_loader: {e}.")

    async def create_revert_hashmap(self) -> Dict[str, Set[str]]:
        if not self.portfolios:
            logging.error("creating revert hashmap before original hashmap.")
            raise 
        for portf_definition, portf_member in self.portfolios.items():
            for ticker in portf_member:
                if ticker not in self.reverted_hashmap:
                    self.reverted_hashmap[ticker] = set()
                self.reverted_hashmap[ticker].add(portf_definition)
        logging.info("revert hashmap creating success!")
      
    # given price_name, return calculatable 'portf_definition' 
    async def calculatable_portf(self, name) -> Set[str]:
        ready_to_pricing = set()
        if self.reverted_hashmap:
            if self.reverted_hashmap[name]:
                for item in self.reverted_hashmap[name]:
                    if all(ticker in self.prices for ticker in self.portfolios[item]):
                        ready_to_pricing.add(item)
            else:
                logging.info(f"{name} is not used in any of existing portfolios.")
        else:
            logging.warning("reverted hashmap is not creating yet while enqurying it.")
        return ready_to_pricing 
    
    async def price_calculator(self):
        #streaming prices.csv and portfolio_prices.csv
        with (open(self.portf_price_file_path, 'w') as portf_price_file,
            open(self.prices_file_path, 'r') as prices_file):
            try:
                portf_price_file.write("PORTFOLIO_DEFINITION, PRICE\n")
                next(prices_file)
                for line in prices_file:
                    splits = line.strip().split(',')
                    if len(splits) == 2 and all(splits):
                        name, price = str(splits[0]), float(splits[1])
                        #check price_detal first to avoid unnecessary checking/calculations
                        if name in self.prices:
                            price_delta = price - self.prices[name] 
                            if price_delta == 0:
                                continue
                            else: 
                                ready_to_pricing = await self.calculatable_portf(name)
                                if ready_to_pricing:
                                    for item in ready_to_pricing:
                                        if self.completed_portfolios[item]:
                                            #update pricing
                                            self.completed_portfolios[item] += price_delta*self.portfolios[item][name]
                                            portf_price_file.write(f"{item},{self.completed_portfolios[item]}\n")
                                        else: #first time prcing
                                            self.prices[name] = price 
                                            self.completed_portfolios[item] = sum(self.prices[port_member] * weight for port_member, weight in self.portfolios[item].items())
                                            portf_price_file.write(f"{item},{self.completed_portfolios[item]}\n")
                                else:
                                    logging.info(f"{name}:{price} receieved, but no portfolio is calculatable.")
                                    continue
                        else:
                            self.prices[name] = price #first time i met this price_name
                            ready_to_pricing= await self.calculatable_portf(name)
                            if ready_to_pricing:
                                for item in ready_to_pricing: 
                                        self.completed_portfolios[item]  = sum(self.prices[port_member] * weight for port_member, weight in self.portfolios[item].items())
                                        portf_price_file.write(f"{item},{self.completed_portfolios[item]}\n")
                            else:
                                logging.info(f"{name}:{price} receieved, but no portfolio is calculatable.")
                                continue
                    else:
                        logging.error(f"unexpected number of values in prices.csv line:{line}.")
                logging.info("price_calculator success!")
            except Exception as e:
                logging.error(f"portf calculator exception error {e}.")

#define asyncio coroutine
async def main(prices_file_path, portf_file_path, portf_price_file_path):
    calculator_instance = PortfolioPriceCalculator(prices_file_path, portf_file_path, portf_price_file_path)
    await asyncio.gather(
        calculator_instance.portfolios_file_loader(),
        calculator_instance.create_revert_hashmap(),
        calculator_instance.price_calculator(),
    )
if __name__=='__main__':
    #logging config
    log_file_path = Path(__file__).parent/f"{datetime.now().strftime('%Y-%m-%d')}.log"
    logging.basicConfig(
        filename = str(log_file_path),
        format = "%(asctime)s %(levelname)-7s %(message)s",
        level = logging.INFO,
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    #define input params
    prices_file_path = "prices.csv" 
    portf_file_path = "portfolios.csv"
    portf_price_file_path = 'portfolio_prices.csv'

    asyncio.run(main(prices_file_path, portf_file_path, portf_price_file_path))




