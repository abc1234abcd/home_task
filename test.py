import pytest
from main import PortfolioPriceCalculator

class TestCalculator:
    @pytest.fixture
    def calculator(self):
        return PortfolioPriceCalculator("prices.csv", "portfolios.csv", "test_portf_prices.csv")
        
    @pytest.mark.asyncio
    async def test_missing_member(self, calculator):
        calculator.portfolios = {"TECH": {"AAPL": 100, "MSFT": 200.0, "NVDA":300.0}}
        calculator.reverted_hashmap = {"AAPL": {"TECH"}, "MSFT": {"TECH"}, "NVDA":{"TECH"}}
        calculator.completed_portfolios = {}
        #missing portf_member
        calculator.prices ={"AAPL": 173, "MSFT": 425.0}
        portf_defnition = await calculator.calculatable_portf("AAPL")
        assert len(portf_defnition) == 0

    @pytest.mark.asyncio
    async def test_enough_member(self, calculator):
        calculator.portfolios = {"TECH": {"AAPL": 100, "MSFT": 200.0, "NVDA":300.0}}
        calculator.reverted_hashmap = {"AAPL": {"TECH"}, "MSFT": {"TECH"}, "NVDA":{"TECH"}}
        calculator.completed_portfolios = {}
        calculator.prices ={"AAPL": 173, "MSFT": 425}
        #give enough portf_member
        calculator.prices['NVDA'] = 880
        portf_defnition = await calculator.calculatable_portf("NVDA")
        assert portf_defnition == {"TECH"}
        assert len(portf_defnition) == 1

    @pytest.mark.asyncio
    async def test_calculator_calculation(self, calculator):
        await calculator.portfolios_file_loader()
        await calculator.create_revert_hashmap()
        await calculator.price_calculator()
        with open('test_portf_prices.csv', 'r') as file:
            next(file)
            splits = next(file).strip().split(',')
            portf_definition, price = str(splits[0]), float(splits[1])
            assert portf_definition == "TECH"
            assert price == 366300.0
            splits = next(file).strip().split(',')
            portf_definition, price = str(splits[0]), float(splits[1])
            assert portf_definition == "TECH"
            assert price == 366400.0

if __name__ == '__main__':
    pytest.main([__file__, '-v'])