# -*- coding: utf-8 -*-
"""
Module Name: newt.py

Description:
The NEWT module computes index levels for a news trading strategy based on signals 
which have been generated and uploaded to the database using the Squawker package.

Notes:
Before the NEWT strategy can be run, the news signals must be generated and uploaded
to the database using the NewsManager and PriceFetcher modules in the Squawker package.
These can be configured to run as background processes using Task Scheduler or Cron.

Author: elreysausage
Date: 2024-11-15
"""

import pandas as pd

from core.strategy import Strategy
import core.utils as ut


class NEWTStrategy(Strategy):
    """
    The NEWTStrategy class orchestrates the retrieval of the 
    NEWT strategy's signals and returns based on specified parameters.

    Attributes:
        SLIPPAGE_COST: The cost of slippage for trading.
    """
    SLIPPAGE_COST: float = 0.0005

    def __init__(self, name: str, position_size: float):
        """
        Initializes the Strategy class with necessary parameters.

        Parameters:
            name: The name of the strategy.
            position_size: The position size for the strategy.
        """
        super().__init__(name)
        self.name = name
        self.position_size = position_size
        self.set_data()
        self.set_params()

    def set_data(self):
        """
        Sets the required input data for the NEWT strategy.
        """
        conn, cursor = ut.connect_db()
        query = (
            "SELECT * FROM news_signals "
            "WHERE price_at_record IS NOT NULL "
            "AND price_plus_30min IS NOT NULL "
            "AND price_plus_1hr IS NOT NULL "
            "AND price_plus_3hr IS NOT NULL "
            "AND price_eod IS NOT NULL; "
        )
        try:
            cursor.execute(query)
            query_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            self.input_data = pd.DataFrame(query_result, columns=columns)
        except Exception as e:
            raise RuntimeError("Input data unavailable...") from e

    def set_params(self) -> None:
        """
        Sets the parameters for the BAM strategy.

        Notes:
            NEWT does not require any external parameters.
        """
        pass
    
    def get_strategy_weights(self, position_size) -> None:
        """
        Computes the constituent weights for the combined strategy.

        Parameters:
            position_size: The position size for the strategy.
        """ 
        return position_size
    
    def get_strategy_returns(self) -> pd.Series:
        """
        Computes the daily returns for the strategy.

        Returns:
            A Series containing the daily returns for the strategy.
        """
        self.input_data['signal_return'] = (
            self.input_data['price_eod'] / self.input_data['price_at_record'] - 1
            - self.SLIPPAGE_COST
        )
        self.input_data['date'] = pd.to_datetime(
            self.input_data['record_timestamp']).dt.date
        
        strategy_returns = (
            self.input_data.groupby('date')['signal_return'].sum() 
            * self.get_strategy_weights(self.position_size)
        )
        return strategy_returns
    
    def get_strategy_output(self) -> dict[str, pd.DataFrame]:
        """
        Generates the strategy output for the NEWT strategy.

        Returns:
            A dictionary containing:
                - 'Strategy Levels': The cumulative strategy levels.
        """
        strategy_levels = pd.DataFrame(
            self.get_strategy_levels(self.get_strategy_returns()))
        return {
            'Strategy Levels': strategy_levels,
        }
