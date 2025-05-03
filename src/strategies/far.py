# -*- coding: utf-8 -*-
"""
Module Name: far.py

Description:
The FAR module computes index levels and constituent weights for 
a short-term mean-reversion strategy based on the price ratio of 
Fallen Angel and High Yield Bond ETFs.

Author: elreysausage
Date: 2025-02-23
"""

from datetime import date

import pandas as pd

from core.strategy import Strategy
import core.utils as ut


class FARStrategy(Strategy):
    """
    The FARStrategy class orchestrates the retrieval of the FAR strategy's signals,
    returns, and constituent weights based on specified parameters.

    Attributes:
        START_DATE: The start date for data retrieval.
        END_DATE: The end date for data retrieval.
        LOOKBACK_WINDOW: The number of days to look back for computing signals.
        SIGNAL_THRESHOLD: The threshold deviation for allowable signals.
        TRADE_LAG: The trade implementation lag.
    """
    START_DATE: str = '2000-01-01'
    END_DATE: str = date.today()
    LOOKBACK_WINDOW: int = 2
    SIGNAL_THRESHOLD: float = 0.0
    TRADE_LAG: int = 1
    TICKERS = ['HYG', 'FALN']

    def __init__(self, name: str) -> None:
        """
        Initializes the STABStrategy class with the specified tickers.

        Parameters:
            name: The name of the strategy
        """
        self.name = name
        self.set_data()
        self.set_params()

    def set_data(self) -> None:
        """
        Sets the data for the FAR strategy.
        """
        self.price_data = (
            ut.get_prices(self.TICKERS, self.START_DATE, self.END_DATE)
            .ffill()
            .dropna()
        )
        self.returns_data = (
            self.price_data.pct_change(fill_method=None)
            .iloc[1:]
            .dropna(axis=1)
        )

    def set_params(self) -> None:
        """
        Sets the parameters for the STAB strategy.

        Notes:
            STAB does not require any additional parameter setting.
        """
        pass

    def get_strategy_weights(self) -> pd.DataFrame:
        """
        Computes strategy positions based on reversion signals.

        Returns:
            positions: A DataFrame containing the strategy positions.
        """
        price_ratio = self.price_data['FALN'] / self.price_data['HYG']
        signals = pd.DataFrame(
            index=self.price_data.index, 
            columns=self.price_data.columns
        )
        signals['HYG'] = (
            (price_ratio - price_ratio.ewm(self.LOOKBACK_WINDOW, adjust=False).mean())
              / price_ratio.ewm(self.LOOKBACK_WINDOW, adjust=False).std()
        )
        signals['FALN'] = -signals['HYG']
        signals[signals.abs()<self.SIGNAL_THRESHOLD] = 0
        positions = signals.shift(self.TRADE_LAG)
        return positions

    def get_strategy_returns(self) -> pd.Series:
        """
        Computes the strategy returns based on the top sub-strategies.

        Returns:
            strategy_returns: A Series containing the strategy returns.
        """
        return (
            self.get_strategy_weights()
            .multiply(self.returns_data)
            .sum(axis=1)
        )

    def get_strategy_output(self) -> dict:
        """
        Retrieves the strategy output for the FAR strategy.

        Returns:
            A DataFrame containing the strategy output.
        """
        strategy_levels = pd.DataFrame(
            self.get_strategy_levels(
                self.get_strategy_returns()
            )
        )
        strategy_weights = self.get_strategy_weights()

        return {
            'Strategy Levels': strategy_levels,
            'Target Weights': strategy_weights,
            'Effective Weights': strategy_weights.shift(self.TRADE_LAG)
        }
