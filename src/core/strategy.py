# -*- coding: utf-8 -*-
"""
Module Name: strategy.py

Description:
The Strategy module defines the `Strategy` abstract base class, a template for building 
trading strategies with methods for data setting, parameter setting, signal generation, 
returns calculation, and portfolio output generation.

Author: elreysausage
Date: 2024-08-15
"""

from abc import ABC, abstractmethod

import pandas as pd

import core.utils as ut


class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    """ 
    def __init__(self, name: str):
        """
        Initializes the Strategy base class.

        Parameters:
            name: The name of the strategy.
        """
        self.name = name

    @abstractmethod
    def set_data(self, data: pd.DataFrame | pd.Series) -> None:
        """
        Sets the input data needed for the strategy.

        Parameters:
            data: Relevant external data inputs for the strategy.
        """
        pass

    @abstractmethod
    def set_params(self, **kwargs) -> None:
        """
        Sets strategy parameters.

        Parameters:
            **kwargs: Arbitrary keyword arguments representing strategy parameters.
        """
        pass

    @abstractmethod
    def get_strategy_output(self) -> dict[str, pd.DataFrame]:
        """
        Generates and returns the strategy's output to be committed to database.

        Returns:
            dict: A dictionary containing:
                - 'Strategy Levels': The cumulative levels for the combined strategy.
                - 'Target Weights': The target weights for each sub-strategy.
                - 'Effective Weights': The effective weights lagged from target weights.
        """
        pass

    @abstractmethod
    def get_strategy_returns(self) -> pd.Series:
        """
        Calculates daily returns for the strategy based on generated signals.

        Returns:
            pd.Series: Daily returns for the strategy.
        """
        pass

    @abstractmethod
    def get_strategy_weights(self) -> pd.DataFrame:
        """
        Calculates and returns portfolio weights based on the strategy's 
        configuration and internal data.

        Returns:
            pd.DataFrame: Daily constituent weights for the strategy.
        """
        pass

    def get_strategy_levels(self, strategy_returns: pd.Series) -> pd.Series:
        """
        Calculates cumulative returns from daily strategy returns.

        Parameters:
            strategy_returns: Series containing daily strategy returns.

        Returns:
            pd.Series: Cumulative strategy levels.
        """
        return ut.get_cum_returns(strategy_returns)
    

    def get_strategy_statistics(
            self, 
            strategy_returns: pd.Series,
            display_chart: bool
    ) -> dict[str, float]:
        """
        Calculates and returns performance statistics for the strategy.

        Parameters:
            strategy_returns: Series containing daily strategy returns.
            display_chart: Boolean indicating whether to display a performance chart.

        Returns:
            dict: Dictionary with performance metrics.
        """
        return ut.get_perf_stats(strategy_returns, display_chart)
